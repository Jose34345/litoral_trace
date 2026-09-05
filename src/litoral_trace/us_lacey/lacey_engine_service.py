"""Tenant-scoped, non-authoritative Engine 2 and AI extraction shadow aggregation."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import logging
import os

from sqlalchemy import select
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    AssuranceDocument,
    UsLaceyEngineDocumentRun,
    UsLaceyEngineShipmentRun,
    UsLaceyOperationDocument,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.lacey_engine.ai_providers import (
    AIProviderConfig,
    ai_shadow_enabled,
    ai_shadow_engine_version,
    build_ai_provider,
)
from litoral_trace.lacey_engine.ai_shadow import (
    AI_SHADOW_SCHEMA_VERSION,
    reconcile_engine2_with_ai,
    serialize_ai_shadow_run,
    verify_ai_evidence,
)
from litoral_trace.lacey_engine.pipeline import ENGINE_VERSION, process_document
from litoral_trace.lacey_engine.serialization import (
    DOCUMENT_RESOLUTION_SCHEMA_VERSION,
    SHIPMENT_RESOLUTION_SCHEMA_VERSION,
    deserialize_document_resolution,
    serialize_document_resolution,
    serialize_shipment_resolution,
)
from litoral_trace.lacey_engine.shipment import LaceyRuleset, ShipmentDocumentInput, process_shipment
from litoral_trace.services.vault import VaultService
from litoral_trace.us_lacey.db import get_us_lacey_db_session

ENGINE2_OFF = "OFF"
ENGINE2_SHADOW = "SHADOW"
LOGGER = logging.getLogger(__name__)


def engine2_mode() -> str:
    return (
        ENGINE2_SHADOW
        if os.getenv("US_LACEY_ENGINE2_MODE", "off").strip().upper() == ENGINE2_SHADOW
        else ENGINE2_OFF
    )


@dataclass(frozen=True, slots=True)
class ShadowAggregationResult:
    status: str
    shipment_run_id: int | None = None


def source_set_fingerprint(
    *,
    organization_id: int,
    operation_id: int,
    documents: list[tuple[UsLaceyOperationDocument, VaultDocument]],
    ruleset_version: str = "lacey_ruleset_2026_01",
    engine_version: str = ENGINE_VERSION,
    shipment_schema_version: str = SHIPMENT_RESOLUTION_SCHEMA_VERSION,
) -> str:
    items = [
        {
            "operation_document_id": link.id,
            "assurance_document_id": link.assurance_document_id,
            "version": link.version_number,
            "sha256": vault.sha256,
        }
        for link, vault in sorted(documents, key=lambda pair: pair[0].id)
    ]
    encoded = json.dumps(
        {
            "organization_id": organization_id,
            "operation_id": operation_id,
            "documents": items,
            "engine_version": engine_version,
            "ruleset_version": ruleset_version,
            "shipment_schema_version": shipment_schema_version,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class UsLaceyEngine2Service:
    def __init__(
        self,
        *,
        session_factory=get_us_lacey_db_session,
        vault_service: VaultService,
        engine_version: str = ENGINE_VERSION,
        ruleset: LaceyRuleset = LaceyRuleset(),
    ) -> None:
        self._session_factory = session_factory
        self._vault = vault_service
        self._engine_version = engine_version
        self._ruleset = ruleset

    def _run_ai_shadow_document(
        self,
        *,
        config: AIProviderConfig,
        organization_id: int,
        operation_id: int,
        link: UsLaceyOperationDocument,
        assurance: AssuranceDocument,
        vault: VaultDocument,
        engine2_resolution,
    ) -> None:
        """Best-effort AI comparison; never changes Engine 2 or authoritative workflow state."""
        if not ai_shadow_enabled(config):
            return

        ai_engine_version = ai_shadow_engine_version(config)
        session: Session = self._session_factory()
        try:
            set_tenant_db_context(session, organization_id)
            succeeded = session.scalar(
                select(UsLaceyEngineDocumentRun).where(
                    UsLaceyEngineDocumentRun.organization_id == organization_id,
                    UsLaceyEngineDocumentRun.assurance_document_id == assurance.id,
                    UsLaceyEngineDocumentRun.source_sha256 == vault.sha256,
                    UsLaceyEngineDocumentRun.engine_version == ai_engine_version,
                    UsLaceyEngineDocumentRun.schema_version == AI_SHADOW_SCHEMA_VERSION,
                    UsLaceyEngineDocumentRun.role_hint == link.document_role,
                    UsLaceyEngineDocumentRun.status == "SUCCEEDED",
                )
            )
            if succeeded is not None:
                return

            provider = build_ai_provider(config)
            if provider is None:
                return
            with self._vault.materialize_verified_download(
                organization_id=organization_id,
                document_id=vault.public_id,
            ) as download:
                content = b"".join(download.iter_chunks())
            ai_result = provider.extract(filename=vault.original_filename, content=content)
            verified_ai = verify_ai_evidence(engine2=engine2_resolution, ai=ai_result)
            comparison = reconcile_engine2_with_ai(engine2=engine2_resolution, ai=verified_ai)
            session.add(
                UsLaceyEngineDocumentRun(
                    organization_id=organization_id,
                    operation_id=operation_id,
                    operation_document_id=link.id,
                    assurance_document_id=assurance.id,
                    engine_version=ai_engine_version,
                    schema_version=AI_SHADOW_SCHEMA_VERSION,
                    source_sha256=vault.sha256,
                    role_hint=link.document_role,
                    status="SUCCEEDED",
                    resolution_json=serialize_ai_shadow_run(ai=verified_ai, comparison=comparison),
                )
            )
            session.commit()
        except Exception:
            session.rollback()
            LOGGER.exception(
                "Lacey AI extraction shadow failed",
                extra={
                    "organization_id": organization_id,
                    "operation_id": operation_id,
                    "assurance_document_id": assurance.id,
                    "ai_provider": config.provider,
                    "ai_model": config.model,
                },
            )
            # Persist one safe failure snapshot for observability. The unique identity
            # includes status, so a later successful retry can coexist immutably.
            try:
                set_tenant_db_context(session, organization_id)
                failed = session.scalar(
                    select(UsLaceyEngineDocumentRun).where(
                        UsLaceyEngineDocumentRun.organization_id == organization_id,
                        UsLaceyEngineDocumentRun.assurance_document_id == assurance.id,
                        UsLaceyEngineDocumentRun.source_sha256 == vault.sha256,
                        UsLaceyEngineDocumentRun.engine_version == ai_engine_version,
                        UsLaceyEngineDocumentRun.schema_version == AI_SHADOW_SCHEMA_VERSION,
                        UsLaceyEngineDocumentRun.role_hint == link.document_role,
                        UsLaceyEngineDocumentRun.status == "FAILED",
                    )
                )
                if failed is None:
                    session.add(
                        UsLaceyEngineDocumentRun(
                            organization_id=organization_id,
                            operation_id=operation_id,
                            operation_document_id=link.id,
                            assurance_document_id=assurance.id,
                            engine_version=ai_engine_version,
                            schema_version=AI_SHADOW_SCHEMA_VERSION,
                            source_sha256=vault.sha256,
                            role_hint=link.document_role,
                            status="FAILED",
                            safe_error_code="AI_EXTRACTION_SHADOW_FAILED",
                            safe_error_message="AI extraction shadow did not complete.",
                        )
                    )
                    session.commit()
            except Exception:
                session.rollback()
                LOGGER.exception(
                    "Unable to persist Lacey AI extraction shadow failure",
                    extra={
                        "organization_id": organization_id,
                        "operation_id": operation_id,
                        "assurance_document_id": assurance.id,
                    },
                )
        finally:
            session.close()

    def resolve_operation_with_engine2(
        self,
        *,
        organization_id: int,
        operation_id: int,
    ) -> ShadowAggregationResult:
        session: Session = self._session_factory()
        set_tenant_db_context(session, organization_id)
        try:
            rows = session.execute(
                select(UsLaceyOperationDocument, AssuranceDocument, VaultDocument)
                .join(
                    AssuranceDocument,
                    (AssuranceDocument.id == UsLaceyOperationDocument.assurance_document_id)
                    & (AssuranceDocument.organization_id == UsLaceyOperationDocument.organization_id),
                )
                .join(
                    VaultDocument,
                    (VaultDocument.id == AssuranceDocument.vault_document_id)
                    & (VaultDocument.organization_id == AssuranceDocument.organization_id),
                )
                .where(
                    UsLaceyOperationDocument.organization_id == organization_id,
                    UsLaceyOperationDocument.operation_id == operation_id,
                    UsLaceyOperationDocument.is_current.is_(True),
                )
                .order_by(UsLaceyOperationDocument.id)
            ).all()
            pairs = [(row[0], row[2]) for row in rows]
            fingerprint = source_set_fingerprint(
                organization_id=organization_id,
                operation_id=operation_id,
                documents=pairs,
                engine_version=self._engine_version,
                ruleset_version=self._ruleset.version,
            )
            existing = session.scalar(
                select(UsLaceyEngineShipmentRun).where(
                    UsLaceyEngineShipmentRun.organization_id == organization_id,
                    UsLaceyEngineShipmentRun.operation_id == operation_id,
                    UsLaceyEngineShipmentRun.source_set_fingerprint == fingerprint,
                    UsLaceyEngineShipmentRun.schema_version == SHIPMENT_RESOLUTION_SCHEMA_VERSION,
                )
            )

            ai_config = AIProviderConfig.from_env()
            inputs: list[ShipmentDocumentInput] = []
            for link, assurance, vault in rows:
                run = session.scalar(
                    select(UsLaceyEngineDocumentRun).where(
                        UsLaceyEngineDocumentRun.organization_id == organization_id,
                        UsLaceyEngineDocumentRun.assurance_document_id == assurance.id,
                        UsLaceyEngineDocumentRun.source_sha256 == vault.sha256,
                        UsLaceyEngineDocumentRun.engine_version == self._engine_version,
                        UsLaceyEngineDocumentRun.schema_version == DOCUMENT_RESOLUTION_SCHEMA_VERSION,
                        UsLaceyEngineDocumentRun.role_hint == link.document_role,
                        UsLaceyEngineDocumentRun.status == "SUCCEEDED",
                    )
                )
                if run is None:
                    try:
                        with self._vault.materialize_verified_download(
                            organization_id=organization_id,
                            document_id=vault.public_id,
                        ) as download:
                            resolution = process_document(
                                filename=vault.original_filename,
                                content=b"".join(download.iter_chunks()),
                                role_hint=link.document_role,
                            )
                        run = UsLaceyEngineDocumentRun(
                            organization_id=organization_id,
                            operation_id=operation_id,
                            operation_document_id=link.id,
                            assurance_document_id=assurance.id,
                            engine_version=self._engine_version,
                            schema_version=DOCUMENT_RESOLUTION_SCHEMA_VERSION,
                            source_sha256=vault.sha256,
                            role_hint=link.document_role,
                            status="SUCCEEDED",
                            resolution_json=serialize_document_resolution(resolution),
                        )
                        session.add(run)
                        session.flush()
                    except Exception:
                        session.add(
                            UsLaceyEngineDocumentRun(
                                organization_id=organization_id,
                                operation_id=operation_id,
                                operation_document_id=link.id,
                                assurance_document_id=assurance.id,
                                engine_version=self._engine_version,
                                schema_version=DOCUMENT_RESOLUTION_SCHEMA_VERSION,
                                source_sha256=vault.sha256,
                                role_hint=link.document_role,
                                status="FAILED",
                                safe_error_code="ENGINE2_SHADOW_FAILED",
                                safe_error_message="Shadow document processing did not complete.",
                            )
                        )
                        session.commit()
                        return ShadowAggregationResult("FAILED")
                else:
                    resolution = deserialize_document_resolution(run.resolution_json)

                # AI is a second non-authoritative extractor. Its own DB session and
                # safe failure snapshots guarantee provider/network errors cannot poison
                # Engine 2 persistence or the legacy authoritative workflow.
                self._run_ai_shadow_document(
                    config=ai_config,
                    organization_id=organization_id,
                    operation_id=operation_id,
                    link=link,
                    assurance=assurance,
                    vault=vault,
                    engine2_resolution=resolution,
                )
                inputs.append(
                    ShipmentDocumentInput(
                        str(link.id),
                        vault.original_filename,
                        role_hint=link.document_role,
                        resolution=resolution,
                    )
                )

            # Existing Engine 2 shipment snapshots are immutable/reusable, but the
            # document loop above still lets a newly enabled AI shadow backfill its
            # isolated document comparison without changing that shipment snapshot.
            if existing is not None:
                session.commit()
                return ShadowAggregationResult("SUCCEEDED", existing.id)

            resolution = process_shipment(documents=inputs, ruleset=self._ruleset)
            snapshot = UsLaceyEngineShipmentRun(
                organization_id=organization_id,
                operation_id=operation_id,
                engine_version=resolution.engine_version,
                ruleset_version=resolution.ruleset_version,
                schema_version=SHIPMENT_RESOLUTION_SCHEMA_VERSION,
                source_set_fingerprint=fingerprint,
                document_count=len(inputs),
                readiness=resolution.readiness.value,
                resolution_json=serialize_shipment_resolution(resolution),
            )
            session.add(snapshot)
            session.commit()
            return ShadowAggregationResult("SUCCEEDED", snapshot.id)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
