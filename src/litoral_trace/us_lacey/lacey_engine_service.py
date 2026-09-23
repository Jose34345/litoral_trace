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
from litoral_trace.lacey_engine.architecture import AIArchitecture, ai_architecture
from litoral_trace.lacey_engine.domain import DocumentResolution
from litoral_trace.lacey_engine.pipeline import ENGINE_VERSION, process_bundle
from litoral_trace.lacey_engine.serialization import (
    BUNDLE_RESOLUTION_SCHEMA_VERSION,
    SHIPMENT_RESOLUTION_SCHEMA_VERSION,
    deserialize_bundle_resolution,
    serialize_bundle_resolution,
    serialize_shipment_resolution,
)
from litoral_trace.lacey_engine.shipment import LaceyRuleset, ShipmentDocumentInput, process_shipment
from litoral_trace.services.vault import VaultService
from litoral_trace.us_lacey import specialized_projection_runtime, specialized_shadow
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
    succeeded_document_count: int = 0
    failed_document_count: int = 0


@dataclass(frozen=True, slots=True)
class _AIShadowDocumentContext:
    link: UsLaceyOperationDocument
    assurance: AssuranceDocument
    vault: VaultDocument
    engine2_resolution: DocumentResolution


@dataclass(frozen=True, slots=True)
class _DocumentBatchSuccess:
    document: object
    result: object


@dataclass(frozen=True, slots=True)
class _DocumentBatchFailure:
    document: object
    safe_error_code: str = "ENGINE2_SHADOW_FAILED"
    safe_error_message: str = "Shadow document processing did not complete."


@dataclass(frozen=True, slots=True)
class _DocumentBatchOutcome:
    status: str
    succeeded: tuple[_DocumentBatchSuccess, ...]
    failed: tuple[_DocumentBatchFailure, ...]


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

    def _process_engine2_document_batch(self, *, documents, process_one) -> _DocumentBatchOutcome:
        """Process each source independently so one bad document cannot hide its siblings."""
        succeeded: list[_DocumentBatchSuccess] = []
        failed: list[_DocumentBatchFailure] = []
        for document in documents:
            try:
                result = process_one(document)
            except Exception:
                LOGGER.exception("Lacey Engine 2 document processing failed")
                failed.append(_DocumentBatchFailure(document=document))
            else:
                succeeded.append(_DocumentBatchSuccess(document=document, result=result))

        if failed and succeeded:
            status = "PARTIAL"
        elif failed:
            status = "FAILED"
        else:
            status = "SUCCEEDED"
        return _DocumentBatchOutcome(status, tuple(succeeded), tuple(failed))

    def _engine2_document_run_identity(
        self,
        *,
        organization_id: int,
        assurance_document_id: int,
        source_sha256: str,
        role_hint: str | None,
        status: str,
    ) -> dict[str, object]:
        """Return the immutable identity for one physical-source bundle run."""
        return {
            "organization_id": organization_id,
            "assurance_document_id": assurance_document_id,
            "source_sha256": source_sha256,
            "engine_version": self._engine_version,
            "schema_version": BUNDLE_RESOLUTION_SCHEMA_VERSION,
            "role_hint": role_hint,
            "status": status,
        }

    def _find_engine2_document_run(
        self,
        session: Session,
        **identity: object,
    ) -> UsLaceyEngineDocumentRun | None:
        conditions = [
            getattr(UsLaceyEngineDocumentRun, key) == value
            for key, value in identity.items()
        ]
        return session.scalar(select(UsLaceyEngineDocumentRun).where(*conditions))

    def _get_or_create_failed_engine2_run(
        self,
        session: Session,
        *,
        operation_id: int,
        operation_document_id: int,
        identity: dict[str, object],
    ) -> UsLaceyEngineDocumentRun:
        """Reuse an existing immutable failure instead of violating its unique identity."""
        existing = self._find_engine2_document_run(session, **identity)
        if existing is not None:
            return existing
        failed = UsLaceyEngineDocumentRun(
            **identity,
            operation_id=operation_id,
            operation_document_id=operation_document_id,
            safe_error_code="ENGINE2_SHADOW_FAILED",
            safe_error_message="Shadow document processing did not complete.",
        )
        session.add(failed)
        return failed

    def _run_ai_shadow_document(
        self,
        *,
        config: AIProviderConfig,
        organization_id: int,
        operation_id: int,
        link: UsLaceyOperationDocument,
        assurance: AssuranceDocument,
        vault: VaultDocument,
        engine2_resolution: DocumentResolution,
    ) -> None:
        """Best-effort legacy AI comparison; never changes authoritative workflow state."""
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
            payload = serialize_ai_shadow_run(ai=verified_ai, comparison=comparison)
            payload["architecture"] = AIArchitecture.LEGACY.value
            payload["candidate_count"] = len(verified_ai.candidates)
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
                    resolution_json=payload,
                )
            )
            session.commit()
            LOGGER.info(
                "Lacey legacy AI shadow persisted",
                extra={
                    "architecture": AIArchitecture.LEGACY.value,
                    "organization_id": organization_id,
                    "operation_id": operation_id,
                    "operation_document_id": link.id,
                    "assurance_document_id": assurance.id,
                    "ai_provider": verified_ai.provider,
                    "ai_model": verified_ai.model,
                    "latency_ms": verified_ai.latency_ms,
                    "input_tokens": verified_ai.input_tokens,
                    "output_tokens": verified_ai.output_tokens,
                    "total_tokens": verified_ai.total_tokens,
                    "candidate_count": len(verified_ai.candidates),
                },
            )
        except Exception:
            session.rollback()
            LOGGER.exception(
                "Lacey AI extraction shadow failed",
                extra={
                    "architecture": AIArchitecture.LEGACY.value,
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
                        "architecture": AIArchitecture.LEGACY.value,
                        "organization_id": organization_id,
                        "operation_id": operation_id,
                        "assurance_document_id": assurance.id,
                    },
                )
        finally:
            session.close()

    def _run_legacy_ai_operation(
        self,
        *,
        config: AIProviderConfig,
        organization_id: int,
        operation_id: int,
        documents: tuple[_AIShadowDocumentContext, ...],
    ) -> None:
        for document in documents:
            self._run_ai_shadow_document(
                config=config,
                organization_id=organization_id,
                operation_id=operation_id,
                link=document.link,
                assurance=document.assurance,
                vault=document.vault,
                engine2_resolution=document.engine2_resolution,
            )

    def _run_specialized_ai_operation(
        self,
        *,
        config: AIProviderConfig,
        organization_id: int,
        operation_id: int,
        documents: tuple[_AIShadowDocumentContext, ...],
        source_set_fingerprint: str,
    ) -> None:
        if not documents:
            return

        specialized_documents: list[specialized_shadow.SpecializedShadowDocument] = []
        for document in documents:
            with self._vault.materialize_verified_download(
                organization_id=organization_id,
                document_id=document.vault.public_id,
            ) as download:
                content = b"".join(download.iter_chunks())
            specialized_documents.append(
                specialized_shadow.SpecializedShadowDocument(
                    document_id=document.assurance.public_id,
                    operation_document_id=document.link.id,
                    assurance_document_id=document.assurance.id,
                    source_sha256=document.vault.sha256,
                    role_hint=document.link.document_role,
                    filename=document.vault.original_filename,
                    content=content,
                    engine2_resolution=document.engine2_resolution,
                )
            )

        run = specialized_shadow.run_specialized_shadow_operation(
            documents=tuple(specialized_documents),
            config=config,
        )
        engine_version = specialized_shadow.specialized_engine_version(
            provider=run.provider,
            model=run.model,
            source_set_fingerprint=source_set_fingerprint,
        )
        row_status = "FAILED" if run.result.operation.value == "FAILED" else "SUCCEEDED"
        session: Session = self._session_factory()
        try:
            set_tenant_db_context(session, organization_id)
            projection = None
            if row_status == "SUCCEEDED":
                projection = specialized_projection_runtime.apply_specialized_projection_runtime(
                    session,
                    organization_id=organization_id,
                    operation_id=operation_id,
                    candidates=run.result.fused_candidates,
                    fusion_conflicts=run.result.fusion_conflicts,
                    judge_evaluation=run.result.field_judge,
                    source_assurance_by_document={
                        document.document_id: document.assurance_document_id
                        for document in specialized_documents
                    },
                )

            persisted = 0
            for document in specialized_documents:
                existing = session.scalar(
                    select(UsLaceyEngineDocumentRun).where(
                        UsLaceyEngineDocumentRun.organization_id == organization_id,
                        UsLaceyEngineDocumentRun.assurance_document_id
                        == document.assurance_document_id,
                        UsLaceyEngineDocumentRun.source_sha256 == document.source_sha256,
                        UsLaceyEngineDocumentRun.engine_version == engine_version,
                        UsLaceyEngineDocumentRun.schema_version
                        == specialized_shadow.SPECIALIZED_SHADOW_SCHEMA_VERSION,
                        UsLaceyEngineDocumentRun.role_hint == document.role_hint,
                        UsLaceyEngineDocumentRun.status == row_status,
                    )
                )
                if existing is not None:
                    continue
                payload = specialized_shadow.serialize_specialized_document_run(
                    run=run,
                    document_id=document.document_id,
                    source_set_fingerprint=source_set_fingerprint,
                )
                if projection is not None:
                    payload["projection"] = projection
                session.add(
                    UsLaceyEngineDocumentRun(
                        organization_id=organization_id,
                        operation_id=operation_id,
                        operation_document_id=document.operation_document_id,
                        assurance_document_id=document.assurance_document_id,
                        engine_version=engine_version,
                        schema_version=specialized_shadow.SPECIALIZED_SHADOW_SCHEMA_VERSION,
                        source_sha256=document.source_sha256,
                        role_hint=document.role_hint,
                        status=row_status,
                        resolution_json=payload,
                        safe_error_code=(
                            "SPECIALIZED_EXTRACTION_FAILED" if row_status == "FAILED" else None
                        ),
                        safe_error_message=(
                            "Specialized extraction did not produce a usable result."
                            if row_status == "FAILED"
                            else None
                        ),
                    )
                )
                persisted += 1
            session.commit()
            LOGGER.info(
                "Lacey specialized AI shadow persisted",
                extra={
                    "architecture": AIArchitecture.SPECIALIZED.value,
                    "organization_id": organization_id,
                    "operation_id": operation_id,
                    "ai_provider": run.provider,
                    "ai_model": run.model,
                    "latency_ms": run.latency_ms,
                    "input_tokens": run.input_tokens,
                    "output_tokens": run.output_tokens,
                    "total_tokens": run.total_tokens,
                    "candidate_count": len(run.result.fused_candidates),
                    "document_count": len(specialized_documents),
                    "persisted_document_count": persisted,
                    "operation_status": run.result.operation.value,
                    "projection_mode": projection.get("mode") if projection else "off",
                    "projected_count": projection.get("projected_count") if projection else 0,
                    "projection_review_count": projection.get("review_count") if projection else 0,
                },
            )
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _dispatch_ai_extractors(
        self,
        *,
        config: AIProviderConfig,
        organization_id: int,
        operation_id: int,
        documents: tuple[_AIShadowDocumentContext, ...],
        source_set_fingerprint: str,
    ) -> None:
        """Execute the configured AI architecture without changing legacy UI authority."""
        if not ai_shadow_enabled(config):
            return

        architecture = ai_architecture()
        if architecture is AIArchitecture.LEGACY:
            self._run_legacy_ai_operation(
                config=config,
                organization_id=organization_id,
                operation_id=operation_id,
                documents=documents,
            )
            return

        if architecture is AIArchitecture.SPECIALIZED:
            self._run_specialized_ai_operation(
                config=config,
                organization_id=organization_id,
                operation_id=operation_id,
                documents=documents,
                source_set_fingerprint=source_set_fingerprint,
            )
            return

        # Shadow production contract: legacy always runs first. Specialized is strictly
        # best-effort and cannot poison the successful legacy/session/worker path.
        self._run_legacy_ai_operation(
            config=config,
            organization_id=organization_id,
            operation_id=operation_id,
            documents=documents,
        )
        try:
            self._run_specialized_ai_operation(
                config=config,
                organization_id=organization_id,
                operation_id=operation_id,
                documents=documents,
                source_set_fingerprint=source_set_fingerprint,
            )
        except Exception:
            LOGGER.exception(
                "Lacey specialized AI shadow failed",
                extra={
                    "architecture": AIArchitecture.SPECIALIZED.value,
                    "organization_id": organization_id,
                    "operation_id": operation_id,
                    "ai_provider": config.provider,
                    "ai_model": config.model,
                    "source_set_fingerprint": source_set_fingerprint,
                },
            )

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

            def process_one(row):
                link, assurance, vault = row
                success_identity = self._engine2_document_run_identity(
                    organization_id=organization_id,
                    assurance_document_id=assurance.id,
                    source_sha256=vault.sha256,
                    role_hint=link.document_role,
                    status="SUCCEEDED",
                )
                run = self._find_engine2_document_run(session, **success_identity)
                if run is not None:
                    bundle = deserialize_bundle_resolution(run.resolution_json)
                else:
                    with self._vault.materialize_verified_download(
                        organization_id=organization_id,
                        document_id=vault.public_id,
                    ) as download:
                        content = b"".join(download.iter_chunks())
                    bundle = process_bundle(
                        filename=vault.original_filename,
                        content=content,
                        role_hint=link.document_role,
                    )
                    run = UsLaceyEngineDocumentRun(
                        **success_identity,
                        operation_id=operation_id,
                        operation_document_id=link.id,
                        resolution_json=serialize_bundle_resolution(bundle),
                    )
                    session.add(run)
                    session.flush()
                return link, assurance, vault, bundle

            batch = self._process_engine2_document_batch(
                documents=tuple(rows),
                process_one=process_one,
            )

            for failure in batch.failed:
                link, assurance, vault = failure.document
                failed_identity = self._engine2_document_run_identity(
                    organization_id=organization_id,
                    assurance_document_id=assurance.id,
                    source_sha256=vault.sha256,
                    role_hint=link.document_role,
                    status="FAILED",
                )
                self._get_or_create_failed_engine2_run(
                    session,
                    operation_id=operation_id,
                    operation_document_id=link.id,
                    identity=failed_identity,
                )

            ai_config = AIProviderConfig.from_env()
            ai_documents: list[_AIShadowDocumentContext] = []
            inputs: list[ShipmentDocumentInput] = []
            for success in batch.succeeded:
                link, assurance, vault, bundle = success.result

                # Legacy/specialized AI shadow still operates on a physical source
                # contract. Until that advisory path becomes bundle-aware, preserve
                # it only for true single-logical-document sources rather than
                # comparing whole-file AI output against one arbitrary segment.
                if len(bundle.documents) == 1:
                    ai_documents.append(
                        _AIShadowDocumentContext(
                            link=link,
                            assurance=assurance,
                            vault=vault,
                            engine2_resolution=bundle.documents[0].resolution,
                        )
                    )
                else:
                    LOGGER.info(
                        "Skipping physical-document AI shadow for multi-document bundle",
                        extra={
                            "organization_id": organization_id,
                            "operation_id": operation_id,
                            "operation_document_id": link.id,
                            "logical_document_count": len(bundle.documents),
                        },
                    )

                for logical in bundle.documents:
                    inputs.append(
                        ShipmentDocumentInput(
                            document_id=f"{link.id}:{logical.logical_document_id}",
                            filename=logical.virtual_filename,
                            role_hint=logical.document_type.value,
                            resolution=logical.resolution,
                        )
                    )

            # AI extraction remains non-authoritative, but successful sibling documents
            # can still contribute shadow telemetry even when another source failed.
            self._dispatch_ai_extractors(
                config=ai_config,
                organization_id=organization_id,
                operation_id=operation_id,
                documents=tuple(ai_documents),
                source_set_fingerprint=fingerprint,
            )

            # Never materialize a shipment snapshot from an incomplete source set.
            # Mixed outcomes surface as BLOCKED_PARTIAL so callers can distinguish
            # usable sibling evidence from a total source-processing failure.
            if batch.status != "SUCCEEDED":
                session.commit()
                public_status = "BLOCKED_PARTIAL" if batch.status == "PARTIAL" else "FAILED"
                return ShadowAggregationResult(
                    public_status,
                    succeeded_document_count=len(batch.succeeded),
                    failed_document_count=len(batch.failed),
                )

            # Existing Engine 2 shipment snapshots are immutable/reusable, but the
            # document loop above still lets newly enabled AI architectures backfill
            # isolated document comparisons without changing that shipment snapshot.
            if existing is not None:
                session.commit()
                return ShadowAggregationResult(
                    "SUCCEEDED",
                    existing.id,
                    succeeded_document_count=len(batch.succeeded),
                )

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
            return ShadowAggregationResult(
                "SUCCEEDED",
                snapshot.id,
                succeeded_document_count=len(batch.succeeded),
            )
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
