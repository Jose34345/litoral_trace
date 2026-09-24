"""Tenant-scoped, non-authoritative Engine 2 and AI extraction shadow aggregation."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import logging
import os
import threading

from sqlalchemy import select
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    AssuranceDocument,
    UsLaceyEngineDocumentRun,
    UsLaceyEngineShipmentRun,
    UsLaceyOperation,
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
from litoral_trace.lacey_engine.domain import BundleResolution, DocumentResolution
from litoral_trace.lacey_engine.errors import UnsupportedDocumentDomainError
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
from litoral_trace.us_lacey.storage import (
    build_us_lacey_storage_settings,
    get_us_lacey_storage_client,
)

ENGINE2_OFF = "OFF"
ENGINE2_SHADOW = "SHADOW"
LOGGER = logging.getLogger(__name__)
_AI_BACKGROUND_INFLIGHT: set[tuple[int, int, str]] = set()
_AI_BACKGROUND_LOCK = threading.Lock()


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
            except UnsupportedDocumentDomainError as exc:
                LOGGER.info(
                    "Lacey Engine 2 rejected out-of-domain document",
                    extra={
                        "domain": exc.domain,
                        "safe_error_code": exc.code,
                    },
                )
                failed.append(
                    _DocumentBatchFailure(
                        document=document,
                        safe_error_code=exc.code,
                        safe_error_message=exc.safe_message,
                    )
                )
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
        """Return the exact immutable uniqueness identity for an Engine 2 document run."""
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
        safe_error_code: str = "ENGINE2_SHADOW_FAILED",
        safe_error_message: str = "Shadow document processing did not complete.",
    ) -> UsLaceyEngineDocumentRun:
        """Reuse an existing immutable failure instead of violating its unique identity."""
        existing = self._find_engine2_document_run(session, **identity)
        if existing is not None:
            return existing
        failed = UsLaceyEngineDocumentRun(
            **identity,
            operation_id=operation_id,
            operation_document_id=operation_document_id,
            safe_error_code=safe_error_code,
            safe_error_message=safe_error_message,
        )
        session.add(failed)
        return failed

    def preflight_document_domain(
        self,
        *,
        organization_id: int,
        operation_id: int,
        assurance_document_id: int,
    ) -> BundleResolution | None:
        """Reject unsupported PDFs before Assurance projection or regulatory work.

        A successful Engine 2 bundle is persisted here and reused later by the
        normal shadow aggregation path, so the early guard does not duplicate
        deterministic extraction work. Non-domain Engine 2 failures remain
        best-effort and do not replace Assurance's existing parser authority.
        """

        session: Session = self._session_factory()
        try:
            set_tenant_db_context(session, organization_id)
            row = session.execute(
                select(
                    UsLaceyOperationDocument,
                    AssuranceDocument,
                    VaultDocument,
                    UsLaceyOperation,
                )
                .join(
                    AssuranceDocument,
                    (AssuranceDocument.id == UsLaceyOperationDocument.assurance_document_id)
                    & (
                        AssuranceDocument.organization_id
                        == UsLaceyOperationDocument.organization_id
                    ),
                )
                .join(
                    VaultDocument,
                    (VaultDocument.id == AssuranceDocument.vault_document_id)
                    & (VaultDocument.organization_id == AssuranceDocument.organization_id),
                )
                .join(
                    UsLaceyOperation,
                    (UsLaceyOperation.id == UsLaceyOperationDocument.operation_id)
                    & (
                        UsLaceyOperation.organization_id
                        == UsLaceyOperationDocument.organization_id
                    ),
                )
                .where(
                    UsLaceyOperationDocument.organization_id == organization_id,
                    UsLaceyOperationDocument.operation_id == operation_id,
                    UsLaceyOperationDocument.assurance_document_id
                    == assurance_document_id,
                    UsLaceyOperationDocument.is_current.is_(True),
                )
            ).one_or_none()
            if row is None:
                return None

            link, assurance, vault, operation = row
            success_identity = self._engine2_document_run_identity(
                organization_id=organization_id,
                assurance_document_id=assurance.id,
                source_sha256=vault.sha256,
                role_hint=link.document_role,
                status="SUCCEEDED",
            )
            succeeded = self._find_engine2_document_run(
                session,
                **success_identity,
            )
            if succeeded is not None:
                return deserialize_bundle_resolution(succeeded.resolution_json)

            with self._vault.materialize_verified_download(
                organization_id=organization_id,
                document_id=vault.public_id,
            ) as download:
                content = b"".join(download.iter_chunks())

            try:
                bundle = process_bundle(
                    filename=vault.original_filename,
                    content=content,
                    role_hint=link.document_role,
                )
            except UnsupportedDocumentDomainError as exc:
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
                    safe_error_code=exc.code,
                    safe_error_message=exc.safe_message,
                )
                assurance.processing_status = "FAILED"
                assurance.last_error_code = exc.code
                assurance.last_error_message = exc.safe_message[:512]
                operation.status = "FAILED"
                operation.review_result = "DOCUMENT_REJECTED"
                session.commit()
                raise

            run = UsLaceyEngineDocumentRun(
                **success_identity,
                operation_id=operation_id,
                operation_document_id=link.id,
                resolution_json=serialize_bundle_resolution(bundle),
            )
            session.add(run)
            session.commit()
            return bundle
        except UnsupportedDocumentDomainError:
            raise
        except Exception:
            session.rollback()
            LOGGER.exception(
                "Lacey domain preflight could not complete; legacy parser remains authoritative",
                extra={
                    "organization_id": organization_id,
                    "operation_id": operation_id,
                    "assurance_document_id": assurance_document_id,
                },
            )
            return None
        finally:
            session.close()


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
        """Best-effort legacy AI comparison without holding DB state across HTTP.

        The read-side idempotency check and the write-side persistence use separate,
        short-lived transactions. Vault I/O and provider HTTP calls happen with no
        SQLAlchemy Session checked out.
        """
        if not ai_shadow_enabled(config):
            return

        operation_document_id = int(link.id)
        assurance_document_id = int(assurance.id)
        assurance_public_id = assurance.public_id
        source_sha256 = str(vault.sha256)
        role_hint = link.document_role
        vault_public_id = vault.public_id
        filename = str(vault.original_filename)
        ai_engine_version = ai_shadow_engine_version(config)

        read_session: Session = self._session_factory()
        try:
            set_tenant_db_context(read_session, organization_id)
            succeeded = read_session.scalar(
                select(UsLaceyEngineDocumentRun.id).where(
                    UsLaceyEngineDocumentRun.organization_id == organization_id,
                    UsLaceyEngineDocumentRun.assurance_document_id == assurance_document_id,
                    UsLaceyEngineDocumentRun.source_sha256 == source_sha256,
                    UsLaceyEngineDocumentRun.engine_version == ai_engine_version,
                    UsLaceyEngineDocumentRun.schema_version == AI_SHADOW_SCHEMA_VERSION,
                    UsLaceyEngineDocumentRun.role_hint == role_hint,
                    UsLaceyEngineDocumentRun.status == "SUCCEEDED",
                )
            )
            if succeeded is not None:
                return
        finally:
            read_session.close()

        try:
            provider = build_ai_provider(config)
            if provider is None:
                return
            with self._vault.materialize_verified_download(
                organization_id=organization_id,
                document_id=vault_public_id,
            ) as download:
                content = b"".join(download.iter_chunks())
            ai_result = provider.extract(filename=filename, content=content)
            verified_ai = verify_ai_evidence(engine2=engine2_resolution, ai=ai_result)
            comparison = reconcile_engine2_with_ai(engine2=engine2_resolution, ai=verified_ai)
            payload = serialize_ai_shadow_run(ai=verified_ai, comparison=comparison)
            payload["architecture"] = AIArchitecture.LEGACY.value
            payload["candidate_count"] = len(verified_ai.candidates)

            write_session: Session = self._session_factory()
            try:
                set_tenant_db_context(write_session, organization_id)
                existing = write_session.scalar(
                    select(UsLaceyEngineDocumentRun.id).where(
                        UsLaceyEngineDocumentRun.organization_id == organization_id,
                        UsLaceyEngineDocumentRun.assurance_document_id == assurance_document_id,
                        UsLaceyEngineDocumentRun.source_sha256 == source_sha256,
                        UsLaceyEngineDocumentRun.engine_version == ai_engine_version,
                        UsLaceyEngineDocumentRun.schema_version == AI_SHADOW_SCHEMA_VERSION,
                        UsLaceyEngineDocumentRun.role_hint == role_hint,
                        UsLaceyEngineDocumentRun.status == "SUCCEEDED",
                    )
                )
                if existing is None:
                    write_session.add(
                        UsLaceyEngineDocumentRun(
                            organization_id=organization_id,
                            operation_id=operation_id,
                            operation_document_id=operation_document_id,
                            assurance_document_id=assurance_document_id,
                            engine_version=ai_engine_version,
                            schema_version=AI_SHADOW_SCHEMA_VERSION,
                            source_sha256=source_sha256,
                            role_hint=role_hint,
                            status="SUCCEEDED",
                            resolution_json=payload,
                        )
                    )
                write_session.commit()
            except Exception:
                write_session.rollback()
                raise
            finally:
                write_session.close()

            LOGGER.info(
                "Lacey legacy AI shadow persisted",
                extra={
                    "architecture": AIArchitecture.LEGACY.value,
                    "organization_id": organization_id,
                    "operation_id": operation_id,
                    "operation_document_id": operation_document_id,
                    "assurance_document_id": assurance_document_id,
                    "assurance_public_id": str(assurance_public_id),
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
            LOGGER.exception(
                "Lacey AI extraction shadow failed",
                extra={
                    "architecture": AIArchitecture.LEGACY.value,
                    "organization_id": organization_id,
                    "operation_id": operation_id,
                    "assurance_document_id": assurance_document_id,
                    "ai_provider": config.provider,
                    "ai_model": config.model,
                },
            )
            failure_session: Session = self._session_factory()
            try:
                set_tenant_db_context(failure_session, organization_id)
                failed = failure_session.scalar(
                    select(UsLaceyEngineDocumentRun.id).where(
                        UsLaceyEngineDocumentRun.organization_id == organization_id,
                        UsLaceyEngineDocumentRun.assurance_document_id == assurance_document_id,
                        UsLaceyEngineDocumentRun.source_sha256 == source_sha256,
                        UsLaceyEngineDocumentRun.engine_version == ai_engine_version,
                        UsLaceyEngineDocumentRun.schema_version == AI_SHADOW_SCHEMA_VERSION,
                        UsLaceyEngineDocumentRun.role_hint == role_hint,
                        UsLaceyEngineDocumentRun.status == "FAILED",
                    )
                )
                if failed is None:
                    failure_session.add(
                        UsLaceyEngineDocumentRun(
                            organization_id=organization_id,
                            operation_id=operation_id,
                            operation_document_id=operation_document_id,
                            assurance_document_id=assurance_document_id,
                            engine_version=ai_engine_version,
                            schema_version=AI_SHADOW_SCHEMA_VERSION,
                            source_sha256=source_sha256,
                            role_hint=role_hint,
                            status="FAILED",
                            safe_error_code="AI_EXTRACTION_SHADOW_FAILED",
                            safe_error_message="AI extraction shadow did not complete.",
                        )
                    )
                failure_session.commit()
            except Exception:
                failure_session.rollback()
                LOGGER.exception(
                    "Unable to persist Lacey AI extraction shadow failure",
                    extra={
                        "architecture": AIArchitecture.LEGACY.value,
                        "organization_id": organization_id,
                        "operation_id": operation_id,
                        "assurance_document_id": assurance_document_id,
                    },
                )
            finally:
                failure_session.close()

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


    def _after_ai_shadow_background(
        self,
        *,
        organization_id: int,
        operation_id: int,
        source_set_fingerprint: str,
    ) -> None:
        """Optional post-shadow hook. Base service keeps AI strictly observational."""
        return None

    def _dispatch_ai_extractors_background(
        self,
        *,
        config: AIProviderConfig,
        organization_id: int,
        operation_id: int,
        documents: tuple[_AIShadowDocumentContext, ...],
        source_set_fingerprint: str,
    ) -> None:
        """Run non-authoritative AI after deterministic commit, never on critical path."""
        if not ai_shadow_enabled(config) or not documents:
            return

        key = (organization_id, operation_id, source_set_fingerprint)
        with _AI_BACKGROUND_LOCK:
            if key in _AI_BACKGROUND_INFLIGHT:
                LOGGER.info(
                    "Lacey AI shadow dispatch already in flight",
                    extra={
                        "organization_id": organization_id,
                        "operation_id": operation_id,
                        "source_set_fingerprint": source_set_fingerprint,
                    },
                )
                return
            _AI_BACKGROUND_INFLIGHT.add(key)

        def run() -> None:
            try:
                self._dispatch_ai_extractors(
                    config=config,
                    organization_id=organization_id,
                    operation_id=operation_id,
                    documents=documents,
                    source_set_fingerprint=source_set_fingerprint,
                )
                self._after_ai_shadow_background(
                    organization_id=organization_id,
                    operation_id=operation_id,
                    source_set_fingerprint=source_set_fingerprint,
                )
            except Exception:
                LOGGER.exception(
                    "Lacey asynchronous AI shadow failed",
                    extra={
                        "organization_id": organization_id,
                        "operation_id": operation_id,
                        "ai_provider": config.provider,
                        "ai_model": config.model,
                        "source_set_fingerprint": source_set_fingerprint,
                    },
                )
            finally:
                with _AI_BACKGROUND_LOCK:
                    _AI_BACKGROUND_INFLIGHT.discard(key)

        threading.Thread(
            target=run,
            name=f"lacey-ai-shadow-{operation_id}",
            daemon=True,
        ).start()

    def resolve_operation_with_engine2(
        self,
        *,
        organization_id: int,
        operation_id: int,
    ) -> ShadowAggregationResult:
        """Resolve deterministic Engine 2 without holding DB transactions over I/O.

        Phase 1 reads and snapshots the current source set in a short transaction.
        Phase 2 performs Vault reads and deterministic parsing with no Session open.
        Phase 3 re-validates the source fingerprint and persists immutable results in
        a fresh short transaction. AI shadow work is launched only after commit.
        """

        # ---- Phase 1: short read transaction ---------------------------------
        read_session: Session = self._session_factory()
        try:
            set_tenant_db_context(read_session, organization_id)
            rows = read_session.execute(
                select(UsLaceyOperationDocument, AssuranceDocument, VaultDocument)
                .join(
                    AssuranceDocument,
                    (AssuranceDocument.id == UsLaceyOperationDocument.assurance_document_id)
                    & (
                        AssuranceDocument.organization_id
                        == UsLaceyOperationDocument.organization_id
                    ),
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

            cached_bundles: dict[int, BundleResolution] = {}
            for link, assurance, vault in rows:
                success_identity = self._engine2_document_run_identity(
                    organization_id=organization_id,
                    assurance_document_id=assurance.id,
                    source_sha256=vault.sha256,
                    role_hint=link.document_role,
                    status="SUCCEEDED",
                )
                run = self._find_engine2_document_run(
                    read_session,
                    **success_identity,
                )
                if run is not None:
                    cached_bundles[int(assurance.id)] = deserialize_bundle_resolution(
                        run.resolution_json
                    )

        finally:
            # Session.close() rolls back the read-only transaction, releases the
            # connection to the pool, and leaves already-loaded scalar state usable.
            read_session.close()

        # ---- Phase 2: external/local processing with no DB Session open -------
        def process_one(row):
            link, assurance, vault = row
            cached = cached_bundles.get(int(assurance.id))
            if cached is not None:
                return link, assurance, vault, cached
            with self._vault.materialize_verified_download(
                organization_id=organization_id,
                document_id=vault.public_id,
            ) as download:
                bundle = process_bundle(
                    filename=vault.original_filename,
                    content=b"".join(download.iter_chunks()),
                    role_hint=link.document_role,
                )
            return link, assurance, vault, bundle

        batch = self._process_engine2_document_batch(
            documents=tuple(rows),
            process_one=process_one,
        )

        ai_config = AIProviderConfig.from_env()
        ai_documents: list[_AIShadowDocumentContext] = []
        inputs: list[ShipmentDocumentInput] = []
        for success in batch.succeeded:
            link, assurance, vault, bundle = success.result
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
                    "Skipping legacy AI shadow for multi-document bundle",
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

        shipment_resolution = (
            process_shipment(documents=inputs, ruleset=self._ruleset)
            if batch.status == "SUCCEEDED"
            else None
        )

        # ---- Phase 3: short validation + persistence transaction --------------
        write_session: Session = self._session_factory()
        try:
            set_tenant_db_context(write_session, organization_id)
            current_rows = write_session.execute(
                select(UsLaceyOperationDocument, AssuranceDocument, VaultDocument)
                .join(
                    AssuranceDocument,
                    (AssuranceDocument.id == UsLaceyOperationDocument.assurance_document_id)
                    & (
                        AssuranceDocument.organization_id
                        == UsLaceyOperationDocument.organization_id
                    ),
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
            current_fingerprint = source_set_fingerprint(
                organization_id=organization_id,
                operation_id=operation_id,
                documents=[(row[0], row[2]) for row in current_rows],
                engine_version=self._engine_version,
                ruleset_version=self._ruleset.version,
            )
            if current_fingerprint != fingerprint:
                write_session.rollback()
                LOGGER.info(
                    "Engine 2 source set changed before deterministic commit",
                    extra={
                        "organization_id": organization_id,
                        "operation_id": operation_id,
                        "expected_source_set_fingerprint": fingerprint,
                        "current_source_set_fingerprint": current_fingerprint,
                    },
                )
                return ShadowAggregationResult(
                    "SOURCE_SET_CHANGED",
                    succeeded_document_count=len(batch.succeeded),
                    failed_document_count=len(batch.failed),
                )

            for success in batch.succeeded:
                link, assurance, vault, bundle = success.result
                success_identity = self._engine2_document_run_identity(
                    organization_id=organization_id,
                    assurance_document_id=assurance.id,
                    source_sha256=vault.sha256,
                    role_hint=link.document_role,
                    status="SUCCEEDED",
                )
                if self._find_engine2_document_run(write_session, **success_identity) is None:
                    write_session.add(
                        UsLaceyEngineDocumentRun(
                            **success_identity,
                            operation_id=operation_id,
                            operation_document_id=link.id,
                            resolution_json=serialize_bundle_resolution(bundle),
                        )
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
                    write_session,
                    operation_id=operation_id,
                    operation_document_id=link.id,
                    identity=failed_identity,
                    safe_error_code=failure.safe_error_code,
                    safe_error_message=failure.safe_error_message,
                )

            if batch.status != "SUCCEEDED":
                write_session.commit()
                result = ShadowAggregationResult(
                    "BLOCKED_PARTIAL" if batch.status == "PARTIAL" else "FAILED",
                    succeeded_document_count=len(batch.succeeded),
                    failed_document_count=len(batch.failed),
                )
            else:
                existing = write_session.scalar(
                    select(UsLaceyEngineShipmentRun).where(
                        UsLaceyEngineShipmentRun.organization_id == organization_id,
                        UsLaceyEngineShipmentRun.operation_id == operation_id,
                        UsLaceyEngineShipmentRun.source_set_fingerprint == fingerprint,
                        UsLaceyEngineShipmentRun.engine_version == self._engine_version,
                        UsLaceyEngineShipmentRun.ruleset_version == self._ruleset.version,
                        UsLaceyEngineShipmentRun.schema_version
                        == SHIPMENT_RESOLUTION_SCHEMA_VERSION,
                    )
                )
                if existing is None:
                    assert shipment_resolution is not None
                    snapshot = UsLaceyEngineShipmentRun(
                        organization_id=organization_id,
                        operation_id=operation_id,
                        engine_version=shipment_resolution.engine_version,
                        ruleset_version=shipment_resolution.ruleset_version,
                        schema_version=SHIPMENT_RESOLUTION_SCHEMA_VERSION,
                        source_set_fingerprint=fingerprint,
                        document_count=len(inputs),
                        readiness=shipment_resolution.readiness.value,
                        resolution_json=serialize_shipment_resolution(shipment_resolution),
                    )
                    write_session.add(snapshot)
                    write_session.flush()
                    shipment_run_id = int(snapshot.id)
                else:
                    shipment_run_id = int(existing.id)
                write_session.commit()
                result = ShadowAggregationResult(
                    "SUCCEEDED",
                    shipment_run_id,
                    succeeded_document_count=len(batch.succeeded),
                )
        except Exception:
            write_session.rollback()
            raise
        finally:
            write_session.close()

        # AI is intentionally launched after deterministic commit. Provider latency,
        # rate limits, timeouts and failures can no longer hold or roll back Engine 2.
        self._dispatch_ai_extractors_background(
            config=ai_config,
            organization_id=organization_id,
            operation_id=operation_id,
            documents=tuple(ai_documents),
            source_set_fingerprint=fingerprint,
        )
        return result



def regenerate_operation_engine2_dossier(
    *,
    organization_id: int,
    operation_id: int,
    session_factory=get_us_lacey_db_session,
) -> ShadowAggregationResult:
    """Publish the Engine 2 shipment snapshot for the current source-set contract.

    Historical shipment snapshots are immutable. A document-set, Engine 2, ruleset
    or shipment-schema change therefore invalidates the old fingerprint rather than
    mutating old evidence. This helper materializes a fresh snapshot using the exact
    current contract and is intended for worker/admin execution only; HTTP read paths
    must never call it synchronously.
    """
    settings = build_us_lacey_storage_settings()
    vault = VaultService(
        storage_settings=settings,
        storage=get_us_lacey_storage_client(),
        session_factory=session_factory,
    )
    return UsLaceyEngine2Service(
        session_factory=session_factory,
        vault_service=vault,
    ).resolve_operation_with_engine2(
        organization_id=organization_id,
        operation_id=operation_id,
    )
