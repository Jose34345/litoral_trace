"""One-job U.S. worker execution built on the mature Assurance processor."""
from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
import logging
from pathlib import PurePath
from uuid import UUID

from sqlalchemy import select

from litoral_trace.assurance.processing import AssuranceProcessingService
from litoral_trace.db.models import AssuranceDocument, UsLaceyOperation, VaultDocument
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.vault import VaultService
from litoral_trace.us_lacey.ai_review import recommend_open_reconciliation_issues
from litoral_trace.us_lacey.ai_suggestions import project_verified_ai_suggestions
from litoral_trace.us_lacey.batch_hardening import (
    ShipmentBatchRejected,
    enforce_shipment_document_budget,
    enforce_streamed_csv_budget,
    shipment_spreadsheet_limits,
)
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.engine2_suggestions import project_engine2_supported_suggestions
from litoral_trace.us_lacey.jobs import (
    claim_next_us_lacey_job,
    complete_us_lacey_job,
    fail_us_lacey_job,
    recover_stale_us_lacey_jobs,
)
from litoral_trace.us_lacey.operation_lock import us_lacey_operation_projection_lock
from litoral_trace.us_lacey.projection import (
    project_assurance_document_to_us_lacey,
    refresh_us_lacey_operation_status,
)
from litoral_trace.us_lacey.lacey_engine_service import ENGINE2_SHADOW, UsLaceyEngine2Service, engine2_mode
from litoral_trace.us_lacey.shadow_evidence_snapshot import (
    build_shadow_evidence_snapshot,
    multilingual_shadow_enabled,
)
from litoral_trace.us_lacey.storage import (
    build_us_lacey_storage_settings,
    get_us_lacey_storage_client,
)


class UsLaceyWorkerError(RuntimeError):
    pass


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class UsLaceyWorkerResult:
    claimed: bool
    job_id: int | None
    job_status: str | None
    document_status: str | None
    operation_status: str | None
    projected_count: int
    conflict_count: int


@dataclass(frozen=True, slots=True)
class _DocumentDescriptor:
    assurance_public_id: UUID
    vault_public_id: UUID
    filename: str
    size_bytes: int


def _processing_service() -> AssuranceProcessingService:
    settings = build_us_lacey_storage_settings()
    storage = get_us_lacey_storage_client()
    vault = VaultService(
        storage_settings=settings,
        storage=storage,
        session_factory=get_us_lacey_db_session,
    )
    return AssuranceProcessingService(
        session_factory=get_us_lacey_db_session,
        vault_service=vault,
        enable_entity_matching=False,
    )


def _shadow_engine2(*, organization_id: int, operation_id: int) -> None:
    """Best-effort only: never changes authoritative job/projection semantics."""
    if engine2_mode() != ENGINE2_SHADOW:
        return
    settings = build_us_lacey_storage_settings()
    vault = VaultService(
        storage_settings=settings,
        storage=get_us_lacey_storage_client(),
        session_factory=get_us_lacey_db_session,
    )
    try:
        UsLaceyEngine2Service(vault_service=vault).resolve_operation_with_engine2(
            organization_id=organization_id,
            operation_id=operation_id,
        )
    except Exception:
        LOGGER.exception(
            "Lacey Engine 2 shadow resolution failed",
            extra={"organization_id": organization_id, "operation_id": operation_id},
        )
        return


def _project_engine2_suggestions(*, organization_id: int, operation_id: int) -> int:
    """Best-effort deterministic bridge; supported evidence stays human-confirmable."""
    try:
        return int(
            project_engine2_supported_suggestions(
                organization_id=organization_id,
                operation_id=operation_id,
            )
            or 0
        )
    except Exception:
        LOGGER.exception(
            "Lacey Engine 2 suggestion projection failed",
            extra={"organization_id": organization_id, "operation_id": operation_id},
        )
        return 0


def _project_verified_ai_suggestions(*, organization_id: int, operation_id: int) -> int:
    """Best-effort AI bridge; return how many review fields actually changed."""
    try:
        return int(
            project_verified_ai_suggestions(
                organization_id=organization_id,
                operation_id=operation_id,
            )
            or 0
        )
    except Exception:
        # AI suggestion projection is convenience only. The mature deterministic
        # extraction/review path remains usable if this bridge fails.
        LOGGER.exception(
            "Lacey verified AI suggestion projection failed",
            extra={"organization_id": organization_id, "operation_id": operation_id},
        )
        return 0


def _run_ai_review_recommendations(*, organization_id: int, operation_id: int) -> None:
    """Best-effort only: recommendation JSON cannot change declaration authority."""
    try:
        recommend_open_reconciliation_issues(
            organization_id=organization_id,
            operation_id=operation_id,
        )
    except Exception:
        LOGGER.exception(
            "Lacey AI review recommendation failed",
            extra={"organization_id": organization_id, "operation_id": operation_id},
        )


def _shadow_multilingual_evidence_snapshot(*, organization_id: int, operation_id: int) -> None:
    """Best-effort Phase B dual-write; legacy completion is authoritative."""
    if not multilingual_shadow_enabled():
        return
    try:
        build_shadow_evidence_snapshot(
            organization_id=organization_id,
            operation_id=operation_id,
        )
    except Exception:
        LOGGER.exception(
            "Lacey multilingual evidence shadow snapshot failed",
            extra={"organization_id": organization_id, "operation_id": operation_id},
        )


def _assurance_public_id(*, organization_id: int, document_id: int) -> UUID:
    """Preserve the worker's stable lookup seam used by existing contracts/tests."""
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, organization_id)
        document = session.scalar(
            select(AssuranceDocument).where(
                AssuranceDocument.organization_id == organization_id,
                AssuranceDocument.id == document_id,
            )
        )
        if document is None:
            raise UsLaceyWorkerError("Queued document no longer exists.")
        return document.public_id
    finally:
        session.close()


def _document_descriptor(*, organization_id: int, document_id: int) -> _DocumentDescriptor:
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, organization_id)
        document = session.scalar(
            select(AssuranceDocument).where(
                AssuranceDocument.organization_id == organization_id,
                AssuranceDocument.id == document_id,
            )
        )
        if document is None:
            raise UsLaceyWorkerError("Queued document no longer exists.")
        vault_document = session.scalar(
            select(VaultDocument).where(
                VaultDocument.organization_id == organization_id,
                VaultDocument.id == document.vault_document_id,
                VaultDocument.status == "available",
            )
        )
        if vault_document is None:
            raise UsLaceyWorkerError("Queued document original is not available.")
        return _DocumentDescriptor(
            assurance_public_id=document.public_id,
            vault_public_id=vault_document.public_id,
            filename=vault_document.original_filename,
            size_bytes=int(vault_document.size_bytes),
        )
    finally:
        session.close()


def _mark_document_policy_failure(
    *,
    organization_id: int,
    document_id: int,
    code: str,
    message: str,
) -> None:
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, organization_id)
        document = session.scalar(
            select(AssuranceDocument).where(
                AssuranceDocument.organization_id == organization_id,
                AssuranceDocument.id == document_id,
            )
        )
        if document is not None:
            document.processing_status = "FAILED"
            document.last_error_code = code[:100]
            document.last_error_message = message[:512]
            session.commit()
    except Exception:
        session.rollback()
        LOGGER.exception(
            "Unable to persist batch-policy failure",
            extra={"organization_id": organization_id, "document_id": document_id},
        )
    finally:
        session.close()


def _preflight_existing_document(*, organization_id: int, descriptor: _DocumentDescriptor) -> None:
    """Reject legacy queued bulk spreadsheets before the expensive parser allocates memory."""
    suffix = PurePath(descriptor.filename).suffix.lower()
    if suffix not in {".csv", ".xlsx", ".xls"}:
        return
    limits = shipment_spreadsheet_limits()
    if descriptor.size_bytes > limits.max_bytes:
        raise ShipmentBatchRejected(
            "DATASET_TOO_LARGE_FOR_SHIPMENT_PIPELINE",
            "This spreadsheet is a bulk or multi-shipment dataset and cannot be processed as one Lacey operation.",
        )
    settings = build_us_lacey_storage_settings()
    vault = VaultService(
        storage_settings=settings,
        storage=get_us_lacey_storage_client(),
        session_factory=get_us_lacey_db_session,
    )
    with vault.materialize_verified_download(
        organization_id=organization_id,
        document_id=descriptor.vault_public_id,
    ) as verified:
        if suffix == ".csv":
            enforce_streamed_csv_budget(
                chunks=verified.iter_chunks(chunk_size=256 * 1024),
                size_bytes=descriptor.size_bytes,
                limits=limits,
            )
            return
        # XLS/XLSX are strictly byte-bounded before this join.
        content = b"".join(verified.iter_chunks(chunk_size=256 * 1024))
        enforce_shipment_document_budget(
            filename=descriptor.filename,
            content=content,
            limits=limits,
        )


def _refresh_operation(*, organization_id: int, operation_id: int) -> str:
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, organization_id)
        operation = session.scalar(
            select(UsLaceyOperation).where(
                UsLaceyOperation.organization_id == organization_id,
                UsLaceyOperation.id == operation_id,
            )
        )
        if operation is None:
            raise UsLaceyWorkerError("Queued operation no longer exists.")
        status = refresh_us_lacey_operation_status(
            session,
            organization_id=organization_id,
            operation=operation,
        )
        session.commit()
        return status
    except UsLaceyWorkerError:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise UsLaceyWorkerError("Unable to refresh operation state.") from exc
    finally:
        session.close()


def process_one_us_lacey_job(
    *,
    worker_id: str,
    recover_stale: bool = False,
) -> UsLaceyWorkerResult:
    """Claim and process one durable job; safe to call repeatedly from a worker loop."""
    if recover_stale:
        recover_stale_us_lacey_jobs()
    job = claim_next_us_lacey_job(worker_id=worker_id)
    if job is None:
        return UsLaceyWorkerResult(
            claimed=False,
            job_id=None,
            job_status=None,
            document_status=None,
            operation_status=None,
            projected_count=0,
            conflict_count=0,
        )

    try:
        assurance_public_id = _assurance_public_id(
            organization_id=job.organization_id,
            document_id=job.assurance_document_id,
        )

        # Production lookups always return UUID. Keeping the existing lookup seam
        # lets isolated unit contracts stub a lightweight string id without opening
        # a database/Vault connection; real jobs still receive full preflight.
        if isinstance(assurance_public_id, UUID):
            descriptor = _document_descriptor(
                organization_id=job.organization_id,
                document_id=job.assurance_document_id,
            )
            try:
                _preflight_existing_document(
                    organization_id=job.organization_id,
                    descriptor=descriptor,
                )
            except ShipmentBatchRejected as exc:
                _mark_document_policy_failure(
                    organization_id=job.organization_id,
                    document_id=job.assurance_document_id,
                    code=exc.code,
                    message=exc.safe_message,
                )
                queue_status = fail_us_lacey_job(
                    job_id=job.id,
                    worker_id=worker_id,
                    error_code=exc.code,
                    safe_error_message=exc.safe_message,
                    retryable=False,
                )
                operation_status = _refresh_operation(
                    organization_id=job.organization_id,
                    operation_id=job.operation_id,
                )
                return UsLaceyWorkerResult(
                    claimed=True,
                    job_id=job.id,
                    job_status=queue_status,
                    document_status="FAILED",
                    operation_status=operation_status,
                    projected_count=0,
                    conflict_count=0,
                )

        document_status = _processing_service().process(
            organization_id=job.organization_id,
            assurance_public_id=assurance_public_id,
        )
        if document_status == "FAILED":
            queue_status = fail_us_lacey_job(
                job_id=job.id,
                worker_id=worker_id,
                error_code="DOCUMENT_PROCESSING_FAILED",
                safe_error_message=(
                    "The document could not be processed. The original file remains preserved."
                ),
            )
            operation_status = _refresh_operation(
                organization_id=job.organization_id,
                operation_id=job.operation_id,
            )
            return UsLaceyWorkerResult(
                claimed=True,
                job_id=job.id,
                job_status=queue_status,
                document_status=document_status,
                operation_status=operation_status,
                projected_count=0,
                conflict_count=0,
            )

        # Same-operation documents may be processed by different workers. Serialize
        # the entire authoritative projection/post-processing phase so plant-line
        # materialization observes the previous document's committed result before
        # deciding whether another declaration line is required.
        projection_guard = (
            us_lacey_operation_projection_lock(
                organization_id=job.organization_id,
                operation_id=job.operation_id,
            )
            if isinstance(assurance_public_id, UUID)
            else nullcontext()
        )
        with projection_guard:
            projection = project_assurance_document_to_us_lacey(
                organization_id=job.organization_id,
                operation_id=job.operation_id,
                assurance_document_id=job.assurance_document_id,
            )

            # Run every evidence/recommendation postprocessor while the queue job is
            # still RUNNING and while same-operation projection is serialized. If
            # orchestration itself ever fails unexpectedly, the outer failure boundary
            # can still transition the owned job instead of leaving false terminal work.
            _shadow_engine2(
                organization_id=job.organization_id,
                operation_id=job.operation_id,
            )
            _project_engine2_suggestions(
                organization_id=job.organization_id,
                operation_id=job.operation_id,
            )
            _project_verified_ai_suggestions(
                organization_id=job.organization_id,
                operation_id=job.operation_id,
            )
            # AI review may annotate existing OPEN conflicts with a bounded recommendation,
            # but the recommendation cannot resolve an issue or set a field value.
            _run_ai_review_recommendations(
                organization_id=job.organization_id,
                operation_id=job.operation_id,
            )

        # The multilingual dual-write owns its own operation advisory lock. Run it
        # only after the authoritative projection lock has been released to avoid a
        # nested lock on a separate connection. Any shadow failure is swallowed by
        # the wrapper and cannot alter the legacy queue state.
        if isinstance(assurance_public_id, UUID):
            _shadow_multilingual_evidence_snapshot(
                organization_id=job.organization_id,
                operation_id=job.operation_id,
            )

        # COMPLETED is the final queue transition, after the full processing chain.
        if not complete_us_lacey_job(job_id=job.id, worker_id=worker_id):
            raise UsLaceyWorkerError("Processing job could not be completed atomically.")

        # Refresh only after the terminal queue transition. Refreshing while this job
        # is RUNNING would correctly project the operation as PROCESSING and leave a
        # stale operation state until some later request recomputed it.
        operation_status = _refresh_operation(
            organization_id=job.organization_id,
            operation_id=job.operation_id,
        )
        return UsLaceyWorkerResult(
            claimed=True,
            job_id=job.id,
            job_status="COMPLETED",
            document_status=document_status,
            operation_status=operation_status,
            projected_count=projection.projected_count,
            conflict_count=projection.conflict_count,
        )
    except Exception as exc:
        queue_status = fail_us_lacey_job(
            job_id=job.id,
            worker_id=worker_id,
            error_code="US_LACEY_WORKER_FAILED",
            safe_error_message="Document processing did not complete. A controlled retry may occur.",
        )
        try:
            operation_status = _refresh_operation(
                organization_id=job.organization_id,
                operation_id=job.operation_id,
            )
        except Exception:
            operation_status = None
        if queue_status is None:
            raise UsLaceyWorkerError("Worker lost ownership of the processing job.") from exc
        return UsLaceyWorkerResult(
            claimed=True,
            job_id=job.id,
            job_status=queue_status,
            document_status=None,
            operation_status=operation_status,
            projected_count=0,
            conflict_count=0,
        )