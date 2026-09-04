"""One-job U.S. worker execution built on the mature Assurance processor."""
from __future__ import annotations

from dataclasses import dataclass
import logging

from sqlalchemy import select

from litoral_trace.assurance.processing import AssuranceProcessingService
from litoral_trace.db.models import AssuranceDocument, UsLaceyOperation
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.vault import VaultService
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.jobs import (
    claim_next_us_lacey_job,
    complete_us_lacey_job,
    fail_us_lacey_job,
    recover_stale_us_lacey_jobs,
)
from litoral_trace.us_lacey.projection import (
    project_assurance_document_to_us_lacey,
    refresh_us_lacey_operation_status,
)
from litoral_trace.us_lacey.lacey_engine_service import ENGINE2_SHADOW, UsLaceyEngine2Service, engine2_mode
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
        # The U.S. product is intentionally isolated from Argentina lot/shipment
        # entities. Document extraction stays shared; legacy entity matching does not.
        enable_entity_matching=False,
    )


def _shadow_engine2(*, organization_id: int, operation_id: int) -> None:
    """Best-effort only: never changes authoritative job/projection semantics."""
    if engine2_mode() != ENGINE2_SHADOW:
        return
    settings = build_us_lacey_storage_settings()
    vault = VaultService(storage_settings=settings, storage=get_us_lacey_storage_client(), session_factory=get_us_lacey_db_session)
    try:
        UsLaceyEngine2Service(vault_service=vault).resolve_operation_with_engine2(organization_id=organization_id, operation_id=operation_id)
    except Exception:
        # The isolated service records per-document safe failures where possible;
        # shadow faults intentionally cannot fail the authoritative worker job.
        LOGGER.exception("Lacey Engine 2 shadow resolution failed", extra={"organization_id": organization_id, "operation_id": operation_id})
        return


def _assurance_public_id(*, organization_id: int, document_id: int):
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

        projection = project_assurance_document_to_us_lacey(
            organization_id=job.organization_id,
            operation_id=job.operation_id,
            assurance_document_id=job.assurance_document_id,
        )
        if not complete_us_lacey_job(job_id=job.id, worker_id=worker_id):
            raise UsLaceyWorkerError("Processing job could not be completed atomically.")
        operation_status = _refresh_operation(
            organization_id=job.organization_id,
            operation_id=job.operation_id,
        )
        _shadow_engine2(organization_id=job.organization_id, operation_id=job.operation_id)
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
