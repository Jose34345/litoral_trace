"""Owner-lab-only replay of current source documents without re-upload or rebilling.

Reprocessing rebuilds operation/source-set projections and V2 evidence claims from
the original current PDFs. Existing parser caches may be reused when compatible;
this command NEVER silently promotes a parser candidate to human authority.
"""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select

from litoral_trace.db.models import (
    AssuranceDocument, UsLaceyOperation, UsLaceyOperationDocument,
    UsLaceyProcessingJob, VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.audit_trail import (
    OperationActorType, OperationEventType, append_operation_event,
)
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.jobs import UsLaceyJobError
from litoral_trace.us_lacey.operation_lock import us_lacey_operation_projection_lock
from litoral_trace.us_lacey.source_sets import reseal_same_sources_for_reprocessing


def reprocess_existing_v2lab_documents(
    *, organization_id: int, operation_public_id: UUID | str, user_id: int,
    session_factory=get_us_lacey_db_session,
    lock_factory=us_lacey_operation_projection_lock,
) -> int:
    """Atomically reseal and requeue all current lab documents; return their count.

    All mutations, including the append-only request audit, commit together. A
    second click or an operation currently in progress fails without any writes.
    Existing document/Vault identity, immutable review journal and quota rows
    remain untouched; the new source-set revision requires new V2 authority.
    """
    org_id = int(organization_id)
    actor_id = int(user_id)
    if org_id < 1 or actor_id < 1:
        raise UsLaceyJobError("A valid organization and reviewer are required.")
    try:
        operation_id = UUID(str(operation_public_id))
    except (ValueError, TypeError) as exc:
        raise UsLaceyJobError("A valid shipment identifier is required.") from exc

    session = session_factory()
    try:
        set_tenant_db_context(session, org_id)
        operation = session.scalar(
            select(UsLaceyOperation).where(
                UsLaceyOperation.organization_id == org_id,
                UsLaceyOperation.public_id == operation_id,
            )
        )
        if operation is None:
            raise UsLaceyJobError("Shipment not found.")
        if not str(operation.client_reference or "").upper().startswith("V2LAB-"):
            raise UsLaceyJobError("Reprocessing is restricted to private V2 lab shipments.")
        if operation.status not in {"REVIEW_REQUIRED", "READY_FOR_REVIEW", "COMPLETED"}:
            raise UsLaceyJobError("Wait for the current analysis to finish before reprocessing.")
        op_id = int(operation.id)
        # Serialize against upload and worker finalization on this SAME operation.
        # The operation row lock also serializes two simultaneous user requests.
        with lock_factory(organization_id=org_id, operation_id=op_id):
            # Lock in the same order as the worker: advisory lock THEN row lock.
            # Revalidate after waiting; the first SELECT was only a locator.
            session.refresh(operation, with_for_update=True)
            if operation.status not in {"REVIEW_REQUIRED", "READY_FOR_REVIEW", "COMPLETED"}:
                raise UsLaceyJobError("Wait for the current analysis to finish before reprocessing.")
            if not str(operation.client_reference or "").upper().startswith("V2LAB-"):
                raise UsLaceyJobError("Reprocessing is restricted to private V2 lab shipments.")
            rows = session.execute(
                select(UsLaceyOperationDocument, VaultDocument)
                .join(
                    AssuranceDocument,
                    (AssuranceDocument.id == UsLaceyOperationDocument.assurance_document_id)
                    & (AssuranceDocument.organization_id == org_id),
                )
                .join(
                    VaultDocument,
                    (VaultDocument.id == AssuranceDocument.vault_document_id)
                    & (VaultDocument.organization_id == org_id),
                )
                .where(
                    UsLaceyOperationDocument.organization_id == org_id,
                    UsLaceyOperationDocument.operation_id == op_id,
                    UsLaceyOperationDocument.is_current.is_(True),
                )
            ).all()
            if not rows or any(vault.status != "available" for _, vault in rows):
                raise UsLaceyJobError(
                    "An original document is unavailable. Restore evidence before reprocessing."
                )
            jobs = session.scalars(
                select(UsLaceyProcessingJob).where(
                    UsLaceyProcessingJob.organization_id == org_id,
                    UsLaceyProcessingJob.operation_id == op_id,
                ).with_for_update()
            ).all()
            if any(job.status in {"QUEUED", "RETRY", "RUNNING"} for job in jobs):
                raise UsLaceyJobError("This shipment already has processing work.")
            job_by_document = {int(job.assurance_document_id): job for job in jobs}
            if any(
                (job := job_by_document.get(int(link.assurance_document_id))) is None
                or job.status != "COMPLETED"
                for link, _vault in rows
            ):
                raise UsLaceyJobError(
                    "Every current document must finish successfully before reprocessing."
                )

            try:
                revision = reseal_same_sources_for_reprocessing(
                    session, organization_id=org_id, operation_id=op_id,
                )
            except ValueError as exc:
                raise UsLaceyJobError(str(exc)) from exc

            now = datetime.now(timezone.utc)
            for link, _vault in rows:
                job = job_by_document[int(link.assurance_document_id)]
                job.status = "QUEUED"
                job.attempt_count = 0
                job.available_at = now
                job.locked_by = None
                job.locked_at = None
                job.heartbeat_at = None
                job.current_stage = None
                job.stage_started_at = None
                job.started_at = None
                job.completed_at = None
                job.last_error_code = None
                job.last_error_message = None
                job.updated_at = now
            operation.status = "PROCESSING"
            operation.review_result = None
            operation.updated_at = now
            append_operation_event(
                session,
                organization_id=org_id, operation_id=op_id,
                actor_type=OperationActorType.USER,
                actor_identity=f"user:{actor_id}",
                event_type=OperationEventType.HUMAN_REVIEW,
                event_key=f"reprocess:source-revision:{revision.id}",
                details={
                    "action": "reprocess_requested",
                    "source_set_revision_id": int(revision.id),
                    "source_set_generation": int(revision.generation),
                    "document_count": len(rows),
                    "unchanged_document_bytes": True,
                },
            )
            session.commit()
            return len(rows)
    except UsLaceyJobError:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise UsLaceyJobError("Unable to schedule shipment reprocessing.") from exc
    finally:
        session.close()
