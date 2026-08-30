"""Customer-facing U.S. operation orchestration.

This module is deliberately thin: it composes the existing tenant-safe operation,
Vault-first ingestion and durable PostgreSQL queue primitives without creating a
second storage or job system for the U.S. product.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from uuid import UUID

from sqlalchemy import select

from litoral_trace.db.models import UsLaceyOperation
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.access import require_us_lacey_operational_access
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.ingestion import UsLaceyIngestionResult, UsLaceyIngestionService
from litoral_trace.us_lacey.jobs import UsLaceyJob, enqueue_us_lacey_document_job
from litoral_trace.us_lacey.operations import OperationSnapshot, UsLaceyOperationService


class UsLaceyWorkflowError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class UsLaceyQueuedUpload:
    ingestion: UsLaceyIngestionResult
    job: UsLaceyJob


def create_us_lacey_customer_operation(
    *,
    organization_id: int,
    user_id: int,
    client_reference: str,
    importer_name: str | None = None,
    consignee_name: str | None = None,
    broker_name: str | None = None,
    supplier_name: str | None = None,
    operation_date: date | None = None,
    line_references: tuple[str, ...] | None = None,
    operations: UsLaceyOperationService | None = None,
) -> OperationSnapshot:
    """Create one billable operation after server-side entitlement verification."""
    require_us_lacey_operational_access(organization_id=organization_id)
    service = operations or UsLaceyOperationService()
    return service.create_operation(
        organization_id=organization_id,
        created_by_user_id=user_id,
        client_reference=client_reference,
        importer_name=importer_name,
        consignee_name=consignee_name,
        broker_name=broker_name,
        supplier_name=supplier_name,
        operation_date=operation_date,
        line_references=line_references,
        consume_subscription_slot=True,
    )


def _mark_operation_processing(*, organization_id: int, operation_id: int) -> None:
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
            raise UsLaceyWorkflowError("Operation not found.")
        if operation.status not in {"COMPLETED", "FAILED"}:
            operation.status = "PROCESSING"
            operation.review_result = None
        session.commit()
    except UsLaceyWorkflowError:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise UsLaceyWorkflowError("Unable to update operation processing state.") from exc
    finally:
        session.close()


def upload_and_enqueue_us_lacey_document(
    *,
    organization_id: int,
    user_id: int,
    operation_public_id: UUID | str,
    filename: str,
    content_type: str,
    content: bytes,
    document_role: str = "UNKNOWN",
    ingestion: UsLaceyIngestionService | None = None,
    operations: UsLaceyOperationService | None = None,
) -> UsLaceyQueuedUpload:
    """Persist the immutable original, link it to the operation, then queue processing."""
    require_us_lacey_operational_access(organization_id=organization_id)
    operation_service = operations or UsLaceyOperationService()
    operation_id = operation_service.get_internal_id(
        organization_id=organization_id,
        operation_public_id=operation_public_id,
    )
    ingestion_service = ingestion or UsLaceyIngestionService()
    ingested = ingestion_service.ingest_document(
        organization_id=organization_id,
        user_id=user_id,
        operation_public_id=operation_public_id,
        filename=filename,
        content_type=content_type,
        content=content,
        document_role=document_role,
    )
    try:
        job = enqueue_us_lacey_document_job(
            organization_id=organization_id,
            operation_id=operation_id,
            assurance_document_id=ingested.assurance_document_id,
        )
        _mark_operation_processing(
            organization_id=organization_id,
            operation_id=operation_id,
        )
        return UsLaceyQueuedUpload(ingestion=ingested, job=job)
    except Exception as exc:
        # The original remains intentionally preserved in private Vault even when
        # queueing fails. Retrying is safe because ingestion is SHA-256 idempotent
        # and the queue has a tenant+operation+document uniqueness constraint.
        if isinstance(exc, UsLaceyWorkflowError):
            raise
        raise UsLaceyWorkflowError(
            "The document was stored, but processing could not be queued. Retry is safe."
        ) from exc
