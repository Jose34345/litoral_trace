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
from litoral_trace.us_lacey.access import (
    UsLaceyOperationalEntitlement,
    require_us_lacey_operational_access,
)
from litoral_trace.us_lacey.batch_hardening import ShipmentBatchRejected, enforce_shipment_document_budget
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.ingestion import UsLaceyIngestionResult, UsLaceyIngestionService
from litoral_trace.us_lacey.jobs import UsLaceyJob, enqueue_us_lacey_document_job
from litoral_trace.us_lacey.operation_lock import us_lacey_operation_projection_lock
from litoral_trace.us_lacey.operations import OperationSnapshot, UsLaceyOperationService
from litoral_trace.us_lacey.source_sets import seal_current_source_set


class UsLaceyWorkflowError(RuntimeError):
    pass


SANDBOX_MAX_DOCUMENTS_PER_OPERATION = 3


def _enforce_sandbox_document_budget(
    *,
    organization_id: int,
    operation_id: int,
    incoming_documents: int,
    entitlement: UsLaceyOperationalEntitlement,
) -> None:
    """Enforce the sandbox LLM budget while the operation advisory lock is held."""

    if not entitlement.is_sandbox:
        return
    if incoming_documents <= 0:
        raise UsLaceyWorkflowError(
            "Choose at least one shipment or supplier document."
        )

    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, organization_id)
        operation = session.scalar(
            select(UsLaceyOperation).where(
                UsLaceyOperation.organization_id == int(organization_id),
                UsLaceyOperation.id == int(operation_id),
            )
        )
        if operation is None:
            raise UsLaceyWorkflowError("Operation not found.")

        current_count = int(operation.document_count or 0)
        if (
            current_count + int(incoming_documents)
            > SANDBOX_MAX_DOCUMENTS_PER_OPERATION
        ):
            raise UsLaceyWorkflowError(
                "Sandbox workspaces can process at most "
                f"{SANDBOX_MAX_DOCUMENTS_PER_OPERATION} documents per operation."
            )
    finally:
        session.close()


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
    require_us_lacey_operational_access(
        organization_id=organization_id,
        require_operation_slot=True,
    )
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
    """Persist one-shipment evidence, link it, then queue bounded processing."""
    # This operation has already consumed its plan slot. Customers must always be
    # able to finish uploads/review/exports for existing work, even at quota.
    entitlement = require_us_lacey_operational_access(
        organization_id=organization_id,
        require_operation_slot=False,
    )
    # Fail before Vault writes and before queue creation when a spreadsheet is a
    # bulk/multi-shipment dataset. Such datasets belong to the benchmark importer.
    try:
        enforce_shipment_document_budget(filename=filename, content=content)
    except ShipmentBatchRejected as exc:
        raise UsLaceyWorkflowError(exc.safe_message) from exc

    operation_service = operations or UsLaceyOperationService()
    operation_id = operation_service.get_internal_id(
        organization_id=organization_id,
        operation_public_id=operation_public_id,
    )
    ingestion_service = ingestion or UsLaceyIngestionService()

    # Source membership mutation and operation-level finalization share one
    # PostgreSQL advisory lock. A worker can therefore observe either the complete
    # previous generation or this newly sealed generation, never an interleaving.
    with us_lacey_operation_projection_lock(
        organization_id=organization_id,
        operation_id=operation_id,
    ):
        _enforce_sandbox_document_budget(
            organization_id=organization_id,
            operation_id=operation_id,
            incoming_documents=1,
            entitlement=entitlement,
        )
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
            # A one-document request is an explicitly complete source set. Multi-file
            # requests use the batch helper below so no worker can observe a prefix.
            seal_current_source_set(organization_id=organization_id, operation_id=operation_id)
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


def upload_and_enqueue_us_lacey_document_batch(
    *,
    organization_id: int,
    user_id: int,
    operation_public_id: UUID | str,
    documents: tuple[tuple[str, str, bytes, str], ...],
    ingestion: UsLaceyIngestionService | None = None,
    operations: UsLaceyOperationService | None = None,
) -> tuple[UsLaceyQueuedUpload, ...]:
    """Attach an entire HTTP batch before sealing or making any job eligible."""
    if not documents:
        raise UsLaceyWorkflowError("Choose at least one shipment or supplier document.")

    entitlement = require_us_lacey_operational_access(
        organization_id=organization_id,
        require_operation_slot=False,
    )

    # Validate the full HTTP batch before the first Vault write. A rejected
    # multi-shipment source must not leave a partially-attached source set behind.
    for filename, _content_type, content, _document_role in documents:
        if not content:
            raise UsLaceyWorkflowError("The uploaded document is empty.")
        try:
            enforce_shipment_document_budget(filename=filename, content=content)
        except ShipmentBatchRejected as exc:
            raise UsLaceyWorkflowError(exc.safe_message) from exc

    operation_service = operations or UsLaceyOperationService()
    operation_id = operation_service.get_internal_id(
        organization_id=organization_id, operation_public_id=operation_public_id,
    )
    ingestion_service = ingestion or UsLaceyIngestionService()

    # Hold one lock for the entire request: all documents become visible together,
    # then exactly one revision is sealed and only then are its jobs made eligible.
    with us_lacey_operation_projection_lock(
        organization_id=organization_id,
        operation_id=operation_id,
    ):
        _enforce_sandbox_document_budget(
            organization_id=organization_id,
            operation_id=operation_id,
            incoming_documents=len(documents),
            entitlement=entitlement,
        )
        ingested = tuple(
            ingestion_service.ingest_document(
                organization_id=organization_id, user_id=user_id,
                operation_public_id=operation_public_id, filename=filename,
                content_type=content_type, content=content, document_role=document_role,
            )
            for filename, content_type, content, document_role in documents
        )
        seal_current_source_set(organization_id=organization_id, operation_id=operation_id)
        try:
            queued = tuple(
                UsLaceyQueuedUpload(
                    ingestion=item,
                    job=enqueue_us_lacey_document_job(
                        organization_id=organization_id, operation_id=operation_id,
                        assurance_document_id=item.assurance_document_id,
                    ),
                )
                for item in ingested
            )
            _mark_operation_processing(organization_id=organization_id, operation_id=operation_id)
            return queued
        except Exception as exc:
            raise UsLaceyWorkflowError(
                "The document batch was stored, but processing could not be queued. Retry is safe."
            ) from exc
