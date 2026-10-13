"""Same-tenant, current-source, verified PDF delivery for shipment review.

Never expose a direct object-storage URL. Every request rechecks current finalized
source membership, storage integrity, content-type, and auditable access.
"""
from __future__ import annotations

from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from litoral_trace.services.vault import (
    VaultService, VaultConflictError, VaultIntegrityError, VaultNotFoundError,
    VaultPersistenceError, VaultStorageOperationError, VaultValidationError,
)
from litoral_trace.services.audit import (
    AuditAction, AuditActor, AuditOutcome, record_audit_event,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.web.us_lacey_assurance_v2_runtime import _resolve_current_source_document


def stream_current_pdf(*, identity, operation_public_id: str, assurance_document_id: int):
    # Membership check is tenant-scoped and requires the finalized current
    # source set; old/superseded documents cannot be opened by guessing IDs.
    current = _resolve_current_source_document(
        organization_id=int(identity.organization_id),
        operation_public_id=operation_public_id,
        assurance_document_id=str(assurance_document_id),
    )
    if current is None:
        raise HTTPException(404, "Current source PDF unavailable. Re-upload original evidence if required.")
    try:
        verified = VaultService().materialize_verified_download(
            organization_id=int(identity.organization_id),
            document_id=current["vault_public_id"],
        )
    except (VaultNotFoundError, VaultValidationError):
        raise HTTPException(404, "Original PDF is no longer available.") from None
    except (VaultIntegrityError, VaultConflictError):
        raise HTTPException(409, "Original PDF integrity changed.") from None
    except (VaultPersistenceError, VaultStorageOperationError):
        raise HTTPException(503, "Original source is temporarily unavailable.") from None

    if (verified.document.sha256 != current["sha256"]
            or verified.document.content_type != "application/pdf"):
        verified.close()
        raise HTTPException(409, "Verified PDF integrity or type mismatch.")

    db = get_us_lacey_db_session()
    try:
        set_tenant_db_context(db, int(identity.organization_id))
        record_audit_event(
            db,
            actor=AuditActor(
                organization_id=int(identity.organization_id),
                user_id=int(identity.user_id),
                username=identity.email,
                role="us_lacey_customer",
            ),
            action=AuditAction.VAULT_DOWNLOAD,
            entity_type="assurance_document",
            entity_id=int(assurance_document_id),
            outcome=AuditOutcome.SUCCESS,
            metadata={
                "operation_public_id": operation_public_id,
                "source": "shipment_source_inspector",
            },
            detail="Original PDF inspected during shipment review.",
        )
        db.commit()
    except Exception:
        db.rollback()
        verified.close()
        raise HTTPException(503, "Source access audit unavailable.") from None
    finally:
        db.close()

    def chunks():
        try:
            yield from verified.iter_chunks(chunk_size=64 * 1024)
        finally:
            verified.close()
    return StreamingResponse(chunks(), media_type="application/pdf", headers={
        "Content-Disposition": 'inline; filename="verified-source.pdf"',
        "Content-Length": str(verified.document.size_bytes),
        "Cache-Control": "private, no-store",
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "SAMEORIGIN",
    })
