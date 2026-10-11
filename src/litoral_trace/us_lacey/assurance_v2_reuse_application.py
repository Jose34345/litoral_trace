"""Authenticated, source-fenced application of historical Assurance V2 evidence."""
from __future__ import annotations

from hashlib import sha256
from uuid import UUID

from sqlalchemy import select

from litoral_trace.db.models import UsLaceyOperation, UsLaceySourceSetRevision
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.assurance_v2_case_reader import _case
from litoral_trace.us_lacey.assurance_v2_memory import (
    _current_claim_snapshot, authorize_reuse_application,
    canonical_claim_snapshot_digest,
)
from litoral_trace.us_lacey.assurance_v2_reuse_guard import current_claim_values_for_reuse
from litoral_trace.us_lacey.db import get_us_lacey_db_session


def apply_authorized_assurance_v2_reuse(
    *, organization_id: int, operation_public_id: str, authenticated_user_id: int,
    memory_public_id: str, line_reference: str, field_name: str,
    reason: str, expected_source_set_fingerprint: str,
    expected_case_fingerprint: str,
    session_factory=get_us_lacey_db_session,
) -> dict:
    """Lock, re-read, authorize, and audit one exact source-set application."""
    try:
        public_id = UUID(str(operation_public_id))
        memory_id = UUID(str(memory_public_id))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("Invalid operation or historical memory reference") from exc
    org_id, user_id = int(organization_id), int(authenticated_user_id)
    if org_id <= 0 or user_id <= 0:
        raise PermissionError("Authenticated tenant reviewer required")
    session = session_factory()
    try:
        set_tenant_db_context(session, org_id)
        operation = session.scalar(select(UsLaceyOperation).where(
            UsLaceyOperation.organization_id == org_id,
            UsLaceyOperation.public_id == public_id,
        ).with_for_update())
        if operation is None:
            raise PermissionError("Operation unavailable")
        revision = session.scalar(select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == org_id,
            UsLaceySourceSetRevision.operation_id == operation.id,
            UsLaceySourceSetRevision.is_current.is_(True),
            UsLaceySourceSetRevision.status == "FINALIZED",
        ).with_for_update())
        if revision is None or revision.source_set_fingerprint != expected_source_set_fingerprint:
            raise ValueError("STALE_SOURCE_SET")
        case = _case(session, org_id, public_id)
        if (
            case is None or case.source_set_revision != revision.id
            or case.authority_fingerprint() != expected_case_fingerprint
            or not any(item.get("status") == "ELIGIBLE"
                       and item.get("id") == str(memory_id)
                       and item.get("line_reference") == line_reference
                       and item.get("field_name") == field_name
                       for item in case.reuses)
        ):
            raise ValueError("STALE_CASE_AUTHORITY")
        snapshot = _current_claim_snapshot(
            session, organization_id=org_id, operation_id=operation.id,
            revision=revision,
        )
        if snapshot is None:
            raise ValueError("SOURCE_CLAIMS_INVALID")
        claims = tuple(claim for source in snapshot["documents"]
                       for claim in source["claims"])
        current_values = current_claim_values_for_reuse(
            claims, field_name=field_name, line_reference=line_reference,
        )
        if current_values is None:
            raise ValueError("SOURCE_CLAIMS_INVALID")
        request_key = sha256(
            f"{org_id}:{operation.id}:{revision.id}:{line_reference}:{field_name}:"
            f"{memory_id}:{user_id}".encode("utf-8")
        ).hexdigest()
        application = authorize_reuse_application(
            session, organization_id=org_id, operation_id=operation.id,
            source_set_revision_id=revision.id,
            authenticated_user_id=user_id, line_reference=line_reference,
            field_name=field_name, memory_public_id=memory_id,
            current_claim_values=current_values,
            current_claim_snapshot_digest=canonical_claim_snapshot_digest(snapshot),
            reason=reason, idempotency_key=f"portal:{request_key}",
        )
        session.commit()
        return {
            "status": application.status,
            "field_name": application.field_name,
            "line_reference": application.line_reference,
            "value": application.value,
            "memory_id": str(application.memory_public_id),
            "origin_document_id": str(application.origin_document_id),
            "audit_event_id": application.audit_event_id,
            "source_claim_snapshot_digest": application.source_claim_snapshot_digest,
        }
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
