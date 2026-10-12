"""Atomic one-click review of a reproducible set of documentary candidates.

One reviewer action records multiple field-level V2 decisions sharing ONE
content-addressed batch ID. Every field retains a distinct auditable decision,
exact current document source and human actor. Never approves historical memory,
botanical origin or unresolved customs entry information.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from uuid import UUID

from sqlalchemy import select

from litoral_trace.db.models import (
    ReconciliationIssue, UsLaceyOperation, UsLaceyOperationField,
    UsLaceySourceSetRevision,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.assurance_v2_case_reader import _case
from litoral_trace.us_lacey.assurance_v2_memory import _actor, record_human_decision
from litoral_trace.us_lacey.assurance_v2_review_policy import (
    batch_candidates, batch_fingerprint, simplified_review,
)
from litoral_trace.us_lacey.assurance_v2_source_claim_review import (
    select_reviewable_source_claim,
)
from litoral_trace.us_lacey.audit_trail import append_operation_event
from litoral_trace.us_lacey.assurance_v2_review_adapter import review_journal_enabled
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.ppq505 import validate_ppq_value
from litoral_trace.us_lacey.projection import refresh_us_lacey_operation_status


def approve_assurance_review_batch(
    *, organization_id: int, operation_public_id: str, authenticated_user_id: int,
    reason: str, explicit_confirmation: bool, expected_source_set_fingerprint: str,
    expected_case_fingerprint: str, expected_batch_fingerprint: str,
    session_factory=get_us_lacey_db_session,
) -> dict:
    """Fail-closed transaction. All decisions commit together or none do."""
    try:
        org_id, user_id = int(organization_id), int(authenticated_user_id)
        public_id = UUID(str(operation_public_id))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("Invalid shipment or reviewer") from exc
    justification = str(reason or "").strip()
    if org_id <= 0 or user_id <= 0:
        raise PermissionError("Authenticated reviewer required")
    if (explicit_confirmation is not True or not justification
            or len(justification) > 1000):
        raise ValueError("Explicit review confirmation and reason are required")
    if (len(expected_batch_fingerprint) != 64
            or len(expected_case_fingerprint) != 64
            or len(expected_source_set_fingerprint) != 64):
        raise ValueError("A current source and batch fingerprint are required")

    session = session_factory()
    try:
        set_tenant_db_context(session, org_id)
        _actor(session, org_id, user_id)
        op = session.scalar(select(UsLaceyOperation).where(
            UsLaceyOperation.organization_id == org_id,
            UsLaceyOperation.public_id == public_id,
        ).with_for_update())
        if op is None:
            raise PermissionError("Shipment unavailable")
        if not review_journal_enabled(org_id, op.client_reference):
            raise PermissionError("V2 lab review journal disabled for this shipment")
        if not str(op.client_reference or "").upper().startswith("V2LAB-"):
            raise PermissionError("Batch review is private-lab only")
        revision = session.scalar(select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == org_id,
            UsLaceySourceSetRevision.operation_id == op.id,
            UsLaceySourceSetRevision.is_current.is_(True),
            UsLaceySourceSetRevision.status == "FINALIZED",
        ).with_for_update())
        if revision is None or revision.source_set_fingerprint != expected_source_set_fingerprint:
            raise ValueError("STALE_SOURCE_SET")
        case = _case(session, org_id, public_id)
        if (case is None or case.source_set_revision != revision.id
                or case.authority_fingerprint() != expected_case_fingerprint):
            raise ValueError("STALE_CASE_AUTHORITY")
        plan = simplified_review(case)
        candidates = batch_candidates(case)
        if (not plan["can_batch"] or not candidates
                or batch_fingerprint(case, candidates) != expected_batch_fingerprint):
            raise ValueError("STALE_BATCH_OR_UNRESOLVED_IDENTITY")
        ids = [candidate.field_id for candidate in candidates]
        fields = session.scalars(select(UsLaceyOperationField).where(
            UsLaceyOperationField.organization_id == org_id,
            UsLaceyOperationField.operation_id == op.id,
            UsLaceyOperationField.id.in_(ids),
        ).order_by(UsLaceyOperationField.id).with_for_update()).all()
        by_id = {field.id: field for field in fields}
        if len(fields) != len(ids):
            raise ValueError("BATCH_FIELD_CHANGED")
        conflicts = session.scalars(select(ReconciliationIssue).where(
            ReconciliationIssue.organization_id == org_id,
            ReconciliationIssue.operation_reference == f"us_lacey:{public_id}",
            ReconciliationIssue.status == "OPEN",
            ReconciliationIssue.us_lacey_operation_field_id.in_(ids),
        )).all()
        if conflicts:
            raise ValueError("BATCH_HAS_OPEN_CONFLICTS")
        batch_id = sha256(
            f"{org_id}:{op.id}:{revision.id}:{user_id}:{expected_batch_fingerprint}".encode()
        ).hexdigest()
        now = datetime.now(timezone.utc)
        for candidate in candidates:
            field = by_id[candidate.field_id]
            if (field.field_name != candidate.field_name
                    or (field.merchandise_line_reference or "") != candidate.line_reference
                    or field.human_value or field.reviewed_at is not None
                    or field.field_status not in {"REVIEW", "SUPPORTED", "MISSING"}):
                raise ValueError("BATCH_FIELD_CHANGED")
            source = select_reviewable_source_claim(
                case, claim_id=candidate.claim_id, field_name=candidate.field_name,
                line_reference=candidate.line_reference,
            )
            value = validate_ppq_value(field.field_name, source.normalized_value)
            if value.status.value != "VALID" or value.normalized_value != candidate.value:
                raise ValueError("BATCH_VALUE_CHANGED")
            field.original_value = source.raw_value
            field.normalized_value = source.normalized_value
            field.source_assurance_document_id = source.assurance_document_id
            field.source_page = source.page
            field.source_locator = source.locator
            field.human_value = candidate.value
            field.field_status = "MATCHED"
            field.validation_status = "VALID"
            field.validation_error = None
            field.not_required_reason_code = None
            field.reviewed_by_user_id = user_id
            field.reviewed_at = now
            session.flush()
            record_human_decision(
                session, organization_id=org_id, operation_id=op.id,
                source_set_revision_id=revision.id, authenticated_user_id=user_id,
                action="ACCEPT", field_name=field.field_name,
                line_reference=field.merchandise_line_reference,
                selected_value=candidate.value, reason=justification,
                idempotency_key=f"batch:{batch_id}:{field.id}",
                assurance_document_ids=(source.assurance_document_id,),
                context={"batch_id": batch_id, "batch_fingerprint": expected_batch_fingerprint},
            )
        append_operation_event(
            session, organization_id=org_id, operation_id=op.id,
            actor_type="USER", actor_identity=f"user:{user_id}",
            event_type="HUMAN_REVIEW", event_key=f"v2lab:batch_review:{batch_id}",
            details={"action": "BATCH_REVIEW_CONFIRMED",
                     "batch_id": batch_id,
                     "source_set_revision_id": revision.id,
                     "field_count": len(candidates),
                     "human_actions": 1,
                     "unresolved_external_fields": plan["missing_count"]},
        )
        refresh_us_lacey_operation_status(
            session, organization_id=org_id, operation=op,
        )
        session.commit()
        return {
            "status": "RECORDED", "batch_id": batch_id,
            "reviewed_fields": len(candidates),
            "filing_ready": False,
        }
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
