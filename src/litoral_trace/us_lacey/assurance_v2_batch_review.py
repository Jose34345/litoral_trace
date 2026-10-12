"""Atomic review of a selected set of current, exact PDF-backed declaration fields.

Batching reduces reviewer clicks, NOT evidence requirements. Every accepted
field receives its own immutable Assurance V2 decision and source link; the
operation, field projections and audit are committed as ONE transaction.
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
from litoral_trace.us_lacey.assurance_v2_memory import _actor
from litoral_trace.us_lacey.assurance_v2_review_adapter import (
    journal_reviewed_field, review_journal_enabled,
)
from litoral_trace.us_lacey.assurance_v2_source_claim_review import (
    select_reviewable_source_claim,
)
from litoral_trace.us_lacey.audit_trail import append_operation_event
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.ppq505 import validate_ppq_value
from litoral_trace.us_lacey.projection import refresh_us_lacey_operation_status
from litoral_trace.us_lacey.regulatory_assessment_snapshot import (
    refresh_regulatory_assessment_snapshot_for_review,
)
from litoral_trace.us_lacey.reconciliation_invariants import reconcile_entered_value_invariant

# These have their own explicit supplier-evidence decision, not a bulk PDF
# review shortcut. Reuse authorization requires independent origin validation.
_SEPARATE_BOTANICAL_AUTHORITY = frozenset(
    {"genus", "species", "country_of_harvest"}
)
_MAX_BATCH = 32


def _eligible_case_choices(case) -> dict[int, dict[str, str]]:
    """Only one conflict-free documentary value per field is eligible."""
    candidates: dict[int, dict[str, str]] = {}
    for exception in case.exceptions:
        if exception.get("blocking") is not True:
            continue
        token = str(exception.get("id") or "")
        if not token.startswith("field:"):
            continue
        try:
            field_id = int(token[6:])
        except ValueError:
            continue
        if exception.get("field_name") in _SEPARATE_BOTANICAL_AUTHORITY:
            continue
        sources = tuple(
            c for c in (exception.get("candidates") or ())
            if str(c.get("id") or "").startswith("source:")
        )
        if not sources:
            continue
        # Zero guesswork: ambiguous values and conflicting document claims
        # require individual resolution, not a checkbox shortcut.
        if len({str(c.get("value") or "").strip() for c in sources}) != 1:
            continue
        chosen = sorted(sources, key=lambda c: str(c["id"]))[0]
        claim_id = str(chosen["id"])[7:]
        try:
            source = select_reviewable_source_claim(
                case, claim_id=claim_id,
                field_name=str(exception["field_name"]),
                line_reference=str(exception["line_reference"]),
            )
        except ValueError:
            continue
        checked = validate_ppq_value(str(exception["field_name"]), source.normalized_value)
        if checked.status.value != "VALID" or not checked.normalized_value:
            continue
        candidates[field_id] = {
            "field_id": field_id,
            "claim_id": claim_id,
            "field_name": str(exception["field_name"]),
            "line_reference": str(exception["line_reference"]),
            "value": checked.normalized_value,
            "document_id": source.assurance_document_id,
            "page": source.page,
        }
    return candidates


def preview_batch_review(case) -> tuple[dict, ...]:
    """Show selected, supported evidence BEFORE requesting reviewer approval."""
    return tuple(_eligible_case_choices(case).values())


def accept_assurance_v2_batch(
    *, organization_id: int, operation_public_id: str,
    authenticated_user_id: int, selected: tuple[dict, ...],
    reason: str, explicit_confirmation: bool,
    expected_source_set_fingerprint: str, expected_case_fingerprint: str,
    session_factory=get_us_lacey_db_session,
) -> dict:
    org_id, user_id = int(organization_id), int(authenticated_user_id)
    try:
        operation_uuid = UUID(str(operation_public_id))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("Invalid shipment identifier") from exc
    explanation = str(reason or "").strip()
    if org_id <= 0 or user_id <= 0:
        raise PermissionError("Authenticated reviewer required")
    if not explicit_confirmation or not 6 <= len(explanation) <= 500:
        raise ValueError("Explicit confirmation and a reviewer explanation are required")
    if not isinstance(selected, (tuple, list)) or not 1 <= len(selected) <= _MAX_BATCH:
        raise ValueError("Select between 1 and 32 documentary fields")
    if not all(isinstance(x, dict) and set(x) == {"field_id", "claim_id"} for x in selected):
        raise ValueError("Only exact displayed field/source pairs may be approved")
    try:
        pairs = tuple((int(x["field_id"]), str(x["claim_id"])) for x in selected)
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid field or source claim") from exc
    if len(set(pairs)) != len(pairs) or len({a for a, _ in pairs}) != len(pairs):
        raise ValueError("Repeated or conflicting field selections")

    db = session_factory()
    try:
        set_tenant_db_context(db, org_id)
        _actor(db, org_id, user_id)
        operation = db.scalar(select(UsLaceyOperation).where(
            UsLaceyOperation.organization_id == org_id,
            UsLaceyOperation.public_id == operation_uuid,
        ).with_for_update())
        if operation is None:
            raise PermissionError("Shipment unavailable")
        if not review_journal_enabled(org_id, operation.client_reference):
            raise PermissionError("Batch review is restricted to the private V2 lab")
        revision = db.scalar(select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == org_id,
            UsLaceySourceSetRevision.operation_id == operation.id,
            UsLaceySourceSetRevision.is_current.is_(True),
            UsLaceySourceSetRevision.status == "FINALIZED",
        ).with_for_update())
        if revision is None or revision.source_set_fingerprint != expected_source_set_fingerprint:
            raise ValueError("STALE_SOURCE_SET")
        current_case = _case(db, org_id, operation_uuid)
        if (current_case is None or current_case.source_set_revision != revision.id
                or current_case.authority_fingerprint() != expected_case_fingerprint):
            raise ValueError("STALE_CASE_AUTHORITY")
        allowed = _eligible_case_choices(current_case)
        for field_id, claim_id in pairs:
            if allowed.get(field_id, {}).get("claim_id") != claim_id:
                raise ValueError("BATCH_EVIDENCE_CHANGED")

        # Immutable batch ID binds the EXACT set of individual reviewed source
        # claims; never reuse approval following source-set replacement.
        request_digest = sha256((
            str(org_id) + ":" + str(operation.id) + ":" + str(revision.id)
            + ":" + str(user_id) + ":"
            + "|".join(f"{field_id}:{claim}" for field_id, claim in sorted(pairs))
        ).encode("utf-8")).hexdigest()
        reviewed = datetime.now(timezone.utc)
        for field_id, claim_id in sorted(pairs):
            field = db.scalar(select(UsLaceyOperationField).where(
                UsLaceyOperationField.organization_id == org_id,
                UsLaceyOperationField.operation_id == operation.id,
                UsLaceyOperationField.id == field_id,
            ).with_for_update())
            approved = allowed[field_id]
            if (field is None or field.field_name != approved["field_name"]
                    or field.merchandise_line_reference != approved["line_reference"]
                    or field.reviewed_at is not None or field.human_value
                    or field.field_status not in {"REVIEW", "SUPPORTED", "FOUND"}):
                raise ValueError("BATCH_FIELD_CHANGED")
            source = select_reviewable_source_claim(
                current_case, claim_id=claim_id,
                field_name=field.field_name,
                line_reference=field.merchandise_line_reference,
            )
            checked = validate_ppq_value(field.field_name, source.normalized_value)
            if checked.status.value != "VALID" or checked.normalized_value != approved["value"]:
                raise ValueError("BATCH_VALUE_CHANGED")
            conflict = db.scalar(select(ReconciliationIssue.id).where(
                ReconciliationIssue.organization_id == org_id,
                ReconciliationIssue.operation_reference == f"us_lacey:{operation.public_id}",
                ReconciliationIssue.us_lacey_operation_field_id == field.id,
                ReconciliationIssue.status == "OPEN",
            ).limit(1))
            if conflict is not None:
                raise ValueError("BATCH_HAS_REAL_CONFLICT")
            field.original_value = source.raw_value
            field.normalized_value = source.normalized_value
            field.source_assurance_document_id = source.assurance_document_id
            field.source_page = source.page
            field.source_locator = source.locator
            field.human_value = checked.normalized_value
            field.field_status = "MATCHED"
            field.validation_status = "VALID"
            field.validation_error = None
            field.not_required_reason_code = None
            field.reviewed_at = reviewed
            field.reviewed_by_user_id = user_id
            # Current source and human selection are journaled in THIS same DB
            # transaction, one provenance row and audit event per field.
            db.flush()
            journal_reviewed_field(
                db, organization_id=org_id, operation=operation, field=field,
                authenticated_user_id=user_id, action="accept",
                reason=explanation,
            )

        reconcile_entered_value_invariant(
            db, organization_id=org_id, operation=operation,
        )
        refresh_regulatory_assessment_snapshot_for_review(
            db, organization_id=org_id, operation=operation,
        )
        status = refresh_us_lacey_operation_status(
            db, organization_id=org_id, operation=operation,
        )
        event = append_operation_event(
            db, organization_id=org_id, operation_id=operation.id,
            actor_type="USER", actor_identity=f"user:{user_id}",
            event_type="HUMAN_REVIEW",
            event_key=f"assurance_v2:batch_review:{request_digest}",
            details={
                "action": "BATCH_REVIEW_ACCEPT",
                "revision_id": int(revision.id),
                "source_set_fingerprint": revision.source_set_fingerprint,
                "reviewed_count": len(pairs),
                "field_ids": [field_id for field_id, _ in sorted(pairs)],
                "actor_user_id": user_id,
                "reason": explanation,
                "claim_fingerprint": request_digest,
            },
        )
        db.commit()
        return {
            "status": "RECORDED", "accepted_count": len(pairs),
            "operation_status": status, "audit_event_id": event.id,
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
