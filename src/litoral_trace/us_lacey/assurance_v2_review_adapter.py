"""Opt-in, transactional bridge from authenticated portal review to V2 authority.

The legacy workflow remains unchanged while the V2 release gate is OFF.
When explicitly enabled, a reviewer decision must have current source evidence
and is journaled in the same transaction as the existing review mutation.
"""
from __future__ import annotations

import os

from sqlalchemy import select
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    AssuranceV2Decision, UsLaceyOperation, UsLaceyOperationField,
    UsLaceySourceSetRevision,
)
from litoral_trace.us_lacey.assurance_v2_memory import (
    promote_decision_to_memory, record_human_decision,
)
from litoral_trace.us_lacey.reusable_evidence import REUSABLE_FIELD_NAMES


def review_journal_enabled(
    organization_id: int | None = None, operation_reference: str | None = None,
) -> bool:
    """Limit production journaling to explicitly allowlisted TEST operations.

    Ordinary shipments must keep their original review behavior even when a
    private owner-only V2 laboratory is enabled in the same deployment.
    """
    if os.environ.get("US_LACEY_ASSURANCE_V2_REVIEW_JOURNAL", "OFF").upper() != "ON":
        return False
    environment = os.environ.get("US_LACEY_ENVIRONMENT", "").strip().lower()
    if environment not in {"production", "pilot"}:
        return True
    allowed = {
        value.strip() for value in
        os.environ.get("US_LACEY_ASSURANCE_V2_LAB_ORG_IDS", "").split(",")
        if value.strip().isdigit()
    }
    return (
        organization_id is not None
        and str(int(organization_id)) in allowed
        and str(operation_reference or "").upper().startswith("V2LAB-")
    )


def journal_reviewed_field(
    session: Session, *, organization_id: int, operation: UsLaceyOperation,
    field: UsLaceyOperationField, authenticated_user_id: int,
    action: str, reason: str | None,
) -> AssuranceV2Decision:
    """Record one source-linked review and optionally promote verified memory.

    The caller owns the transaction; failures MUST roll back the legacy review.
    No AI/source candidate is promoted without the authenticated review event.
    """
    normalized_action = action.strip().lower()
    if normalized_action not in {"accept", "edit"}:
        raise ValueError("V2 journal does not authorize this review action")
    if normalized_action == "edit" and not (reason or "").strip():
        raise ValueError("V2 corrections require an explicit reviewer reason")
    if not field.source_assurance_document_id or not (field.source_locator or "").strip():
        raise ValueError("V2 review requires a source-linked current document")
    if str(field.source_locator).startswith("reused_evidence:"):
        raise ValueError("Historical evidence cannot masquerade as current source")
    if not (field.human_value or "").strip() or field.field_status != "MATCHED":
        raise ValueError("V2 review requires a validated human-selected value")

    org_id = int(organization_id)
    revision = session.scalar(select(UsLaceySourceSetRevision).where(
        UsLaceySourceSetRevision.organization_id == org_id,
        UsLaceySourceSetRevision.operation_id == operation.id,
        UsLaceySourceSetRevision.is_current.is_(True),
        UsLaceySourceSetRevision.status == "FINALIZED",
    ).with_for_update())
    if revision is None:
        raise ValueError("V2 review requires the finalized current source set")

    previous = session.scalar(select(AssuranceV2Decision).where(
        AssuranceV2Decision.organization_id == org_id,
        AssuranceV2Decision.operation_id == operation.id,
        AssuranceV2Decision.source_set_revision_id == revision.id,
        AssuranceV2Decision.field_name == field.field_name,
        AssuranceV2Decision.line_reference == field.merchandise_line_reference,
    ).order_by(AssuranceV2Decision.id.desc()))
    decision = record_human_decision(
        session, organization_id=org_id, operation_id=operation.id,
        source_set_revision_id=revision.id,
        authenticated_user_id=int(authenticated_user_id),
        action="ACCEPT" if normalized_action == "accept" else "CORRECT",
        field_name=field.field_name,
        line_reference=field.merchandise_line_reference,
        selected_value=field.human_value,
        reason=(reason or "Reviewer explicitly accepted source-linked evidence").strip(),
        assurance_document_ids=(int(field.source_assurance_document_id),),
        supersedes_decision_id=previous.id if previous is not None else None,
        idempotency_key=(
            f"review-v2:{operation.id}:{field.id}:{revision.id}:"
            f"{field.reviewed_at.isoformat()}"
        ),
    )
    if field.field_name in REUSABLE_FIELD_NAMES:
        promote_decision_to_memory(
            session, organization_id=org_id, decision_id=decision.id,
        )
    return decision
