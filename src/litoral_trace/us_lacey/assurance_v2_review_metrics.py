"""Read-only effort and replay-delta metrics for the private V2 laboratory."""
from __future__ import annotations

from uuid import UUID
from sqlalchemy import select

from litoral_trace.db.models import (
    UsLaceyOperation, UsLaceyOperationEvent, UsLaceySourceSetRevision,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session


def review_effort_snapshot(*, organization_id: int, operation_public_id: str,
                           session_factory=get_us_lacey_db_session) -> dict:
    """Never infer human confirmations from worker events or parser counts."""
    blank = {
        "human_batch_actions": 0,
        "fields_confirmed_by_batch": 0,
        "historical_fields_authorized_by_group": 0,
        "previous_source_bytes_unchanged": None,
        "prior_revision": None,
        "approval_delta": "NO_PREVIOUS_SOURCE_SET",
    }
    try:
        public_id = UUID(str(operation_public_id))
    except (TypeError, ValueError):
        return blank
    db = session_factory()
    try:
        org_id = int(organization_id)
        set_tenant_db_context(db, org_id)
        op = db.scalar(select(UsLaceyOperation).where(
            UsLaceyOperation.organization_id == org_id,
            UsLaceyOperation.public_id == public_id,
        ))
        if op is None or not str(op.client_reference or "").upper().startswith("V2LAB-"):
            return blank
        revisions = db.scalars(select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == org_id,
            UsLaceySourceSetRevision.operation_id == op.id,
        ).order_by(UsLaceySourceSetRevision.generation.desc()).limit(2)).all()
        if not revisions:
            return blank
        current = revisions[0]
        result = dict(blank)
        if len(revisions) > 1:
            prior = revisions[1]
            unchanged = (
                current.source_set_fingerprint == prior.source_set_fingerprint
                and current.document_count == prior.document_count
            )
            result.update(
                previous_source_bytes_unchanged=unchanged,
                prior_revision=int(prior.generation),
                approval_delta=(
                    "UNCHANGED_SOURCE_BYTES_REVIEW_REQUIRED"
                    if unchanged else "DOCUMENT_SET_CHANGED_REVIEW_REQUIRED"
                ),
            )
        events = db.scalars(select(UsLaceyOperationEvent).where(
            UsLaceyOperationEvent.organization_id == org_id,
            UsLaceyOperationEvent.operation_id == op.id,
            UsLaceyOperationEvent.event_type == "human_review",
        )).all()
        for event in events:
            data = dict(event.details or {})
            if data.get("source_set_revision_id") != current.id:
                continue
            action = data.get("action")
            if action == "BATCH_REVIEW_CONFIRMED":
                result["human_batch_actions"] += 1
                result["fields_confirmed_by_batch"] += int(data.get("field_count") or 0)
            elif action == "MEMORY_GROUP_REVIEW_CONFIRMED":
                result["human_batch_actions"] += 1
                result["historical_fields_authorized_by_group"] += int(data.get("field_count") or 0)
        return result
    finally:
        db.close()
