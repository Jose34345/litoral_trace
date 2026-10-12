"""P1 non-authoritative differential review and audit-derived human-effort metrics.

No historical decision is copied into current regulatory truth. A reviewer may
reconfirm only when the current source-backed claim passes the existing strict
batch-review writer; these hints can never bypass its checks.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import and_, select

from litoral_trace.db.models import (
    AssuranceDocument, AssuranceV2Decision, AssuranceV2DecisionSource,
    UsLaceyOperation, UsLaceyOperationEvent, UsLaceySourceSetRevision,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.assurance_v2_batch_review import preview_batch_review
from litoral_trace.us_lacey.assurance_v2_review_adapter import review_journal_enabled
from litoral_trace.us_lacey.db import get_us_lacey_db_session


def _safe_int(value, minimum=0, maximum=100000) -> int:
    if isinstance(value, bool):
        return minimum
    try:
        n = int(value)
    except (TypeError, ValueError, OverflowError):
        return minimum
    return n if minimum <= n <= maximum else minimum


def aggregate_human_effort(events) -> dict:
    """Count documented USER actions, never guess minutes from timestamps.

    Legacy field-review events are excluded rather than miscounted as batches.
    Client-side durations are *observations* and cannot prove net time saved.
    """
    actions = 0
    reviewed = 0
    client_seconds = 0
    timed_actions = 0
    for event in events:
        if getattr(event, "actor_type", None) != "USER" or getattr(event, "event_type", None) != "HUMAN_REVIEW":
            continue
        details = getattr(event, "details", None) or {}
        kind = details.get("action")
        if kind not in {"BATCH_REVIEW_ACCEPT", "BOTANICAL_GROUP_REVIEW"}:
            continue
        n = _safe_int(details.get("reviewed_count" if kind == "BATCH_REVIEW_ACCEPT" else "field_count"), 1, 32)
        if not n:
            continue
        actions += 1
        reviewed += n
        duration = _safe_int(details.get("client_review_seconds"), 0, 3600)
        if duration:
            client_seconds += duration
            timed_actions += 1
    return {
        "grouped_review_actions": actions,
        "fields_authorized_by_group": reviewed,
        "estimated_clicks_avoided": max(0, reviewed - actions),
        "observed_review_seconds": client_seconds,
        "timed_actions": timed_actions,
        "time_basis": "CLIENT_REPORTED_ACTIVE_INTERVAL" if timed_actions else "NOT_MEASURED",
    }


def classify_prior_review(
    *, prior_value: str,
    prior_source_fingerprint: str,
    current_source_fingerprint: str,
    prior_sources: tuple[dict, ...],
    current_documents: tuple[dict, ...],
    current_choice: dict | None,
    current_authority: str | None,
) -> str:
    """Fail closed on absent/changed source, claim, identity or selected value."""
    if not prior_sources:
        return "SOURCE_UNVERIFIED"
    current = {
        (str(doc.get("id")), _safe_int(doc.get("operation_document_id")),
         _safe_int(doc.get("version")), str(doc.get("sha256") or "")):
        doc for doc in current_documents if doc.get("raw_available") is True
    }
    for ref in prior_sources:
        key = (
            str(ref.get("document_id")), _safe_int(ref.get("operation_document_id")),
            _safe_int(ref.get("version")), str(ref.get("sha256") or ""),
        )
        if not key[3] or key not in current:
            return "SOURCE_CHANGED"
    if prior_source_fingerprint != current_source_fingerprint:
        return "SOURCE_SET_CHANGED"
    if current_authority == "HUMAN_CONFIRMED":
        return "CURRENTLY_AUTHORIZED"
    if (current_choice is None or
            str(current_choice.get("value") or "").strip() != str(prior_value or "").strip()):
        return "REVIEW_NEEDED"
    if str(current_choice.get("document_id")) not in {
        str(ref["document_id"]) for ref in prior_sources
    }:
        return "SOURCE_CHANGED"
    return "UNCHANGED_RECONFIRM_REQUIRED"


def read_p1_review_insights(*, organization_id: int, operation_public_id: str,
                            current_case, session_factory=get_us_lacey_db_session) -> dict:
    """Read-only tenant-scoped comparison of latest prior reviewed revision."""
    empty = {
        "prior_review_count": 0, "unchanged_reconfirm": 0,
        "needs_review": 0, "previously_reviewed_fields": (),
        "effort": aggregate_human_effort(()),
    }
    try:
        op_uuid = UUID(str(operation_public_id))
    except (ValueError, TypeError, AttributeError):
        return empty
    session = session_factory()
    try:
        set_tenant_db_context(session, int(organization_id))
        op = session.scalar(select(UsLaceyOperation).where(
            UsLaceyOperation.organization_id == int(organization_id),
            UsLaceyOperation.public_id == op_uuid,
        ))
        if op is None or not review_journal_enabled(
            int(organization_id), op.client_reference
        ):
            return empty
        events = session.scalars(select(UsLaceyOperationEvent).where(
            UsLaceyOperationEvent.organization_id == int(organization_id),
            UsLaceyOperationEvent.operation_id == op.id,
            UsLaceyOperationEvent.event_type == "HUMAN_REVIEW",
            UsLaceyOperationEvent.actor_type == "USER",
        ).order_by(UsLaceyOperationEvent.id)).all()
        effort = aggregate_human_effort(events)
        past = session.scalars(select(AssuranceV2Decision).join(
            UsLaceySourceSetRevision,
            and_(
                UsLaceySourceSetRevision.id == AssuranceV2Decision.source_set_revision_id,
                UsLaceySourceSetRevision.organization_id == AssuranceV2Decision.organization_id,
            ),
        ).where(
            AssuranceV2Decision.organization_id == int(organization_id),
            AssuranceV2Decision.operation_id == op.id,
            AssuranceV2Decision.action.in_(("ACCEPT", "CORRECT")),
            UsLaceySourceSetRevision.generation < current_case.source_set_generation,
        ).order_by(AssuranceV2Decision.id.desc())).all()
        if not past:
            return {**empty, "effort": effort}
        # One latest decision per field; never resurrect superseded corrections.
        prior = {}
        for d in past:
            key = (d.line_reference or "__shipment__", d.field_name)
            prior.setdefault(key, d)
        rows = session.execute(select(
            AssuranceV2DecisionSource, AssuranceDocument, VaultDocument,
        ).join(
            AssuranceDocument,
            and_(AssuranceDocument.id == AssuranceV2DecisionSource.assurance_document_id,
                 AssuranceDocument.organization_id == AssuranceV2DecisionSource.organization_id),
        ).join(
            VaultDocument,
            and_(VaultDocument.id == AssuranceDocument.vault_document_id,
                 VaultDocument.organization_id == AssuranceDocument.organization_id),
        ).where(
            AssuranceV2DecisionSource.organization_id == int(organization_id),
            AssuranceV2DecisionSource.decision_id.in_([d.id for d in prior.values()]),
        )).all()
        by_decision = defaultdict(list)
        for source, _, vault in rows:
            by_decision[source.decision_id].append({
                "document_id": str(source.assurance_document_id),
                "operation_document_id": source.operation_document_id,
                "version": source.document_version_number,
                "sha256": str(vault.sha256),
            })
        choices = {
            (str(c["line_reference"]), str(c["field_name"])): c
            for c in preview_batch_review(current_case)
        }
        fields = {
            (str(f.get("line_reference")), str(f.get("name"))): f
            for f in current_case.fields
        }
        notes = []
        for key, decision in prior.items():
            current = fields.get(key) or {}
            status = classify_prior_review(
                prior_value=decision.selected_value,
                prior_source_fingerprint=decision.source_set_fingerprint,
                current_source_fingerprint=current_case.source_set_fingerprint,
                prior_sources=tuple(by_decision.get(decision.id, ())),
                current_documents=tuple(current_case.documents),
                current_choice=choices.get(key),
                current_authority=current.get("authority"),
            )
            notes.append({
                "line_reference": key[0], "field_name": key[1],
                "status": status,
                # Do not expose historical raw document or customer values
                # in telemetry; only field names and decision statuses.
            })
        return {
            "prior_review_count": len(notes),
            "unchanged_reconfirm": sum(n["status"] == "UNCHANGED_RECONFIRM_REQUIRED" for n in notes),
            "needs_review": sum(n["status"] not in ("UNCHANGED_RECONFIRM_REQUIRED", "CURRENTLY_AUTHORIZED") for n in notes),
            "previously_reviewed_fields": tuple(notes),
            "effort": effort,
        }
    finally:
        session.close()
