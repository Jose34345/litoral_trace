"""Append-only activity timeline for U.S. Lacey operations."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from litoral_trace.db.models import UsLaceyOperation, UsLaceyOperationEvent
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import sanitize_audit_metadata
from litoral_trace.us_lacey.db import get_us_lacey_db_session


class OperationActorType(StrEnum):
    SYSTEM = "SYSTEM"
    USER = "USER"


class OperationEventType(StrEnum):
    CREATED = "CREATED"
    DOCUMENT_UPLOADED = "DOCUMENT_UPLOADED"
    EXTRACTED = "EXTRACTED"
    HUMAN_REVIEW = "HUMAN_REVIEW"
    EVIDENCE_REUSED = "EVIDENCE_REUSED"
    PACKAGE_GENERATED = "PACKAGE_GENERATED"


_EVENT_LABELS = {
    OperationEventType.CREATED.value: "Operation created",
    OperationEventType.DOCUMENT_UPLOADED.value: "Document uploaded",
    OperationEventType.EXTRACTED.value: "Automated extraction completed",
    OperationEventType.HUMAN_REVIEW.value: "Human review",
    OperationEventType.EVIDENCE_REUSED.value: "Verified supplier evidence reused",
    OperationEventType.PACKAGE_GENERATED.value: "Preparation package generated",
}


@dataclass(frozen=True, slots=True)
class OperationEventView:
    id: int
    timestamp: datetime
    actor_type: str
    actor_identity: str
    event_type: str
    action_label: str
    detail_text: str
    details: dict[str, Any]


def _display_value(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return "—"
    return normalized[:180]


def _detail_text(event_type: str, details: dict[str, Any]) -> str:
    if event_type == OperationEventType.CREATED.value:
        reference = _display_value(details.get("client_reference"))
        return f"Shipment operation {reference} entered the workspace."
    if event_type == OperationEventType.DOCUMENT_UPLOADED.value:
        filename = _display_value(details.get("filename"))
        role = _display_value(details.get("document_role")).replace("_", " ").title()
        version = details.get("version_number")
        suffix = f" · {role}" if role != "—" else ""
        if version:
            suffix += f" · version {version}"
        return f"{filename} added to the evidence set{suffix}."
    if event_type == OperationEventType.EXTRACTED.value:
        projected = int(details.get("projected_field_count") or 0)
        conflicts = int(details.get("conflict_count") or 0)
        return (
            f"Automated extraction and reconciliation finished · "
            f"{projected} field{'s' if projected != 1 else ''} projected · "
            f"{conflicts} conflict{'s' if conflicts != 1 else ''}."
        )
    if event_type == OperationEventType.EVIDENCE_REUSED.value:
        count = int(details.get("claim_count") or 0)
        return (
            f"{count} verified supplier claim{'s' if count != 1 else ''} "
            "reused automatically for this shipment."
        )
    if event_type == OperationEventType.PACKAGE_GENERATED.value:
        package = _display_value(details.get("package_type"))
        return f"{package} preparation package generated from the reviewed record."
    if event_type == OperationEventType.HUMAN_REVIEW.value:
        action = str(details.get("action") or "").strip().lower()
        if action == "accept_supported":
            count = int(details.get("accepted_field_count") or 0)
            return f"{count} supported field{'s' if count != 1 else ''} confirmed in bulk."
        if action == "review_completed":
            return "Human review completed and the preparation record was locked for export."
        field_name = _display_value(details.get("field_name")).replace("_", " ").title()
        before = _display_value(details.get("before_value"))
        after = _display_value(details.get("after_value"))
        if action == "not_required":
            message = f"{field_name} marked not required."
        elif before != after:
            message = f"{field_name}: {before} → {after}."
        else:
            message = f"{field_name} confirmed as {after}."
        source_label = str(details.get("source_label") or "").strip()
        source_page = details.get("source_page")
        if source_label:
            message += f" Source: {source_label}"
            if source_page:
                message += f" p.{int(source_page)}"
            message += "."
        return message
    return "Operation activity recorded."


def append_operation_event(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
    actor_type: OperationActorType | str,
    actor_identity: str,
    event_type: OperationEventType | str,
    details: dict[str, Any] | None = None,
    event_key: str | None = None,
) -> UsLaceyOperationEvent:
    """Append one immutable event in the caller transaction."""
    org_id = int(organization_id)
    op_id = int(operation_id)
    actor = str(
        actor_type.value if isinstance(actor_type, OperationActorType) else actor_type
    ).strip().upper()
    event = str(
        event_type.value if isinstance(event_type, OperationEventType) else event_type
    ).strip().upper()
    identity = str(actor_identity or "").strip()[:255]
    key = str(event_key or "").strip()[:255] or None

    if actor not in {item.value for item in OperationActorType}:
        raise ValueError("Unsupported audit actor type.")
    if event not in {item.value for item in OperationEventType}:
        raise ValueError("Unsupported audit event type.")
    if not identity:
        raise ValueError("actor_identity is required.")

    if key is not None:
        existing = session.scalar(
            select(UsLaceyOperationEvent).where(
                UsLaceyOperationEvent.organization_id == org_id,
                UsLaceyOperationEvent.operation_id == op_id,
                UsLaceyOperationEvent.event_key == key,
            )
        )
        if existing is not None:
            return existing

    row = UsLaceyOperationEvent(
        public_id=uuid4(),
        organization_id=org_id,
        operation_id=op_id,
        actor_type=actor,
        actor_identity=identity,
        event_type=event,
        event_key=key,
        details=sanitize_audit_metadata(details) or {},
    )
    session.add(row)
    session.flush()
    return row


def record_operation_event(
    *,
    organization_id: int,
    operation_id: int,
    actor_type: OperationActorType | str,
    actor_identity: str,
    event_type: OperationEventType | str,
    details: dict[str, Any] | None = None,
    event_key: str | None = None,
    session_factory=None,
) -> int:
    """Persist one event atomically and return its id."""
    factory = session_factory or get_us_lacey_db_session
    session = factory()
    try:
        set_tenant_db_context(session, int(organization_id))
        row = append_operation_event(
            session,
            organization_id=organization_id,
            operation_id=operation_id,
            actor_type=actor_type,
            actor_identity=actor_identity,
            event_type=event_type,
            details=details,
            event_key=event_key,
        )
        session.commit()
        return int(row.id)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def list_operation_events(
    *,
    organization_id: int,
    operation_public_id: UUID | str,
    limit: int = 200,
    session_factory=None,
) -> tuple[OperationEventView, ...]:
    """Return newest-first tenant-scoped events for one public operation."""
    org_id = int(organization_id)
    try:
        public_id = (
            operation_public_id
            if isinstance(operation_public_id, UUID)
            else UUID(str(operation_public_id))
        )
    except (TypeError, ValueError, AttributeError):
        return ()

    factory = session_factory or get_us_lacey_db_session
    session = factory()
    try:
        set_tenant_db_context(session, org_id)
        operation_id = session.scalar(
            select(UsLaceyOperation.id).where(
                UsLaceyOperation.organization_id == org_id,
                UsLaceyOperation.public_id == public_id,
            )
        )
        if operation_id is None:
            return ()
        rows = session.scalars(
            select(UsLaceyOperationEvent)
            .where(
                UsLaceyOperationEvent.organization_id == org_id,
                UsLaceyOperationEvent.operation_id == int(operation_id),
            )
            .order_by(
                UsLaceyOperationEvent.timestamp.desc(),
                UsLaceyOperationEvent.id.desc(),
            )
            .limit(max(1, min(int(limit), 500)))
        ).all()
        return tuple(
            OperationEventView(
                id=int(row.id),
                timestamp=(
                    row.timestamp
                    if row.timestamp.tzinfo is None
                    else row.timestamp.astimezone(timezone.utc)
                ),
                actor_type=str(row.actor_type),
                actor_identity=str(row.actor_identity),
                event_type=str(row.event_type),
                action_label=_EVENT_LABELS.get(
                    str(row.event_type),
                    str(row.event_type).replace("_", " ").title(),
                ),
                detail_text=_detail_text(str(row.event_type), dict(row.details or {})),
                details=dict(row.details or {}),
            )
            for row in rows
        )
    finally:
        session.close()
