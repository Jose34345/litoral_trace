"""Append-only activity timeline for U.S. Lacey operations."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import String, and_, cast, or_, select, text
from sqlalchemy.orm import Session

from litoral_trace.db.models import User, UsLaceyOperation, UsLaceyOperationEvent
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


@dataclass(frozen=True, slots=True)
class OrganizationAuditEventView:
    id: int
    timestamp: datetime
    actor_type: str
    actor_identity: str
    event_type: str
    action_label: str
    detail_text: str
    details: dict[str, Any]
    operation_public_id: UUID
    operation_reference: str


AUDIT_EVENT_FILTER_OPTIONS = tuple(
    (event.value, _EVENT_LABELS[event.value]) for event in OperationEventType
)
AUDIT_DATE_RANGE_OPTIONS = (
    ("7d", "Last 7 days"),
    ("30d", "Last 30 days"),
    ("90d", "Last 90 days"),
    ("all", "All time"),
)

_DOCUMENT_ROLE_LABELS = {
    "COMMERCIAL_INVOICE": "Commercial Invoice",
    "PACKING_LIST": "Packing List",
    "BILL_OF_LADING": "Bill of Lading",
    "SUPPLIER_DECLARATION": "Supplier Declaration",
    "ARRIVAL_NOTICE": "Arrival Notice",
    "ENTRY_WORKSHEET": "Entry Worksheet",
    "CUSTOMS_ENTRY_SUMMARY": "Customs Entry Summary",
    "CERTIFICATE": "Certificate",
}
_UNKNOWN_DOCUMENT_ROLES = {"", "UNKNOWN", "OTHER", "UNCLASSIFIED"}


def _display_value(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return "—"
    return normalized[:180]


def _document_role_label(value: Any) -> str | None:
    normalized = str(value or "").strip().upper()
    if normalized in _UNKNOWN_DOCUMENT_ROLES:
        return None
    return _DOCUMENT_ROLE_LABELS.get(
        normalized,
        normalized.replace("_", " ").title() if normalized else None,
    )


def _actor_user_id(actor_identity: str) -> int | None:
    normalized = str(actor_identity or "").strip()
    if normalized.lower().startswith("user:"):
        normalized = normalized.split(":", 1)[1].strip()
    if normalized.isdigit():
        value = int(normalized)
        return value if value > 0 else None
    return None


def _actor_identity_map(
    session: Session,
    *,
    organization_id: int,
    actor_identities: tuple[str, ...],
) -> dict[int, str]:
    user_ids = {
        user_id
        for identity in actor_identities
        if (user_id := _actor_user_id(identity)) is not None
    }
    if not user_ids:
        return {}

    dialect = getattr(getattr(session, "bind", None), "dialect", None)
    dialect_name = str(getattr(dialect, "name", ""))
    if dialect_name != "postgresql":
        rows = session.execute(
            select(User.id, User.full_name, User.email).where(
                User.organization_id == int(organization_id),
                User.id.in_(tuple(sorted(user_ids))),
            )
        ).all()
        return {
            int(user_id): (
                str(full_name or "").strip()
                or str(email or "").strip()
                or "Workspace user"
            )
            for user_id, full_name, email in rows
        }

    resolved: dict[int, str] = {}
    for user_id in sorted(user_ids):
        row = session.execute(
            text(
                "SELECT user_id, display_identity "
                "FROM public.us_lacey_audit_user_identity(:user_id)"
            ),
            {"user_id": int(user_id)},
        ).mappings().one_or_none()
        if row is not None:
            resolved[int(row["user_id"])] = (
                str(row["display_identity"] or "").strip() or "Workspace user"
            )
    return resolved


def _actor_display(
    *,
    actor_type: str,
    actor_identity: str,
    user_identity_map: dict[int, str],
) -> str:
    raw = str(actor_identity or "").strip()
    if str(actor_type or "").upper() != OperationActorType.USER.value:
        return raw or "Litoral Trace"
    user_id = _actor_user_id(raw)
    if user_id is not None:
        return user_identity_map.get(user_id, "Workspace user")
    return raw or "Workspace user"


def _utc_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _detail_text(event_type: str, details: dict[str, Any]) -> str:
    if event_type == OperationEventType.CREATED.value:
        reference = _display_value(details.get("client_reference"))
        return f"Shipment operation {reference} entered the workspace."
    if event_type == OperationEventType.DOCUMENT_UPLOADED.value:
        filename = _display_value(details.get("filename"))
        role = _document_role_label(details.get("document_role"))
        version = details.get("version_number")
        descriptor = role or filename
        suffix = f" · version {version}" if version else ""
        if role and filename != "—":
            return f"{descriptor} · {filename} added to the evidence set{suffix}."
        return f"{descriptor} added to the evidence set{suffix}."
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
        identity_map = _actor_identity_map(
            session,
            organization_id=org_id,
            actor_identities=tuple(str(row.actor_identity or "") for row in rows),
        )
        return tuple(
            OperationEventView(
                id=int(row.id),
                timestamp=_utc_timestamp(row.timestamp),
                actor_type=str(row.actor_type),
                actor_identity=_actor_display(
                    actor_type=str(row.actor_type),
                    actor_identity=str(row.actor_identity),
                    user_identity_map=identity_map,
                ),
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


def list_organization_audit_events(
    *,
    organization_id: int,
    operation_query: str | None = None,
    actor_query: str | None = None,
    event_type: str | None = None,
    date_range: str | None = "30d",
    limit: int = 250,
    session_factory=None,
) -> tuple[OrganizationAuditEventView, ...]:
    """Return newest-first organization audit events with simple filters."""
    org_id = int(organization_id)
    normalized_operation = str(operation_query or "").strip()
    normalized_actor = str(actor_query or "").strip()
    normalized_event = str(event_type or "").strip().upper()
    normalized_range = str(date_range or "30d").strip().lower()

    factory = session_factory or get_us_lacey_db_session
    session = factory()
    try:
        set_tenant_db_context(session, org_id)
        statement = (
            select(
                UsLaceyOperationEvent,
                UsLaceyOperation.public_id,
                UsLaceyOperation.client_reference,
            )
            .join(
                UsLaceyOperation,
                and_(
                    UsLaceyOperation.id == UsLaceyOperationEvent.operation_id,
                    UsLaceyOperation.organization_id
                    == UsLaceyOperationEvent.organization_id,
                ),
            )
            .where(UsLaceyOperationEvent.organization_id == org_id)
        )

        if normalized_operation:
            pattern = f"%{normalized_operation}%"
            statement = statement.where(
                or_(
                    UsLaceyOperation.client_reference.ilike(pattern),
                    cast(UsLaceyOperation.public_id, String).ilike(pattern),
                )
            )

        if normalized_event:
            valid_events = {item.value for item in OperationEventType}
            if normalized_event not in valid_events:
                return ()
            statement = statement.where(
                UsLaceyOperationEvent.event_type == normalized_event
            )

        range_days = {"7d": 7, "30d": 30, "90d": 90}
        if normalized_range in range_days:
            cutoff = datetime.now(timezone.utc) - timedelta(
                days=range_days[normalized_range]
            )
            statement = statement.where(
                UsLaceyOperationEvent.timestamp >= cutoff
            )
        elif normalized_range != "all":
            normalized_range = "30d"
            cutoff = datetime.now(timezone.utc) - timedelta(days=30)
            statement = statement.where(
                UsLaceyOperationEvent.timestamp >= cutoff
            )

        result_limit = max(1, min(int(limit), 500))
        fetch_limit = 500 if normalized_actor else result_limit
        rows = session.execute(
            statement.order_by(
                UsLaceyOperationEvent.timestamp.desc(),
                UsLaceyOperationEvent.id.desc(),
            ).limit(fetch_limit)
        ).all()

        event_rows = tuple(row[0] for row in rows)
        identity_map = _actor_identity_map(
            session,
            organization_id=org_id,
            actor_identities=tuple(
                str(row.actor_identity or "") for row in event_rows
            ),
        )
        views = tuple(
            OrganizationAuditEventView(
                id=int(event.id),
                timestamp=_utc_timestamp(event.timestamp),
                actor_type=str(event.actor_type),
                actor_identity=_actor_display(
                    actor_type=str(event.actor_type),
                    actor_identity=str(event.actor_identity),
                    user_identity_map=identity_map,
                ),
                event_type=str(event.event_type),
                action_label=_EVENT_LABELS.get(
                    str(event.event_type),
                    str(event.event_type).replace("_", " ").title(),
                ),
                detail_text=_detail_text(
                    str(event.event_type),
                    dict(event.details or {}),
                ),
                details=dict(event.details or {}),
                operation_public_id=operation_public_id,
                operation_reference=str(operation_reference or "").strip()
                or str(operation_public_id),
            )
            for event, operation_public_id, operation_reference in rows
        )
        if normalized_actor:
            actor_needle = normalized_actor.casefold()
            views = tuple(
                event
                for event in views
                if actor_needle in event.actor_identity.casefold()
            )
        return views[:result_limit]
    finally:
        session.close()
