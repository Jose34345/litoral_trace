"""Persistent arithmetic invariants for U.S. Lacey preparation workspaces.

The PPQ 505 contract stores Entered Value at plant-line scope. Commercial source
packages can also contain a shipment/invoice total that is useful for arithmetic
reconciliation but must never compete as a plant-line candidate. This module keeps
that total as internal auditable evidence and maintains one stable blocking issue
whenever the explicit plant-line allocations do not reconcile to it.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
from uuid import UUID

from sqlalchemy import select

from litoral_trace.db.models import (
    ReconciliationIssue,
    UsLaceyFieldCandidate,
    UsLaceyOperation,
    UsLaceyOperationField,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.ppq505 import PPQ505_SHIPMENT_REFERENCE, validate_ppq_value


SHIPMENT_TOTAL_ENTERED_VALUE = "shipment_total_entered_value"
ENTERED_VALUE_RECONCILIATION_FIELD = "entered_value_reconciliation"
ENTERED_VALUE_RECONCILIATION_RULE = "US_LACEY_ENTERED_VALUE_RECONCILIATION"


@dataclass(frozen=True, slots=True)
class EnteredValueReconciliation:
    evaluated: bool
    reconciled: bool
    line_total: Decimal | None
    shipment_total: Decimal | None
    line_field_ids: tuple[int, ...]
    line_values: tuple[str, ...]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _fingerprint(*parts: object) -> str:
    payload = "\x1f".join(str(part if part is not None else "") for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normalized_entered_value(value: object) -> Decimal | None:
    validation = validate_ppq_value("entered_value", value)
    if validation.status.value != "VALID" or validation.normalized_value is None:
        return None
    return Decimal(validation.normalized_value)


def evaluate_entered_value_reconciliation(
    line_values: tuple[object, ...] | list[object],
    shipment_total: object | None,
    *,
    line_field_ids: tuple[int, ...] | list[int] = (),
) -> EnteredValueReconciliation:
    """Evaluate the invariant without guessing through missing or invalid values."""
    total = _normalized_entered_value(shipment_total) if shipment_total is not None else None
    ids = tuple(int(value) for value in line_field_ids)
    if total is None or not line_values:
        return EnteredValueReconciliation(False, False, None, total, ids, ())

    normalized: list[Decimal] = []
    normalized_text: list[str] = []
    for raw in line_values:
        value = _normalized_entered_value(raw)
        if value is None:
            return EnteredValueReconciliation(False, False, None, total, ids, tuple(normalized_text))
        normalized.append(value)
        normalized_text.append(format(value, "f"))

    line_total = sum(normalized, Decimal("0"))
    return EnteredValueReconciliation(
        True,
        line_total == total,
        line_total,
        total,
        ids,
        tuple(normalized_text),
    )


def upsert_shipment_total_entered_value(
    session,
    *,
    organization_id: int,
    operation: UsLaceyOperation,
    raw_value: object,
    source_assurance_document_id: int,
    source_page: int | None,
    source_locator: str | None,
    extractor: str | None,
    extractor_version: str | None,
    confidence: float,
) -> UsLaceyOperationField | None:
    """Persist shipment total as internal evidence, never as PPQ line field 12."""
    validation = validate_ppq_value("entered_value", raw_value)
    if validation.status.value != "VALID" or validation.normalized_value is None:
        return None

    field = session.scalar(
        select(UsLaceyOperationField).where(
            UsLaceyOperationField.organization_id == int(organization_id),
            UsLaceyOperationField.operation_id == operation.id,
            UsLaceyOperationField.merchandise_line_reference == PPQ505_SHIPMENT_REFERENCE,
            UsLaceyOperationField.field_name == SHIPMENT_TOTAL_ENTERED_VALUE,
        )
    )
    if field is None:
        field = UsLaceyOperationField(
            organization_id=int(organization_id),
            operation_id=operation.id,
            merchandise_line_reference=PPQ505_SHIPMENT_REFERENCE,
            field_name=SHIPMENT_TOTAL_ENTERED_VALUE,
            field_scope="SHIPMENT",
            plant_line_id=None,
        )
        session.add(field)

    field.original_value = str(raw_value).strip()
    field.normalized_value = validation.normalized_value
    field.human_value = None
    field.field_status = "MATCHED"
    field.validation_status = "VALID"
    field.validation_error = None
    field.not_required_reason_code = None
    field.confidence = max(0.0, min(float(confidence), 1.0))
    field.source_assurance_document_id = int(source_assurance_document_id)
    field.source_page = source_page
    field.source_locator = source_locator
    field.extractor = extractor or "us-lacey-deterministic-projector"
    field.extractor_version = extractor_version or "1"
    return field


def _current_issue(session, *, organization_id: int, operation: UsLaceyOperation):
    fingerprint = _fingerprint(ENTERED_VALUE_RECONCILIATION_RULE, operation.public_id)
    issue = session.scalar(
        select(ReconciliationIssue).where(
            ReconciliationIssue.organization_id == int(organization_id),
            ReconciliationIssue.fingerprint == fingerprint,
        )
    )
    return fingerprint, issue


def _resolve_issue(issue: ReconciliationIssue | None, reason: str) -> None:
    if issue is None or issue.status != "OPEN":
        return
    issue.status = "RESOLVED"
    issue.resolution_justification = reason
    issue.resolved_at = _utc_now()


def _expose_single_line_shipment_total_candidate(
    session,
    *,
    organization_id: int,
    operation: UsLaceyOperation,
    line_fields: list[UsLaceyOperationField],
    shipment_total_field: UsLaceyOperationField | None,
) -> None:
    """Expose invoice total as a competing Entered Value only for one-line shipments.

    With exactly one plant line, shipment total and line allocation are the same
    economic quantity and can be presented as two evidence-backed alternatives.
    Multi-line shipments remain arithmetic-only because a total cannot be assigned
    to any individual line without allocation evidence.
    """
    if len(line_fields) != 1 or shipment_total_field is None:
        return
    source_document_id = shipment_total_field.source_assurance_document_id
    raw_value = (
        shipment_total_field.human_value
        or shipment_total_field.normalized_value
        or shipment_total_field.original_value
    )
    normalized = _normalized_entered_value(raw_value)
    if source_document_id is None or normalized is None:
        return

    field = line_fields[0]
    normalized_text = format(normalized, "f")
    fingerprint = _fingerprint(
        "US_LACEY_SHIPMENT_TOTAL_AS_SINGLE_LINE_CANDIDATE",
        operation.public_id,
        field.id,
        source_document_id,
        normalized_text,
        shipment_total_field.source_locator,
    )
    existing = session.scalar(
        select(UsLaceyFieldCandidate).where(
            UsLaceyFieldCandidate.organization_id == int(organization_id),
            UsLaceyFieldCandidate.fingerprint == fingerprint,
        )
    )
    if existing is not None:
        return

    session.add(
        UsLaceyFieldCandidate(
            organization_id=int(organization_id),
            operation_id=operation.id,
            operation_field_id=field.id,
            source_assurance_document_id=int(source_document_id),
            original_value=str(raw_value).strip(),
            normalized_value=normalized_text,
            validation_status="VALID",
            validation_error=None,
            confidence=float(shipment_total_field.confidence or 0.0),
            source_page=shipment_total_field.source_page,
            source_locator=shipment_total_field.source_locator,
            extractor=shipment_total_field.extractor,
            extractor_version=shipment_total_field.extractor_version,
            fingerprint=fingerprint,
            decision="PENDING",
        )
    )


def _mark_line_fields_reconciliation_state(
    line_fields: list[UsLaceyOperationField],
    *,
    reconciled: bool,
) -> None:
    """Represent arithmetic inconsistency as an explicit customer-facing conflict.

    Human values remain preserved as evidence. While allocations do not reconcile,
    the affected values are CONFLICT rather than a generic REVIEW state. Once the
    invariant reconciles, human-reviewed fields return to MATCHED and unreviewed
    evidence-backed values return to SUPPORTED.
    """
    for field in line_fields:
        effective = field.human_value or field.normalized_value or field.original_value
        if _normalized_entered_value(effective) is None:
            continue
        if reconciled:
            if field.human_value is not None and field.reviewed_at is not None:
                field.field_status = "MATCHED"
            elif field.field_status == "CONFLICT":
                field.field_status = "SUPPORTED"
        else:
            if field.human_value is not None or field.field_status in {
                "MATCHED",
                "FOUND",
                "SUPPORTED",
            }:
                field.field_status = "CONFLICT"


def reconcile_entered_value_invariant(
    session,
    *,
    organization_id: int,
    operation: UsLaceyOperation,
) -> EnteredValueReconciliation:
    """Persist/reopen/resolve the arithmetic invariant from current effective values.

    The issue fingerprint is intentionally stable across edits. A customer edit that
    breaks a previously reconciled operation therefore reopens the same auditable
    invariant instead of creating disposable one-off errors.
    """
    org_id = int(organization_id)
    session.flush()
    shipment_total_field = session.scalar(
        select(UsLaceyOperationField).where(
            UsLaceyOperationField.organization_id == org_id,
            UsLaceyOperationField.operation_id == operation.id,
            UsLaceyOperationField.merchandise_line_reference == PPQ505_SHIPMENT_REFERENCE,
            UsLaceyOperationField.field_name == SHIPMENT_TOTAL_ENTERED_VALUE,
        )
    )
    line_fields = session.scalars(
        select(UsLaceyOperationField)
        .where(
            UsLaceyOperationField.organization_id == org_id,
            UsLaceyOperationField.operation_id == operation.id,
            UsLaceyOperationField.field_scope == "PLANT_LINE",
            UsLaceyOperationField.field_name == "entered_value",
        )
        .order_by(UsLaceyOperationField.id.asc())
    ).all()

    shipment_total_raw = None
    if shipment_total_field is not None:
        shipment_total_raw = (
            shipment_total_field.human_value
            or shipment_total_field.normalized_value
            or shipment_total_field.original_value
        )
    line_raw_values = [
        field.human_value or field.normalized_value or field.original_value
        for field in line_fields
    ]
    result = evaluate_entered_value_reconciliation(
        line_raw_values,
        shipment_total_raw,
        line_field_ids=[field.id for field in line_fields],
    )
    fingerprint, issue = _current_issue(
        session,
        organization_id=org_id,
        operation=operation,
    )

    if not result.evaluated:
        _resolve_issue(
            issue,
            "Arithmetic reconciliation is not currently evaluable because one or more required values are missing or invalid.",
        )
        return result

    if result.evaluated:
        _expose_single_line_shipment_total_candidate(
            session,
            organization_id=org_id,
            operation=operation,
            line_fields=list(line_fields),
            shipment_total_field=shipment_total_field,
        )

    if result.reconciled:
        _mark_line_fields_reconciliation_state(line_fields, reconciled=True)
        _resolve_issue(
            issue,
            "Line Entered Value allocations reconcile exactly to the declared shipment total.",
        )
        if issue is not None:
            issue.left_value = format(result.line_total or Decimal("0"), "f")
            issue.right_value = format(result.shipment_total or Decimal("0"), "f")
            issue.delta_numeric = Decimal("0")
            issue.evidence_json = {
                "source": "us_lacey_entered_value_invariant",
                "line_field_ids": list(result.line_field_ids),
                "line_values": list(result.line_values),
                "shipment_total_field_id": getattr(shipment_total_field, "id", None),
            }
        return result

    _mark_line_fields_reconciliation_state(line_fields, reconciled=False)
    line_total = result.line_total or Decimal("0")
    shipment_total = result.shipment_total or Decimal("0")
    evidence = {
        "source": "us_lacey_entered_value_invariant",
        "line_field_ids": list(result.line_field_ids),
        "line_values": list(result.line_values),
        "shipment_total_field_id": getattr(shipment_total_field, "id", None),
    }
    explanation = (
        "The sum of plant-line Entered Value allocations does not equal the declared shipment total. "
        "Resolution is required before final preparation can be completed."
    )
    if issue is None:
        issue = ReconciliationIssue(
            organization_id=org_id,
            operation_reference=f"us_lacey:{operation.public_id}",
            fingerprint=fingerprint,
            rule_code=ENTERED_VALUE_RECONCILIATION_RULE,
            severity="BLOCKING",
            status="OPEN",
            field_name=ENTERED_VALUE_RECONCILIATION_FIELD,
            us_lacey_operation_field_id=None,
            left_document_id=None,
            right_document_id=(
                shipment_total_field.source_assurance_document_id
                if shipment_total_field is not None
                else None
            ),
            left_source="operation:plant_line_entered_value_allocations",
            right_source=(
                f"assurance:{shipment_total_field.source_assurance_document_id}:{shipment_total_field.source_locator or 'shipment_total'}"
                if shipment_total_field is not None
                and shipment_total_field.source_assurance_document_id is not None
                else "operation:shipment_total_entered_value"
            ),
            explanation=explanation,
        )
        session.add(issue)

    issue.severity = "BLOCKING"
    issue.status = "OPEN"
    issue.field_name = ENTERED_VALUE_RECONCILIATION_FIELD
    issue.us_lacey_operation_field_id = None
    issue.left_value = format(line_total, "f")
    issue.right_value = format(shipment_total, "f")
    issue.delta_numeric = line_total - shipment_total
    issue.explanation = explanation
    issue.evidence_json = evidence
    issue.resolution_justification = None
    issue.resolved_at = None
    return result


def reconcile_entered_value_operation(
    *,
    organization_id: int,
    operation_public_id: UUID | str,
) -> tuple[EnteredValueReconciliation, str] | None:
    """Recompute and persist the invariant after any externally committed review edit.

    This convenience boundary is used by the HTMX review actions and the completion
    guard. It deliberately opens its own tenant-scoped transaction so the invariant
    remains current even when the legacy review service has already committed.
    """
    try:
        public_id = (
            operation_public_id
            if isinstance(operation_public_id, UUID)
            else UUID(str(operation_public_id))
        )
    except (ValueError, TypeError, AttributeError):
        return None

    org_id = int(organization_id)
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, org_id)
        operation = session.scalar(
            select(UsLaceyOperation).where(
                UsLaceyOperation.organization_id == org_id,
                UsLaceyOperation.public_id == public_id,
            )
        )
        if operation is None:
            return None
        result = reconcile_entered_value_invariant(
            session,
            organization_id=org_id,
            operation=operation,
        )
        # Lazy import avoids a module cycle: projection owns the canonical aggregate
        # status derivation and itself imports the low-level invariant helpers above.
        from litoral_trace.us_lacey.projection import refresh_us_lacey_operation_status

        status = refresh_us_lacey_operation_status(
            session,
            organization_id=org_id,
            operation=operation,
        )
        session.commit()
        return result, status
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
