from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from litoral_trace.assurance.domain import (
    OperationalExceptionPriority,
    OperationalExceptionSource,
    OperationalExceptionStatus,
    ReconciliationIssueStatus,
    ReconciliationSeverity,
)
from litoral_trace.assurance.operational_exceptions import (
    AssuranceOperationalExceptionService,
)
from litoral_trace.assurance.preflight import (
    PreflightInput,
    PreflightSignalState,
    PreflightStatus,
    evaluate_preflight,
)
from litoral_trace.assurance.preflight_service import AssurancePreflightView
from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentEntityLink,
    OperationalException,
    ReconciliationIssue,
)


def _factory():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    # Resolving an exception can immediately re-run Preflight, which now checks
    # operation-linked Assurance documents. Keep the unit schema aligned with
    # that production dependency rather than bypassing the safety gate.
    AssuranceDocument.__table__.create(engine, checkfirst=True)
    DocumentEntityLink.__table__.create(engine, checkfirst=True)
    ReconciliationIssue.__table__.create(engine, checkfirst=True)
    OperationalException.__table__.create(engine, checkfirst=True)
    return sessionmaker(bind=engine, expire_on_commit=False)


def _issue(*, org: int = 42, operation: str = "shipment:abc") -> ReconciliationIssue:
    return ReconciliationIssue(
        organization_id=org,
        operation_reference=operation,
        fingerprint="a" * 64,
        rule_code="QTY_MISMATCH",
        severity=ReconciliationSeverity.BLOCKING.value,
        status=ReconciliationIssueStatus.OPEN.value,
        field_name="quantity",
        left_source="factura.pdf [quantity]",
        right_source="remito.pdf [quantity]",
        left_value="80",
        right_value="75",
        explanation="La cantidad no coincide.",
        evidence_json={"sources": [{"source": "factura.pdf", "value": "80"}]},
    )


def _ready_payload(*, stock: str = "80") -> PreflightInput:
    return PreflightInput(
        customer_reference="Buyer",
        market="US",
        product="Madera",
        quantity=Decimal("80"),
        commitment_date=date(2026, 9, 18),
        stock_available=Decimal(stock),
        origin_state=PreflightSignalState.READY,
        genealogy_state=PreflightSignalState.READY,
        phytosanitary_state=PreflightSignalState.NOT_APPLICABLE,
        eudr_state=PreflightSignalState.NOT_APPLICABLE,
    )


def test_reconciliation_sync_is_idempotent_and_reopens_when_source_remains_open():
    factory = _factory()
    seed: Session = factory()
    seed.add(_issue())
    seed.commit()
    seed.close()

    service = AssuranceOperationalExceptionService(session_factory=factory)
    first = service.sync_reconciliation(organization_id=42)
    second = service.sync_reconciliation(organization_id=42)

    assert first.created_count == 1
    assert second.created_count == 0
    assert second.refreshed_count == 1

    session: Session = factory()
    rows = session.scalars(select(OperationalException)).all()
    assert len(rows) == 1
    row = rows[0]
    assert row.source_type == OperationalExceptionSource.RECONCILIATION.value
    assert row.priority == OperationalExceptionPriority.CRITICAL.value
    row.status = OperationalExceptionStatus.RESOLVED.value
    row.resolution_note = "Cierre provisional"
    session.commit()
    session.close()

    reopened = service.sync_reconciliation(organization_id=42)
    assert reopened.reopened_count == 1
    check: Session = factory()
    stored = check.scalar(select(OperationalException))
    assert stored.status == OperationalExceptionStatus.OPEN.value
    assert stored.resolution_note is None
    check.close()


def test_reconciliation_exception_auto_resolves_when_source_is_no_longer_open():
    factory = _factory()
    seed: Session = factory()
    source = _issue()
    seed.add(source)
    seed.commit()
    seed.close()

    service = AssuranceOperationalExceptionService(session_factory=factory)
    service.sync_reconciliation(organization_id=42)

    mutate: Session = factory()
    source = mutate.scalar(select(ReconciliationIssue))
    source.status = ReconciliationIssueStatus.RESOLVED.value
    mutate.commit()
    mutate.close()

    result = service.sync_reconciliation(organization_id=42)
    assert result.auto_resolved_count == 1

    check: Session = factory()
    row = check.scalar(select(OperationalException))
    assert row.status == OperationalExceptionStatus.RESOLVED.value
    assert "fuente ya no está abierta" in row.resolution_note
    check.close()


def test_resolve_reconciliation_exception_requires_justification_and_recomputes_preflight():
    factory = _factory()
    seed: Session = factory()
    seed.add(_issue())
    seed.commit()
    seed.close()

    service = AssuranceOperationalExceptionService(session_factory=factory)
    service.sync_reconciliation(organization_id=42)
    read: Session = factory()
    exception = read.scalar(select(OperationalException))
    public_id = exception.public_id
    read.close()

    result = service.resolve(
        organization_id=42,
        exception_public_id=public_id,
        resolved_by_user_id=7,
        resolution_note="Diferencia revisada contra documentos originales y aceptada.",
        preflight_payload=_ready_payload(),
    )

    assert result.source_status == ReconciliationIssueStatus.ACCEPTED_WITH_JUSTIFICATION.value
    assert result.preflight is not None
    assert result.preflight.result.status == PreflightStatus.READY
    assert result.preflight.open_reconciliation_issue_count == 0

    check: Session = factory()
    source = check.scalar(select(ReconciliationIssue))
    row = check.scalar(
        select(OperationalException).where(
            OperationalException.source_type == OperationalExceptionSource.RECONCILIATION.value
        )
    )
    assert source.status == ReconciliationIssueStatus.ACCEPTED_WITH_JUSTIFICATION.value
    assert source.resolution_justification.startswith("Diferencia revisada")
    assert row.status == OperationalExceptionStatus.RESOLVED.value
    assert row.resolved_by_user_id == 7
    check.close()


def test_preflight_sync_materializes_blocker_and_auto_closes_when_condition_disappears():
    factory = _factory()
    service = AssuranceOperationalExceptionService(session_factory=factory)

    blocked_view = AssurancePreflightView(
        operation_reference="order:123",
        result=evaluate_preflight(_ready_payload(stock="20")),
        open_reconciliation_issue_count=0,
    )
    created = service.sync_preflight(organization_id=42, view=blocked_view)
    assert created.created_count == 1

    check: Session = factory()
    row = check.scalar(
        select(OperationalException).where(
            OperationalException.source_type == OperationalExceptionSource.PREFLIGHT.value
        )
    )
    assert row.cause_code == "INSUFFICIENT_STOCK"
    assert row.impact == ReconciliationSeverity.BLOCKING.value
    assert row.priority == OperationalExceptionPriority.CRITICAL.value
    check.close()

    ready_view = AssurancePreflightView(
        operation_reference="order:123",
        result=evaluate_preflight(_ready_payload(stock="80")),
        open_reconciliation_issue_count=0,
    )
    resolved = service.sync_preflight(organization_id=42, view=ready_view)
    assert resolved.auto_resolved_count == 1

    final: Session = factory()
    row = final.scalar(
        select(OperationalException).where(
            OperationalException.source_type == OperationalExceptionSource.PREFLIGHT.value
        )
    )
    assert row.status == OperationalExceptionStatus.RESOLVED.value
    final.close()


def test_attention_list_is_tenant_scoped_and_orders_priority_then_deadline():
    factory = _factory()
    now = datetime.now(timezone.utc)
    session: Session = factory()
    rows = [
        OperationalException(
            organization_id=42,
            fingerprint="1" * 64,
            source_type=OperationalExceptionSource.MANUAL.value,
            operation_reference="op-low",
            cause_code="LOW",
            entity_type="OPERATION",
            entity_reference="op-low",
            title="Low",
            description="Low",
            impact=ReconciliationSeverity.INFO.value,
            priority=OperationalExceptionPriority.LOW.value,
            status=OperationalExceptionStatus.OPEN.value,
            due_at=now,
            recommended_action="Revisar",
        ),
        OperationalException(
            organization_id=42,
            fingerprint="2" * 64,
            source_type=OperationalExceptionSource.MANUAL.value,
            operation_reference="op-critical-late",
            cause_code="CRITICAL_LATE",
            entity_type="OPERATION",
            entity_reference="op-critical-late",
            title="Critical late",
            description="Critical late",
            impact=ReconciliationSeverity.BLOCKING.value,
            priority=OperationalExceptionPriority.CRITICAL.value,
            status=OperationalExceptionStatus.OPEN.value,
            due_at=now + timedelta(days=2),
            recommended_action="Revisar",
        ),
        OperationalException(
            organization_id=42,
            fingerprint="3" * 64,
            source_type=OperationalExceptionSource.MANUAL.value,
            operation_reference="op-critical-soon",
            cause_code="CRITICAL_SOON",
            entity_type="OPERATION",
            entity_reference="op-critical-soon",
            title="Critical soon",
            description="Critical soon",
            impact=ReconciliationSeverity.BLOCKING.value,
            priority=OperationalExceptionPriority.CRITICAL.value,
            status=OperationalExceptionStatus.OPEN.value,
            due_at=now + timedelta(hours=1),
            recommended_action="Revisar",
        ),
        OperationalException(
            organization_id=99,
            fingerprint="4" * 64,
            source_type=OperationalExceptionSource.MANUAL.value,
            operation_reference="other-tenant",
            cause_code="SECRET",
            entity_type="OPERATION",
            entity_reference="other-tenant",
            title="Other",
            description="Other tenant",
            impact=ReconciliationSeverity.BLOCKING.value,
            priority=OperationalExceptionPriority.CRITICAL.value,
            status=OperationalExceptionStatus.OPEN.value,
            recommended_action="N/A",
        ),
    ]
    session.add_all(rows)
    session.commit()
    session.close()

    visible = AssuranceOperationalExceptionService(session_factory=factory).list_attention(
        organization_id=42
    )
    assert [row.operation_reference for row in visible] == [
        "op-critical-soon",
        "op-critical-late",
        "op-low",
    ]
    assert all(row.organization_id == 42 for row in visible)
