from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from litoral_trace.assurance.domain import (
    ReconciliationIssueStatus,
    ReconciliationSeverity,
)
from litoral_trace.assurance.preflight import (
    PreflightInput,
    PreflightSignalState,
    PreflightStatus,
)
from litoral_trace.assurance.preflight_service import AssurancePreflightService
from litoral_trace.assurance.reconciliation import ReconciliationFinding
from litoral_trace.db.models import ReconciliationIssue


def _factory():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    ReconciliationIssue.__table__.create(engine, checkfirst=True)
    return sessionmaker(bind=engine, expire_on_commit=False)


def _payload(*, injected_findings=()) -> PreflightInput:
    return PreflightInput(
        customer_reference="Buyer",
        market="US",
        product="Madera",
        quantity=Decimal("80"),
        commitment_date=date(2026, 9, 18),
        stock_available=Decimal("80"),
        origin_state=PreflightSignalState.READY,
        genealogy_state=PreflightSignalState.READY,
        phytosanitary_state=PreflightSignalState.NOT_APPLICABLE,
        eudr_state=PreflightSignalState.NOT_APPLICABLE,
        reconciliation_findings=tuple(injected_findings),
    )


def _issue(*, org: int, operation: str, fingerprint: str, severity: str, status: str):
    return ReconciliationIssue(
        organization_id=org,
        operation_reference=operation,
        fingerprint=fingerprint,
        rule_code="QTY_MISMATCH",
        severity=severity,
        status=status,
        field_name="quantity",
        left_source="factura.pdf [quantity]",
        right_source="remito.pdf [quantity]",
        left_value="80",
        right_value="75",
        explanation="La cantidad no coincide.",
        evidence_json={
            "sources": [
                {"source": "factura.pdf", "field_name": "quantity", "value": "80"},
                {"source": "remito.pdf", "field_name": "quantity", "value": "75"},
            ]
        },
    )


def test_preflight_service_uses_only_open_tenant_operation_issues():
    factory = _factory()
    seed: Session = factory()
    seed.add_all(
        [
            _issue(
                org=42,
                operation="shipment:abc",
                fingerprint="1" * 64,
                severity=ReconciliationSeverity.BLOCKING.value,
                status=ReconciliationIssueStatus.OPEN.value,
            ),
            _issue(
                org=42,
                operation="shipment:abc",
                fingerprint="2" * 64,
                severity=ReconciliationSeverity.WARNING.value,
                status=ReconciliationIssueStatus.RESOLVED.value,
            ),
            _issue(
                org=99,
                operation="shipment:abc",
                fingerprint="3" * 64,
                severity=ReconciliationSeverity.BLOCKING.value,
                status=ReconciliationIssueStatus.OPEN.value,
            ),
            _issue(
                org=42,
                operation="shipment:other",
                fingerprint="4" * 64,
                severity=ReconciliationSeverity.BLOCKING.value,
                status=ReconciliationIssueStatus.OPEN.value,
            ),
        ]
    )
    seed.commit()
    seed.close()

    view = AssurancePreflightService(session_factory=factory).evaluate(
        organization_id=42,
        operation_reference="shipment:abc",
        payload=_payload(),
    )

    assert view.open_reconciliation_issue_count == 1
    assert view.result.status == PreflightStatus.BLOCKED
    assert view.result.reason_codes == ("RECONCILIATION_BLOCKING",)
    assert view.result.reasons[0].source == "factura.pdf [quantity]"


def test_preflight_service_does_not_trust_client_supplied_reconciliation_findings():
    factory = _factory()
    injected = ReconciliationFinding(
        rule_code="INJECTED",
        severity=ReconciliationSeverity.BLOCKING,
        field_name="quantity",
        left_source="client",
        left_value="1",
        right_source="client",
        right_value="2",
        explanation="No debe entrar.",
        evidence=(),
    )

    view = AssurancePreflightService(session_factory=factory).evaluate(
        organization_id=42,
        operation_reference="shipment:no-issues",
        payload=_payload(injected_findings=(injected,)),
    )

    assert view.open_reconciliation_issue_count == 0
    assert view.result.status == PreflightStatus.READY
    assert view.result.reasons == ()
