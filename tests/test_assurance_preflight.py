from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from litoral_trace.assurance.domain import ReconciliationSeverity
from litoral_trace.assurance.preflight import (
    PreflightDocument,
    PreflightInput,
    PreflightSignalState,
    PreflightStatus,
    evaluate_preflight,
    reason_catalog_payload,
)
from litoral_trace.assurance.reconciliation import ReconciliationFinding


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "assurance_operation_v1.json"


def _ready_input(**overrides) -> PreflightInput:
    values = {
        "customer_reference": "Buyer Demo GmbH",
        "market": "US",
        "product": "Madera aserrada de pino",
        "quantity": Decimal("80"),
        "commitment_date": date(2026, 9, 18),
        "stock_available": Decimal("80"),
        "origin_state": PreflightSignalState.READY,
        "genealogy_state": PreflightSignalState.READY,
        "phytosanitary_state": PreflightSignalState.NOT_APPLICABLE,
        "eudr_state": PreflightSignalState.NOT_APPLICABLE,
    }
    values.update(overrides)
    return PreflightInput(**values)


def _finding(severity: ReconciliationSeverity) -> ReconciliationFinding:
    return ReconciliationFinding(
        rule_code="TEST_RULE",
        severity=severity,
        field_name="quantity",
        left_source="factura.pdf [quantity]",
        left_value="80",
        right_source="remito.pdf [quantity]",
        right_value="75",
        explanation="La cantidad no coincide.",
        evidence=(
            {"source": "factura.pdf", "field_name": "quantity", "value": "80"},
            {"source": "remito.pdf", "field_name": "quantity", "value": "75"},
        ),
    )


def test_canonical_pilot_fixture_produces_expected_conditional_preflight():
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    order = fixture["order"]
    documents = tuple(
        PreflightDocument(
            document_type=item["semantic_type"],
            reference=item["filename"],
            valid_until=(
                date.fromisoformat(item["valid_until"])
                if item.get("valid_until")
                else None
            ),
        )
        for item in fixture["documents"]
    )
    payload = PreflightInput(
        customer_reference=order["customer"],
        market=order["market"],
        product=order["product"],
        quantity=Decimal(str(order["quantity_m3"])),
        commitment_date=date.fromisoformat(order["commitment_date"]),
        stock_available=Decimal("80"),
        documents=documents,
        origin_state=PreflightSignalState.READY,
        genealogy_state=PreflightSignalState.READY,
        phytosanitary_state=PreflightSignalState.READY,
        eudr_state=PreflightSignalState.READY,
    )

    result = evaluate_preflight(payload)
    expected = fixture["expected_preflight"]

    assert result.status.value == expected["status"]
    assert list(result.reason_codes) == expected["reason_codes"]
    assert result.requires_human_action is expected["requires_human_action"]
    assert result.reasons[0].source == "certificado_fitosanitario_EU2841.pdf"
    assert result.reasons[0].action
    assert result.reasons[0].explanation


def test_fully_closed_operation_is_ready():
    result = evaluate_preflight(_ready_input())

    assert result.status == PreflightStatus.READY
    assert result.reasons == ()
    assert result.requires_human_action is False


def test_insufficient_stock_is_blocking():
    result = evaluate_preflight(_ready_input(stock_available=Decimal("79.99")))

    assert result.status == PreflightStatus.BLOCKED
    assert "INSUFFICIENT_STOCK" in result.reason_codes


def test_origin_or_genealogy_blocking_dominates_pending_conditions():
    result = evaluate_preflight(
        _ready_input(
            origin_state=PreflightSignalState.BLOCKED,
            genealogy_state=PreflightSignalState.PENDING,
        )
    )

    assert result.status == PreflightStatus.BLOCKED
    assert "ORIGIN_BLOCKED" in result.reason_codes
    assert "GENEALOGY_PENDING" in result.reason_codes


def test_missing_required_document_is_conditional_and_explainable():
    result = evaluate_preflight(
        _ready_input(
            documents=(PreflightDocument("INVOICE", "factura.pdf"),),
            required_document_types=("INVOICE", "CUSTOMS_DOCUMENT"),
        )
    )

    assert result.status == PreflightStatus.CONDITIONAL
    assert result.reason_codes == ("REQUIRED_DOCUMENT_MISSING",)
    reason = result.reasons[0]
    assert reason.source == "required_document:CUSTOMS_DOCUMENT"
    assert reason.action
    assert reason.explanation


def test_eudr_is_fail_closed_for_eu_but_not_applied_to_non_eu_market():
    eu_unassessed = evaluate_preflight(
        _ready_input(
            market="DE",
            phytosanitary_state=PreflightSignalState.READY,
            eudr_state=PreflightSignalState.UNASSESSED,
        )
    )
    eu_na = evaluate_preflight(
        _ready_input(
            market="DE",
            phytosanitary_state=PreflightSignalState.READY,
            eudr_state=PreflightSignalState.NOT_APPLICABLE,
        )
    )
    non_eu = evaluate_preflight(
        _ready_input(market="US", eudr_state=PreflightSignalState.UNASSESSED)
    )

    assert eu_unassessed.status == PreflightStatus.BLOCKED
    assert "EUDR_UNASSESSED" in eu_unassessed.reason_codes
    assert eu_na.status == PreflightStatus.BLOCKED
    assert "EUDR_UNASSESSED" in eu_na.reason_codes
    assert non_eu.status == PreflightStatus.READY
    assert all(not code.startswith("EUDR_") for code in non_eu.reason_codes)


def test_reconciliation_is_part_of_preflight_decision():
    blocked = evaluate_preflight(
        _ready_input(reconciliation_findings=(_finding(ReconciliationSeverity.BLOCKING),))
    )
    warning = evaluate_preflight(
        _ready_input(reconciliation_findings=(_finding(ReconciliationSeverity.WARNING),))
    )

    assert blocked.status == PreflightStatus.BLOCKED
    assert blocked.reason_codes == ("RECONCILIATION_BLOCKING",)
    assert blocked.reasons[0].source == "factura.pdf [quantity]"
    assert warning.status == PreflightStatus.CONDITIONAL
    assert warning.reason_codes == ("RECONCILIATION_WARNING",)


def test_malformed_minimum_values_fail_closed_without_raising():
    payload = _ready_input(quantity="not-a-number", stock_available="also-bad")
    result = evaluate_preflight(payload)

    assert result.status == PreflightStatus.BLOCKED
    assert "INVALID_MINIMUM_INPUT" in result.reason_codes
    assert "quantity" in result.reasons[0].explanation
    assert "stock_available" in result.reasons[0].explanation


def test_reason_catalog_has_unique_codes_and_separates_state_from_explanation():
    catalog = reason_catalog_payload()
    codes = [item["code"] for item in catalog]

    assert len(codes) == len(set(codes))
    assert codes == sorted(codes)
    assert catalog
    for item in catalog:
        assert item["default_status"] in {"CONDITIONAL", "BLOCKED"}
        assert item["explanation"]
        assert item["action"]
        assert item["category"]
