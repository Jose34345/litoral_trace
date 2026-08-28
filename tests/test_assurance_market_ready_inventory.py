from __future__ import annotations

from dataclasses import fields
from datetime import date
from decimal import Decimal

import pytest

from litoral_trace.assurance.market_ready_inventory import (
    InventoryStockCandidate,
    MarketReadyCell,
    MarketReadyTarget,
    build_market_ready_matrix,
    evaluate_inventory_cell,
)
from litoral_trace.assurance.preflight import (
    PreflightDocument,
    PreflightSignalState,
    PreflightStatus,
)


def stock(ref: str, qty: str, *, invoice: bool = True, product: str = "Pino", unit: str = "M3", origin=PreflightSignalState.READY):
    docs = (PreflightDocument("INVOICE", f"INV-{ref}", date(2027, 12, 31)),) if invoice else ()
    return InventoryStockCandidate(
        reference=ref,
        product=product,
        available=Decimal(qty),
        unit=unit,
        documents=docs,
        origin_state=origin,
        genealogy_state=PreflightSignalState.READY,
    )


def target(*, qty: str = "40", product: str = "Pino", unit: str = "M3"):
    return MarketReadyTarget(
        reference="DE-CLIENTE",
        customer_reference="Comprador UE",
        market="DE",
        product=product,
        requested_quantity=Decimal(qty),
        unit=unit,
        commitment_date=date(2027, 2, 15),
        required_document_types=("INVOICE",),
        phytosanitary_state=PreflightSignalState.READY,
        eudr_state=PreflightSignalState.READY,
    )


def test_ready_conditional_and_blocked_cells_reuse_preflight():
    ready = evaluate_inventory_cell(stock=stock("READY", "60"), target=target())
    conditional = evaluate_inventory_cell(stock=stock("COND", "60", invoice=False), target=target())
    blocked = evaluate_inventory_cell(stock=stock("SHORT", "20"), target=target())

    assert ready.status == PreflightStatus.READY
    assert ready.reason_codes == ()
    assert conditional.status == PreflightStatus.CONDITIONAL
    assert "REQUIRED_DOCUMENT_MISSING" in conditional.reason_codes
    assert blocked.status == PreflightStatus.BLOCKED
    assert "INSUFFICIENT_STOCK" in blocked.reason_codes


def test_product_and_unit_mismatch_fail_closed_with_reason():
    wrong_product = evaluate_inventory_cell(
        stock=stock("OTHER", "60", product="MDF"), target=target()
    )
    wrong_unit = evaluate_inventory_cell(
        stock=stock("TON", "60", unit="TON"), target=target()
    )
    assert wrong_product.reason_codes == ("STOCK_PRODUCT_MISMATCH",)
    assert wrong_unit.reason_codes == ("STOCK_UNIT_MISMATCH",)
    assert wrong_product.status == PreflightStatus.BLOCKED
    assert wrong_unit.status == PreflightStatus.BLOCKED


def test_matrix_has_stock_rows_target_columns_and_status_totals():
    matrix = build_market_ready_matrix(
        stocks=(
            stock("READY", "60"),
            stock("COND", "30", invoice=False),
            stock("BLOCK", "20", origin=PreflightSignalState.BLOCKED),
        ),
        targets=(target(qty="10"),),
    )
    assert len(matrix.stocks) == 3
    assert len(matrix.targets) == 1
    assert len(matrix.cells) == 3
    totals = matrix.totals[0]
    assert totals.ready_quantity == Decimal("60")
    assert totals.conditional_quantity == Decimal("30")
    assert totals.blocked_quantity == Decimal("20")
    assert totals.unit == "M3"


def test_cell_exposes_concrete_reasons():
    matrix = build_market_ready_matrix(
        stocks=(stock("COND", "60", invoice=False),),
        targets=(target(),),
    )
    cell = matrix.cell(stock_reference="COND", target_reference="DE-CLIENTE")
    assert cell.reasons
    assert cell.reasons[0].explanation
    assert cell.reasons[0].action


def test_alternative_is_deterministic_smallest_surplus_then_code():
    matrix = build_market_ready_matrix(
        stocks=(
            stock("SHORT", "10"),
            stock("READY-100", "100"),
            stock("READY-45-B", "45"),
            stock("READY-45-A", "45"),
        ),
        targets=(target(qty="40"),),
    )
    cell = matrix.cell(stock_reference="SHORT", target_reference="DE-CLIENTE")
    assert cell.status == PreflightStatus.BLOCKED
    assert cell.alternative_stock_reference == "READY-45-A"
    assert matrix.cell(stock_reference="READY-45-A", target_reference="DE-CLIENTE").alternative_stock_reference is None


def test_unit_mismatch_is_not_mixed_into_market_quantity_totals():
    matrix = build_market_ready_matrix(
        stocks=(stock("M3", "25"), stock("TON", "80", unit="TON")),
        targets=(target(qty="10"),),
    )
    totals = matrix.totals[0]
    assert totals.ready_quantity == Decimal("25")
    assert totals.blocked_quantity == Decimal("0")
    assert matrix.cell(stock_reference="TON", target_reference="DE-CLIENTE").reason_codes == ("STOCK_UNIT_MISMATCH",)


def test_matrix_rejects_empty_or_duplicate_dimensions():
    with pytest.raises(ValueError):
        build_market_ready_matrix(stocks=(), targets=(target(),))
    with pytest.raises(ValueError):
        build_market_ready_matrix(stocks=(stock("A", "10"),), targets=())
    with pytest.raises(ValueError):
        build_market_ready_matrix(
            stocks=(stock("A", "10"), stock("A", "20")),
            targets=(target(),),
        )


def test_v1_has_no_economic_optimizer_contract():
    names = {field.name for field in fields(MarketReadyCell)}
    assert names.isdisjoint({"price", "cost", "revenue", "margin", "optimized_allocation"})
