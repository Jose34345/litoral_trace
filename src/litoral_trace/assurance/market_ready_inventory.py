"""Deterministic Market-Ready Inventory Matrix for Assurance v1.

This module is intentionally a projection layer over the existing Preflight
engine. It does not introduce a second compliance decision system and it does
not optimize economic allocation. Each active stock candidate is evaluated
against each market/customer target with the same explainable READY /
CONDITIONAL / BLOCKED rules used by operational Preflight.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Sequence

from litoral_trace.assurance.preflight import (
    PreflightDocument,
    PreflightInput,
    PreflightReason,
    PreflightSignalState,
    PreflightStatus,
    evaluate_preflight,
)
from litoral_trace.assurance.reconciliation import ReconciliationFinding


@dataclass(frozen=True, slots=True)
class InventoryStockCandidate:
    reference: str
    product: str
    available: Decimal
    unit: str
    documents: tuple[PreflightDocument, ...] = ()
    origin_state: PreflightSignalState = PreflightSignalState.UNASSESSED
    genealogy_state: PreflightSignalState = PreflightSignalState.UNASSESSED
    reconciliation_findings: tuple[ReconciliationFinding, ...] = ()


@dataclass(frozen=True, slots=True)
class MarketReadyTarget:
    reference: str
    customer_reference: str
    market: str
    product: str
    requested_quantity: Decimal
    unit: str
    commitment_date: date
    required_document_types: tuple[str, ...] = ()
    phytosanitary_state: PreflightSignalState = PreflightSignalState.UNASSESSED
    eudr_state: PreflightSignalState = PreflightSignalState.UNASSESSED


@dataclass(frozen=True, slots=True)
class MarketReadyReason:
    code: str
    status: PreflightStatus
    explanation: str
    action: str
    source: str | None = None


@dataclass(frozen=True, slots=True)
class MarketReadyCell:
    stock_reference: str
    target_reference: str
    status: PreflightStatus
    reason_codes: tuple[str, ...]
    reasons: tuple[MarketReadyReason, ...]
    available: Decimal
    unit: str
    alternative_stock_reference: str | None = None


@dataclass(frozen=True, slots=True)
class MarketReadyTargetTotals:
    target_reference: str
    ready_quantity: Decimal
    conditional_quantity: Decimal
    blocked_quantity: Decimal
    unit: str


@dataclass(frozen=True, slots=True)
class MarketReadyMatrix:
    stocks: tuple[InventoryStockCandidate, ...]
    targets: tuple[MarketReadyTarget, ...]
    cells: tuple[MarketReadyCell, ...]
    totals: tuple[MarketReadyTargetTotals, ...]

    def cell(self, *, stock_reference: str, target_reference: str) -> MarketReadyCell:
        for item in self.cells:
            if (
                item.stock_reference == stock_reference
                and item.target_reference == target_reference
            ):
                return item
        raise KeyError(f"Celda no encontrada: {stock_reference} x {target_reference}")


def _decimal(value: object, *, field: str) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} debe ser numérico.") from exc
    if not result.is_finite():
        raise ValueError(f"{field} debe ser finito.")
    return result


def _required_text(value: object, *, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} es obligatorio.")
    return normalized


def _normalize_product(value: str) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def _normalize_unit(value: str) -> str:
    return str(value or "").strip().upper()


def _from_preflight(reason: PreflightReason) -> MarketReadyReason:
    return MarketReadyReason(
        code=reason.code,
        status=reason.status,
        explanation=reason.explanation,
        action=reason.action,
        source=reason.source,
    )


def _matrix_reason(
    code: str,
    explanation: str,
    action: str,
    *,
    source: str,
) -> MarketReadyReason:
    return MarketReadyReason(
        code=code,
        status=PreflightStatus.BLOCKED,
        explanation=explanation,
        action=action,
        source=source,
    )


def _validate_stock(stock: InventoryStockCandidate) -> None:
    _required_text(stock.reference, field="stock.reference")
    _required_text(stock.product, field="stock.product")
    _required_text(stock.unit, field="stock.unit")
    available = _decimal(stock.available, field="stock.available")
    if available < 0:
        raise ValueError("stock.available no puede ser negativo.")


def _validate_target(target: MarketReadyTarget) -> None:
    _required_text(target.reference, field="target.reference")
    _required_text(target.customer_reference, field="target.customer_reference")
    _required_text(target.market, field="target.market")
    _required_text(target.product, field="target.product")
    _required_text(target.unit, field="target.unit")
    quantity = _decimal(target.requested_quantity, field="target.requested_quantity")
    if quantity <= 0:
        raise ValueError("target.requested_quantity debe ser mayor que cero.")
    if not isinstance(target.commitment_date, date):
        raise ValueError("target.commitment_date debe ser una fecha.")


def evaluate_inventory_cell(
    *,
    stock: InventoryStockCandidate,
    target: MarketReadyTarget,
) -> MarketReadyCell:
    """Evaluate one stock x market/customer cell using operational Preflight."""
    _validate_stock(stock)
    _validate_target(target)
    available = _decimal(stock.available, field="stock.available")

    if _normalize_product(stock.product) != _normalize_product(target.product):
        reason = _matrix_reason(
            "STOCK_PRODUCT_MISMATCH",
            f"El stock {stock.reference} corresponde a {stock.product} y el objetivo requiere {target.product}.",
            "Seleccionar stock del mismo producto requerido por la operación.",
            source=stock.reference,
        )
        return MarketReadyCell(
            stock_reference=stock.reference,
            target_reference=target.reference,
            status=PreflightStatus.BLOCKED,
            reason_codes=(reason.code,),
            reasons=(reason,),
            available=available,
            unit=_normalize_unit(stock.unit),
        )

    if _normalize_unit(stock.unit) != _normalize_unit(target.unit):
        reason = _matrix_reason(
            "STOCK_UNIT_MISMATCH",
            f"El stock {stock.reference} está expresado en {stock.unit} y el objetivo usa {target.unit}.",
            "Normalizar o seleccionar stock en la misma unidad antes de asignarlo.",
            source=stock.reference,
        )
        return MarketReadyCell(
            stock_reference=stock.reference,
            target_reference=target.reference,
            status=PreflightStatus.BLOCKED,
            reason_codes=(reason.code,),
            reasons=(reason,),
            available=available,
            unit=_normalize_unit(stock.unit),
        )

    result = evaluate_preflight(
        PreflightInput(
            customer_reference=target.customer_reference,
            market=target.market,
            product=target.product,
            quantity=_decimal(
                target.requested_quantity,
                field="target.requested_quantity",
            ),
            commitment_date=target.commitment_date,
            stock_available=available,
            documents=stock.documents,
            required_document_types=target.required_document_types,
            origin_state=stock.origin_state,
            genealogy_state=stock.genealogy_state,
            phytosanitary_state=target.phytosanitary_state,
            eudr_state=target.eudr_state,
            reconciliation_findings=stock.reconciliation_findings,
        )
    )
    reasons = tuple(_from_preflight(reason) for reason in result.reasons)
    return MarketReadyCell(
        stock_reference=stock.reference,
        target_reference=target.reference,
        status=result.status,
        reason_codes=tuple(reason.code for reason in reasons),
        reasons=reasons,
        available=available,
        unit=_normalize_unit(stock.unit),
    )


def _alternative_for(
    *,
    blocked_cell: MarketReadyCell,
    target: MarketReadyTarget,
    stocks_by_reference: dict[str, InventoryStockCandidate],
    cells: Sequence[MarketReadyCell],
) -> str | None:
    requested = _decimal(target.requested_quantity, field="target.requested_quantity")
    candidates: list[tuple[Decimal, str]] = []
    for cell in cells:
        if cell.target_reference != blocked_cell.target_reference:
            continue
        if cell.stock_reference == blocked_cell.stock_reference:
            continue
        if cell.status != PreflightStatus.READY:
            continue
        stock = stocks_by_reference[cell.stock_reference]
        if _normalize_product(stock.product) != _normalize_product(target.product):
            continue
        if _normalize_unit(stock.unit) != _normalize_unit(target.unit):
            continue
        available = _decimal(stock.available, field="stock.available")
        if available < requested:
            continue
        candidates.append((available - requested, stock.reference))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1].casefold(), item[1]))
    return candidates[0][1]


def _totals_for_target(
    *,
    target: MarketReadyTarget,
    cells: Sequence[MarketReadyCell],
) -> MarketReadyTargetTotals:
    unit = _normalize_unit(target.unit)
    totals = {
        PreflightStatus.READY: Decimal("0"),
        PreflightStatus.CONDITIONAL: Decimal("0"),
        PreflightStatus.BLOCKED: Decimal("0"),
    }
    for cell in cells:
        if cell.target_reference != target.reference:
            continue
        if cell.unit != unit:
            # Unit-mismatch cells are intentionally not arithmetically mixed
            # into market totals. Their reason remains visible in the matrix.
            continue
        totals[cell.status] += cell.available
    return MarketReadyTargetTotals(
        target_reference=target.reference,
        ready_quantity=totals[PreflightStatus.READY],
        conditional_quantity=totals[PreflightStatus.CONDITIONAL],
        blocked_quantity=totals[PreflightStatus.BLOCKED],
        unit=unit,
    )


def build_market_ready_matrix(
    *,
    stocks: Sequence[InventoryStockCandidate],
    targets: Sequence[MarketReadyTarget],
) -> MarketReadyMatrix:
    """Build a deterministic stock x market/customer readiness projection."""
    stock_tuple = tuple(stocks)
    target_tuple = tuple(targets)
    if not stock_tuple:
        raise ValueError("Debe existir al menos un stock para construir la matriz.")
    if not target_tuple:
        raise ValueError("Debe existir al menos un mercado/cliente objetivo.")

    stock_refs = [stock.reference for stock in stock_tuple]
    target_refs = [target.reference for target in target_tuple]
    if len(set(stock_refs)) != len(stock_refs):
        raise ValueError("Las referencias de stock no pueden repetirse.")
    if len(set(target_refs)) != len(target_refs):
        raise ValueError("Las referencias de mercado/cliente no pueden repetirse.")

    for stock in stock_tuple:
        _validate_stock(stock)
    for target in target_tuple:
        _validate_target(target)

    base_cells = tuple(
        evaluate_inventory_cell(stock=stock, target=target)
        for stock in stock_tuple
        for target in target_tuple
    )
    stocks_by_reference = {stock.reference: stock for stock in stock_tuple}
    targets_by_reference = {target.reference: target for target in target_tuple}
    cells = tuple(
        replace(
            cell,
            alternative_stock_reference=(
                None
                if cell.status == PreflightStatus.READY
                else _alternative_for(
                    blocked_cell=cell,
                    target=targets_by_reference[cell.target_reference],
                    stocks_by_reference=stocks_by_reference,
                    cells=base_cells,
                )
            ),
        )
        for cell in base_cells
    )
    totals = tuple(
        _totals_for_target(target=target, cells=cells)
        for target in target_tuple
    )
    return MarketReadyMatrix(
        stocks=stock_tuple,
        targets=target_tuple,
        cells=cells,
        totals=totals,
    )
