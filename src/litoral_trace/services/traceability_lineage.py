"""Reverse genealogy queries for buyer-ready chain-of-custody evidence.

P1C deliberately reads the immutable P1A/P1B graph instead of persisting a
second provenance projection. The main commercial query starts at a shipment
and walks backwards through posted producer events until source ``Lote``
records are reached.

Where a transformation mixes more than one homogeneous input, P1C attributes
output provenance according to each input's share of total event input. This
is an explicit accounting convention (``PROPORTIONAL_INPUT_ALLOCATION``), not
an assertion that individual fibres can be physically distinguished after
mixing. Missing producers, cycles, unit inconsistencies, or broken source
links make the lineage incomplete instead of being silently accepted.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    Lote,
    Shipment,
    ShipmentItem,
    TraceabilityBatch,
    TraceabilityEvent,
    TraceabilityEventInput,
    TraceabilityEventOutput,
)
from litoral_trace.db.tenant import set_tenant_db_context


QTY_QUANTUM = Decimal("0.000001")
ZERO = Decimal("0.000000")
ONE = Decimal("1.000000")
MAX_LINEAGE_DEPTH = 64
ALLOCATION_METHOD = "PROPORTIONAL_INPUT_ALLOCATION"


class TraceabilityLineageError(RuntimeError):
    """Base P1C lineage failure."""


class TraceabilityLineageValidationError(TraceabilityLineageError):
    """Invalid lineage query input."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code


class TraceabilityLineageNotFoundError(TraceabilityLineageError):
    """Requested tenant-scoped shipment does not exist."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code


@dataclass(frozen=True)
class _Profile:
    shares: dict[int, Decimal]
    event_ids: frozenset[int]
    issues: tuple[dict[str, Any], ...]


class TraceabilityLineageService:
    """Read-only reverse genealogy service scoped to one tenant."""

    def __init__(
        self,
        *,
        session: Session,
        organization_id: int,
        max_depth: int = MAX_LINEAGE_DEPTH,
    ) -> None:
        if session is None:
            raise TraceabilityLineageValidationError(
                "LINEAGE_SESSION_REQUIRED",
                "Se requiere una sesión de base de datos válida.",
            )
        try:
            normalized_org = int(organization_id)
        except (TypeError, ValueError) as exc:
            raise TraceabilityLineageValidationError(
                "LINEAGE_TENANT_INVALID",
                "organization_id debe ser un entero válido.",
            ) from exc
        if normalized_org <= 0:
            raise TraceabilityLineageValidationError(
                "LINEAGE_TENANT_INVALID",
                "organization_id debe ser mayor que cero.",
            )
        if max_depth <= 0:
            raise TraceabilityLineageValidationError(
                "LINEAGE_DEPTH_INVALID",
                "max_depth debe ser mayor que cero.",
            )

        self.session = session
        self.organization_id = normalized_org
        self.max_depth = max_depth
        set_tenant_db_context(self.session, self.organization_id)

        self._batch_cache: dict[int, TraceabilityBatch | None] = {}
        self._lote_cache: dict[int, Lote | None] = {}
        self._producer_cache: dict[int, list[tuple[TraceabilityEventOutput, TraceabilityEvent]]] = {}
        self._event_inputs_cache: dict[int, list[TraceabilityEventInput]] = {}
        self._event_outputs_cache: dict[int, list[TraceabilityEventOutput]] = {}
        self._event_cache: dict[int, TraceabilityEvent | None] = {}
        self._profile_cache: dict[int, _Profile] = {}

    @staticmethod
    def _q(value: Decimal | int | str) -> Decimal:
        return Decimal(value).quantize(QTY_QUANTUM, rounding=ROUND_HALF_UP)

    @classmethod
    def _decimal_str(cls, value: Decimal | int | str) -> str:
        return f"{cls._q(value):.6f}"

    @staticmethod
    def _issue(
        code: str,
        message: str,
        *,
        batch_id: int | None = None,
        event_id: int | None = None,
    ) -> dict[str, Any]:
        issue: dict[str, Any] = {
            "code": code,
            "message": message,
        }
        if batch_id is not None:
            issue["batch_id"] = batch_id
        if event_id is not None:
            issue["event_id"] = event_id
        return issue

    def _get_batch(self, batch_id: int) -> TraceabilityBatch | None:
        if batch_id not in self._batch_cache:
            self._batch_cache[batch_id] = self.session.execute(
                select(TraceabilityBatch).where(
                    TraceabilityBatch.id == batch_id,
                    TraceabilityBatch.organization_id == self.organization_id,
                )
            ).scalar_one_or_none()
        return self._batch_cache[batch_id]

    def _get_lote(self, lote_id: int) -> Lote | None:
        if lote_id not in self._lote_cache:
            self._lote_cache[lote_id] = self.session.execute(
                select(Lote).where(
                    Lote.id == lote_id,
                    Lote.organization_id == self.organization_id,
                )
            ).scalar_one_or_none()
        return self._lote_cache[lote_id]

    def _get_event(self, event_id: int) -> TraceabilityEvent | None:
        if event_id not in self._event_cache:
            self._event_cache[event_id] = self.session.execute(
                select(TraceabilityEvent).where(
                    TraceabilityEvent.id == event_id,
                    TraceabilityEvent.organization_id == self.organization_id,
                )
            ).scalar_one_or_none()
        return self._event_cache[event_id]

    def _get_producers(
        self,
        batch_id: int,
    ) -> list[tuple[TraceabilityEventOutput, TraceabilityEvent]]:
        if batch_id not in self._producer_cache:
            rows = self.session.execute(
                select(TraceabilityEventOutput, TraceabilityEvent)
                .join(
                    TraceabilityEvent,
                    (TraceabilityEvent.id == TraceabilityEventOutput.event_id)
                    & (
                        TraceabilityEvent.organization_id
                        == TraceabilityEventOutput.organization_id
                    ),
                )
                .where(
                    TraceabilityEventOutput.organization_id == self.organization_id,
                    TraceabilityEventOutput.batch_id == batch_id,
                    TraceabilityEvent.status == "POSTED",
                )
                .order_by(TraceabilityEvent.id)
            ).all()
            self._producer_cache[batch_id] = [
                (output, event) for output, event in rows
            ]
        return self._producer_cache[batch_id]

    def _get_event_inputs(self, event_id: int) -> list[TraceabilityEventInput]:
        if event_id not in self._event_inputs_cache:
            self._event_inputs_cache[event_id] = list(
                self.session.execute(
                    select(TraceabilityEventInput)
                    .where(
                        TraceabilityEventInput.organization_id == self.organization_id,
                        TraceabilityEventInput.event_id == event_id,
                    )
                    .order_by(TraceabilityEventInput.id)
                ).scalars().all()
            )
        return self._event_inputs_cache[event_id]

    def _get_event_outputs(self, event_id: int) -> list[TraceabilityEventOutput]:
        if event_id not in self._event_outputs_cache:
            self._event_outputs_cache[event_id] = list(
                self.session.execute(
                    select(TraceabilityEventOutput)
                    .where(
                        TraceabilityEventOutput.organization_id == self.organization_id,
                        TraceabilityEventOutput.event_id == event_id,
                    )
                    .order_by(TraceabilityEventOutput.id)
                ).scalars().all()
            )
        return self._event_outputs_cache[event_id]

    def _profile_batch(
        self,
        batch_id: int,
        *,
        path: tuple[int, ...] = (),
        depth: int = 0,
    ) -> _Profile:
        if batch_id in self._profile_cache:
            return self._profile_cache[batch_id]

        if depth > self.max_depth:
            return _Profile(
                shares={},
                event_ids=frozenset(),
                issues=(
                    self._issue(
                        "LINEAGE_DEPTH_EXCEEDED",
                        "La genealogía excede la profundidad máxima permitida.",
                        batch_id=batch_id,
                    ),
                ),
            )

        if batch_id in path:
            return _Profile(
                shares={},
                event_ids=frozenset(),
                issues=(
                    self._issue(
                        "LINEAGE_CYCLE_DETECTED",
                        "Se detectó un ciclo en la genealogía industrial.",
                        batch_id=batch_id,
                    ),
                ),
            )

        batch = self._get_batch(batch_id)
        if batch is None:
            return _Profile(
                shares={},
                event_ids=frozenset(),
                issues=(
                    self._issue(
                        "LINEAGE_BATCH_NOT_FOUND",
                        "Un lote industrial referenciado no está disponible para el tenant.",
                        batch_id=batch_id,
                    ),
                ),
            )

        producers = self._get_producers(batch.id)
        producer_event_ids = frozenset(event.id for _, event in producers)

        if batch.source_lote_id is not None:
            lote = self._get_lote(batch.source_lote_id)
            if lote is None:
                profile = _Profile(
                    shares={},
                    event_ids=producer_event_ids,
                    issues=(
                        self._issue(
                            "SOURCE_LOTE_NOT_FOUND",
                            "El lote industrial declara un origen que no puede resolverse.",
                            batch_id=batch.id,
                        ),
                    ),
                )
                self._profile_cache[batch.id] = profile
                return profile

            issues: list[dict[str, Any]] = []
            if len(producers) > 1:
                issues.append(
                    self._issue(
                        "MULTIPLE_POSTED_PRODUCERS",
                        "El lote fuente tiene más de un evento productor contabilizado.",
                        batch_id=batch.id,
                    )
                )
            for _, event in producers:
                if event.event_type != "RECEIPT":
                    issues.append(
                        self._issue(
                            "SOURCE_BATCH_NON_RECEIPT_PRODUCER",
                            "Un lote vinculado a parcela fue producido por un evento distinto de RECEIPT.",
                            batch_id=batch.id,
                            event_id=event.id,
                        )
                    )

            profile = _Profile(
                shares={lote.id: ONE},
                event_ids=producer_event_ids,
                issues=tuple(issues),
            )
            self._profile_cache[batch.id] = profile
            return profile

        if not producers:
            profile = _Profile(
                shares={},
                event_ids=frozenset(),
                issues=(
                    self._issue(
                        "MISSING_PROVENANCE",
                        "El lote industrial no tiene parcela de origen ni evento productor POSTED.",
                        batch_id=batch.id,
                    ),
                ),
            )
            self._profile_cache[batch.id] = profile
            return profile

        if len(producers) != 1:
            profile = _Profile(
                shares={},
                event_ids=producer_event_ids,
                issues=(
                    self._issue(
                        "MULTIPLE_POSTED_PRODUCERS",
                        "El lote industrial tiene más de un evento productor contabilizado.",
                        batch_id=batch.id,
                    ),
                ),
            )
            self._profile_cache[batch.id] = profile
            return profile

        output_edge, event = producers[0]
        inputs = self._get_event_inputs(event.id)
        issues: list[dict[str, Any]] = []

        if output_edge.unit != batch.unit:
            issues.append(
                self._issue(
                    "OUTPUT_UNIT_MISMATCH",
                    "La unidad del output no coincide con la unidad del lote industrial.",
                    batch_id=batch.id,
                    event_id=event.id,
                )
            )

        if not inputs:
            issues.append(
                self._issue(
                    "PRODUCER_WITHOUT_INPUTS",
                    "El evento productor no contiene entradas trazables.",
                    batch_id=batch.id,
                    event_id=event.id,
                )
            )
            profile = _Profile(
                shares={},
                event_ids=frozenset({event.id}),
                issues=tuple(issues),
            )
            self._profile_cache[batch.id] = profile
            return profile

        input_units = {edge.unit for edge in inputs}
        if input_units != {batch.unit}:
            issues.append(
                self._issue(
                    "LINEAGE_UNIT_CONVERSION_REQUIRED",
                    "La genealogía requiere una conversión de unidades no documentada.",
                    batch_id=batch.id,
                    event_id=event.id,
                )
            )
            profile = _Profile(
                shares={},
                event_ids=frozenset({event.id}),
                issues=tuple(issues),
            )
            self._profile_cache[batch.id] = profile
            return profile

        total_input = sum((self._q(edge.quantity) for edge in inputs), ZERO)
        if total_input <= ZERO:
            issues.append(
                self._issue(
                    "PRODUCER_INPUT_TOTAL_INVALID",
                    "El total de entradas del evento productor no es positivo.",
                    batch_id=batch.id,
                    event_id=event.id,
                )
            )
            profile = _Profile(
                shares={},
                event_ids=frozenset({event.id}),
                issues=tuple(issues),
            )
            self._profile_cache[batch.id] = profile
            return profile

        combined: dict[int, Decimal] = defaultdict(lambda: ZERO)
        event_ids: set[int] = {event.id}
        next_path = (*path, batch.id)

        for edge in inputs:
            child = self._profile_batch(
                edge.batch_id,
                path=next_path,
                depth=depth + 1,
            )
            input_weight = self._q(edge.quantity) / total_input
            for lote_id, child_share in child.shares.items():
                combined[lote_id] += child_share * input_weight
            event_ids.update(child.event_ids)
            issues.extend(child.issues)

        normalized_shares = {
            lote_id: self._q(share)
            for lote_id, share in combined.items()
            if share > ZERO
        }
        profile = _Profile(
            shares=normalized_shares,
            event_ids=frozenset(event_ids),
            issues=tuple(issues),
        )
        self._profile_cache[batch.id] = profile
        return profile

    def _serialize_batch(self, batch: TraceabilityBatch) -> dict[str, Any]:
        return {
            "id": batch.id,
            "public_id": str(batch.public_id),
            "code": batch.code,
            "product_name": batch.product_name,
            "stage": batch.stage,
            "unit": batch.unit,
            "status": batch.status,
            "source_lote_id": batch.source_lote_id,
        }

    def _serialize_lote(self, lote: Lote) -> dict[str, Any]:
        return {
            "id": lote.id,
            "identificador": lote.identificador,
            "productor_id": lote.productor_id,
            "producto_forestal": lote.producto_forestal,
            "hectareas": lote.hectareas,
            "latitud": lote.latitud,
            "longitud": lote.longitud,
            "polygon_wkt": lote.polygon_wkt,
            "estatus": lote.estatus,
        }

    def _serialize_event(self, event_id: int) -> dict[str, Any] | None:
        event = self._get_event(event_id)
        if event is None:
            return None
        inputs = self._get_event_inputs(event.id)
        outputs = self._get_event_outputs(event.id)

        input_units = {edge.unit for edge in inputs}
        output_units = {edge.unit for edge in outputs}
        homogeneous_unit = None
        if len(input_units | output_units) == 1:
            homogeneous_unit = next(iter(input_units | output_units), None)

        total_input = sum((self._q(edge.quantity) for edge in inputs), ZERO)
        total_output = sum((self._q(edge.quantity) for edge in outputs), ZERO)
        loss = None
        yield_ratio = None
        if homogeneous_unit is not None and inputs:
            loss = self._q(total_input - total_output)
            if total_input > ZERO:
                yield_ratio = self._q(total_output / total_input)

        def edge_payload(edge: TraceabilityEventInput | TraceabilityEventOutput) -> dict[str, Any]:
            batch = self._get_batch(edge.batch_id)
            return {
                "batch_id": edge.batch_id,
                "batch_code": batch.code if batch is not None else None,
                "quantity": self._decimal_str(edge.quantity),
                "unit": edge.unit,
            }

        return {
            "id": event.id,
            "public_id": str(event.public_id),
            "event_code": event.event_code,
            "event_type": event.event_type,
            "status": event.status,
            "occurred_at": event.occurred_at.isoformat(),
            "facility_reference": event.facility_reference,
            "inputs": [edge_payload(edge) for edge in inputs],
            "outputs": [edge_payload(edge) for edge in outputs],
            "reconciliation": {
                "unit": homogeneous_unit,
                "input_quantity": self._decimal_str(total_input),
                "output_quantity": self._decimal_str(total_output),
                "loss_quantity": self._decimal_str(loss) if loss is not None else None,
                "yield_ratio": self._decimal_str(yield_ratio) if yield_ratio is not None else None,
            },
        }

    def trace_shipment(self, shipment_code: str) -> dict[str, Any]:
        normalized_code = (shipment_code or "").strip()
        if not normalized_code:
            raise TraceabilityLineageValidationError(
                "SHIPMENT_CODE_REQUIRED",
                "shipment_code es obligatorio.",
            )

        shipment = self.session.execute(
            select(Shipment).where(
                Shipment.organization_id == self.organization_id,
                func.lower(Shipment.shipment_code) == normalized_code.lower(),
            )
        ).scalar_one_or_none()
        if shipment is None:
            raise TraceabilityLineageNotFoundError(
                "SHIPMENT_NOT_FOUND",
                "El despacho solicitado no existe para la organización autenticada.",
            )

        items = list(
            self.session.execute(
                select(ShipmentItem)
                .where(
                    ShipmentItem.organization_id == self.organization_id,
                    ShipmentItem.shipment_id == shipment.id,
                )
                .order_by(ShipmentItem.id)
            ).scalars().all()
        )

        top_issues: list[dict[str, Any]] = []
        if not items:
            top_issues.append(
                self._issue(
                    "SHIPMENT_WITHOUT_ITEMS",
                    "El despacho no contiene lotes industriales asignados.",
                )
            )

        event_ids: set[int] = set()
        source_totals: dict[tuple[int, str], Decimal] = defaultdict(lambda: ZERO)
        unit_shipped: dict[str, Decimal] = defaultdict(lambda: ZERO)
        unit_attributed: dict[str, Decimal] = defaultdict(lambda: ZERO)
        item_payloads: list[dict[str, Any]] = []

        for item in items:
            quantity = self._q(item.quantity)
            unit_shipped[item.unit] += quantity
            batch = self._get_batch(item.batch_id)
            item_issues: list[dict[str, Any]] = []
            source_payloads: list[dict[str, Any]] = []

            if batch is None:
                item_issues.append(
                    self._issue(
                        "SHIPMENT_BATCH_NOT_FOUND",
                        "Un lote del despacho no puede resolverse para el tenant.",
                        batch_id=item.batch_id,
                    )
                )
                item_payloads.append(
                    {
                        "shipment_item_id": item.id,
                        "batch": None,
                        "shipped_quantity": self._decimal_str(quantity),
                        "unit": item.unit,
                        "complete": False,
                        "issues": item_issues,
                        "source_contributions": [],
                    }
                )
                continue

            if item.unit != batch.unit:
                item_issues.append(
                    self._issue(
                        "SHIPMENT_UNIT_MISMATCH",
                        "La unidad despachada no coincide con la unidad del lote industrial.",
                        batch_id=batch.id,
                    )
                )

            profile = self._profile_batch(batch.id)
            event_ids.update(profile.event_ids)
            item_issues.extend(profile.issues)

            attributed = ZERO
            for lote_id, share in sorted(profile.shares.items()):
                lote = self._get_lote(lote_id)
                if lote is None:
                    item_issues.append(
                        self._issue(
                            "SOURCE_LOTE_NOT_FOUND",
                            "Un origen atribuido no puede resolverse para el tenant.",
                            batch_id=batch.id,
                        )
                    )
                    continue
                allocated = self._q(quantity * share)
                if allocated <= ZERO:
                    continue
                attributed += allocated
                source_totals[(lote.id, item.unit)] += allocated
                source_payloads.append(
                    {
                        "lote": self._serialize_lote(lote),
                        "attributed_shipment_quantity": self._decimal_str(allocated),
                        "unit": item.unit,
                        "share_of_shipment_item": self._decimal_str(
                            allocated / quantity if quantity > ZERO else ZERO
                        ),
                    }
                )

            attributed = self._q(attributed)
            unit_attributed[item.unit] += attributed
            unresolved = self._q(max(quantity - attributed, ZERO))
            complete = not item_issues and unresolved == ZERO

            item_payloads.append(
                {
                    "shipment_item_id": item.id,
                    "batch": self._serialize_batch(batch),
                    "shipped_quantity": self._decimal_str(quantity),
                    "unit": item.unit,
                    "attributed_quantity": self._decimal_str(attributed),
                    "unresolved_quantity": self._decimal_str(unresolved),
                    "complete": complete,
                    "issues": item_issues,
                    "source_contributions": source_payloads,
                }
            )

        unit_totals: list[dict[str, Any]] = []
        for unit in sorted(unit_shipped):
            shipped = self._q(unit_shipped[unit])
            attributed = self._q(unit_attributed[unit])
            unit_totals.append(
                {
                    "unit": unit,
                    "shipped_quantity": self._decimal_str(shipped),
                    "attributed_quantity": self._decimal_str(attributed),
                    "unresolved_quantity": self._decimal_str(max(shipped - attributed, ZERO)),
                }
            )

        source_lotes: list[dict[str, Any]] = []
        for (lote_id, unit), quantity in sorted(source_totals.items()):
            lote = self._get_lote(lote_id)
            if lote is None:
                continue
            shipped_in_unit = self._q(unit_shipped[unit])
            source_lotes.append(
                {
                    "lote": self._serialize_lote(lote),
                    "attributed_shipment_quantity": self._decimal_str(quantity),
                    "unit": unit,
                    "share_of_shipped_unit": self._decimal_str(
                        self._q(quantity) / shipped_in_unit
                        if shipped_in_unit > ZERO
                        else ZERO
                    ),
                }
            )

        serialized_events = [
            payload
            for event_id in sorted(event_ids)
            if (payload := self._serialize_event(event_id)) is not None
        ]

        all_item_issues = [
            issue
            for item_payload in item_payloads
            for issue in item_payload["issues"]
        ]
        complete = (
            bool(items)
            and not top_issues
            and not all_item_issues
            and all(item_payload["complete"] for item_payload in item_payloads)
        )

        return {
            "organization_id": self.organization_id,
            "shipment": {
                "id": shipment.id,
                "public_id": str(shipment.public_id),
                "shipment_code": shipment.shipment_code,
                "sale_reference": shipment.sale_reference,
                "buyer_reference": shipment.buyer_reference,
                "destination_country": shipment.destination_country,
                "shipped_at": shipment.shipped_at.isoformat() if shipment.shipped_at else None,
                "status": shipment.status,
                "lineage_state": "FINAL" if shipment.status == "DISPATCHED" else "PREVIEW",
            },
            "allocation_method": ALLOCATION_METHOD,
            "complete": complete,
            "issues": [*top_issues, *all_item_issues],
            "unit_totals": unit_totals,
            "items": item_payloads,
            "events": serialized_events,
            "source_lotes": source_lotes,
        }
