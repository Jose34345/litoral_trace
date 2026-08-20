"""Transactional industrial ledger for P1B chain-of-custody enforcement.

The ledger is intentionally derived from immutable business movements instead
of persisting a mutable ``current_stock`` field. A batch balance is:

    posted outputs - posted inputs - dispatched shipment items

Posting and dispatch operations lock all affected material batches in stable
ID order before validating availability. On PostgreSQL this serializes
competing consumers of the same stock and prevents double-spend.

P1B uses a homogeneous physical accounting basis per event. For the initial
Corrientes forestry workflow this is normally M3 (solid wood volume) through
roundwood -> sawn/intermediate -> finished product. Cross-unit transformation
requires a later explicit, evidence-backed conversion profile and therefore
fails closed here instead of applying hidden density/yield assumptions.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import (
    Shipment,
    ShipmentItem,
    TraceabilityBatch,
    TraceabilityEvent,
    TraceabilityEventInput,
    TraceabilityEventOutput,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import (
    AuditAction,
    AuditActor,
    AuditOutcome,
    AuditRequestContext,
    record_audit_event,
)


QTY_QUANTUM = Decimal("0.000001")
ZERO = Decimal("0.000000")
LEDGER_EVENT_TYPES = frozenset(
    {"RECEIPT", "TRANSFORMATION", "MIX", "SPLIT", "REPACK", "ADJUSTMENT"}
)


class TraceabilityLedgerError(RuntimeError):
    """Base class for safe P1B ledger failures."""


class TraceabilityAuthorizationError(TraceabilityLedgerError):
    """Authenticated actor does not belong to the requested tenant."""


class TraceabilityNotFoundError(TraceabilityLedgerError):
    """Requested tenant-scoped traceability entity does not exist."""


class TraceabilityStateError(TraceabilityLedgerError):
    """Requested transition is invalid for the current entity state."""


class TraceabilityValidationError(TraceabilityLedgerError):
    """Stable domain validation failure suitable for API translation."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail)


class TraceabilityPersistenceError(TraceabilityLedgerError):
    """Sanitized persistence failure that never exposes raw DB details."""


@dataclass(frozen=True)
class BatchInventoryBalance:
    organization_id: int
    batch_id: int
    batch_public_id: UUID
    batch_code: str
    unit: str
    produced: Decimal
    consumed: Decimal
    dispatched: Decimal
    available: Decimal


@dataclass(frozen=True)
class UnitBalanceSummary:
    unit: str
    input_quantity: Decimal
    output_quantity: Decimal
    loss_quantity: Decimal
    yield_ratio: Decimal | None


@dataclass(frozen=True)
class EventPostingResult:
    organization_id: int
    event_id: int
    event_public_id: UUID
    event_code: str
    event_type: str
    status: str
    unit_balances: tuple[UnitBalanceSummary, ...]


@dataclass(frozen=True)
class ShipmentDispatchResult:
    organization_id: int
    shipment_id: int
    shipment_public_id: UUID
    shipment_code: str
    status: str
    shipped_at: datetime
    quantities_by_unit: tuple[tuple[str, Decimal], ...]


SessionFactory = Callable[[], Session | None]


def _qty(value: Any) -> Decimal:
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise TraceabilityValidationError(
            "INVALID_QUANTITY",
            "La cantidad de trazabilidad no es válida.",
        ) from exc
    return decimal_value.quantize(QTY_QUANTUM, rounding=ROUND_HALF_UP)


def _normalize_organization_id(value: int | str) -> int:
    try:
        organization_id = int(value)
    except (TypeError, ValueError) as exc:
        raise TraceabilityAuthorizationError(
            "El tenant de trazabilidad no es válido."
        ) from exc
    if organization_id <= 0:
        raise TraceabilityAuthorizationError(
            "El tenant de trazabilidad no es válido."
        )
    return organization_id


def _as_utc(value: datetime) -> datetime:
    """Normalize SQLite-naive and PostgreSQL-aware timestamps for comparison."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _sum_scalar(session: Session, statement) -> Decimal:
    value = session.execute(statement).scalar_one()
    return _qty(value or ZERO)


def _line_totals(lines: Iterable[Any]) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = defaultdict(lambda: ZERO)
    for line in lines:
        totals[str(line.unit)] = _qty(totals[str(line.unit)] + _qty(line.quantity))
    return dict(totals)


def _unit_summaries(
    inputs: Iterable[TraceabilityEventInput],
    outputs: Iterable[TraceabilityEventOutput],
) -> tuple[UnitBalanceSummary, ...]:
    input_totals = _line_totals(inputs)
    output_totals = _line_totals(outputs)
    units = sorted(set(input_totals) | set(output_totals))
    summaries: list[UnitBalanceSummary] = []
    for unit in units:
        input_quantity = input_totals.get(unit, ZERO)
        output_quantity = output_totals.get(unit, ZERO)
        loss_quantity = _qty(max(input_quantity - output_quantity, ZERO))
        yield_ratio = None
        if input_quantity > ZERO:
            yield_ratio = (
                output_quantity / input_quantity
            ).quantize(QTY_QUANTUM, rounding=ROUND_HALF_UP)
        summaries.append(
            UnitBalanceSummary(
                unit=unit,
                input_quantity=input_quantity,
                output_quantity=output_quantity,
                loss_quantity=loss_quantity,
                yield_ratio=yield_ratio,
            )
        )
    return tuple(summaries)


class TraceabilityLedgerService:
    """Post industrial movements atomically while enforcing provenance stock."""

    def __init__(self, *, session_factory: SessionFactory | None = None) -> None:
        self._session_factory = session_factory or get_db_session

    @staticmethod
    def _validate_actor_scope(actor: AuditActor, organization_id: int) -> None:
        if int(actor.organization_id) != organization_id:
            raise TraceabilityAuthorizationError(
                "El actor autenticado no pertenece al tenant de trazabilidad."
            )

    @staticmethod
    def _lock_batches(
        session: Session,
        *,
        organization_id: int,
        batch_ids: Iterable[int],
    ) -> dict[int, TraceabilityBatch]:
        normalized_ids = sorted({int(batch_id) for batch_id in batch_ids})
        if not normalized_ids:
            return {}

        statement = (
            select(TraceabilityBatch)
            .where(
                TraceabilityBatch.organization_id == organization_id,
                TraceabilityBatch.id.in_(normalized_ids),
            )
            .order_by(TraceabilityBatch.id)
            .with_for_update()
        )
        batches = session.execute(statement).scalars().all()
        by_id = {int(batch.id): batch for batch in batches}
        if len(by_id) != len(normalized_ids):
            raise TraceabilityNotFoundError(
                "Uno o más lotes industriales no existen en el tenant activo."
            )
        return by_id

    @staticmethod
    def _latest_producer_time(
        session: Session,
        *,
        organization_id: int,
        batch_id: int,
    ) -> datetime | None:
        return session.execute(
            select(func.max(TraceabilityEvent.occurred_at))
            .join(
                TraceabilityEventOutput,
                TraceabilityEventOutput.event_id == TraceabilityEvent.id,
            )
            .where(
                TraceabilityEvent.organization_id == organization_id,
                TraceabilityEvent.status == "POSTED",
                TraceabilityEventOutput.organization_id == organization_id,
                TraceabilityEventOutput.batch_id == batch_id,
            )
        ).scalar_one()

    @staticmethod
    def _posted_producer_count(
        session: Session,
        *,
        organization_id: int,
        batch_id: int,
    ) -> int:
        return int(
            session.execute(
                select(func.count(TraceabilityEventOutput.id))
                .join(
                    TraceabilityEvent,
                    TraceabilityEvent.id == TraceabilityEventOutput.event_id,
                )
                .where(
                    TraceabilityEvent.organization_id == organization_id,
                    TraceabilityEvent.status == "POSTED",
                    TraceabilityEventOutput.organization_id == organization_id,
                    TraceabilityEventOutput.batch_id == batch_id,
                )
            ).scalar_one()
        )

    @staticmethod
    def _balance_for_batch(
        session: Session,
        *,
        organization_id: int,
        batch: TraceabilityBatch,
    ) -> BatchInventoryBalance:
        produced = _sum_scalar(
            session,
            select(func.coalesce(func.sum(TraceabilityEventOutput.quantity), 0))
            .join(
                TraceabilityEvent,
                TraceabilityEvent.id == TraceabilityEventOutput.event_id,
            )
            .where(
                TraceabilityEventOutput.organization_id == organization_id,
                TraceabilityEventOutput.batch_id == batch.id,
                TraceabilityEvent.status == "POSTED",
                TraceabilityEvent.organization_id == organization_id,
            ),
        )
        consumed = _sum_scalar(
            session,
            select(func.coalesce(func.sum(TraceabilityEventInput.quantity), 0))
            .join(
                TraceabilityEvent,
                TraceabilityEvent.id == TraceabilityEventInput.event_id,
            )
            .where(
                TraceabilityEventInput.organization_id == organization_id,
                TraceabilityEventInput.batch_id == batch.id,
                TraceabilityEvent.status == "POSTED",
                TraceabilityEvent.organization_id == organization_id,
            ),
        )
        dispatched = _sum_scalar(
            session,
            select(func.coalesce(func.sum(ShipmentItem.quantity), 0))
            .join(Shipment, Shipment.id == ShipmentItem.shipment_id)
            .where(
                ShipmentItem.organization_id == organization_id,
                ShipmentItem.batch_id == batch.id,
                Shipment.organization_id == organization_id,
                Shipment.status == "DISPATCHED",
            ),
        )
        available = _qty(produced - consumed - dispatched)
        return BatchInventoryBalance(
            organization_id=organization_id,
            batch_id=int(batch.id),
            batch_public_id=batch.public_id,
            batch_code=batch.code,
            unit=batch.unit,
            produced=produced,
            consumed=consumed,
            dispatched=dispatched,
            available=available,
        )

    def get_batch_balance(
        self,
        *,
        organization_id: int | str,
        batch_id: int,
    ) -> BatchInventoryBalance:
        org_id = _normalize_organization_id(organization_id)
        session = self._session_factory()
        if session is None:
            raise TraceabilityPersistenceError(
                "Servicio de base de datos no disponible."
            )
        try:
            set_tenant_db_context(session, org_id)
            batch = session.execute(
                select(TraceabilityBatch).where(
                    TraceabilityBatch.organization_id == org_id,
                    TraceabilityBatch.id == int(batch_id),
                )
            ).scalar_one_or_none()
            if batch is None:
                raise TraceabilityNotFoundError(
                    "El lote industrial no existe en el tenant activo."
                )
            return self._balance_for_batch(
                session,
                organization_id=org_id,
                batch=batch,
            )
        except TraceabilityLedgerError:
            session.rollback()
            raise
        except SQLAlchemyError as exc:
            session.rollback()
            raise TraceabilityPersistenceError(
                "No fue posible consultar el saldo del lote industrial."
            ) from exc
        finally:
            session.close()

    @staticmethod
    def _validate_lines_against_batches(
        *,
        batches: dict[int, TraceabilityBatch],
        inputs: list[TraceabilityEventInput],
        outputs: list[TraceabilityEventOutput],
    ) -> None:
        for line in [*inputs, *outputs]:
            batch = batches[int(line.batch_id)]
            if batch.status != "ACTIVE":
                raise TraceabilityValidationError(
                    "BATCH_NOT_ACTIVE",
                    f"El lote industrial {batch.code} no está activo.",
                )
            if str(line.unit) != str(batch.unit):
                raise TraceabilityValidationError(
                    "BATCH_UNIT_MISMATCH",
                    (
                        f"La línea del lote {batch.code} usa {line.unit}, "
                        f"pero el lote está contabilizado en {batch.unit}."
                    ),
                )
            if _qty(line.quantity) <= ZERO:
                raise TraceabilityValidationError(
                    "NON_POSITIVE_QUANTITY",
                    "Todas las cantidades del evento deben ser mayores que cero.",
                )

    @staticmethod
    def _validate_event_shape(
        event: TraceabilityEvent,
        inputs: list[TraceabilityEventInput],
        outputs: list[TraceabilityEventOutput],
        batches: dict[int, TraceabilityBatch],
    ) -> None:
        event_type = str(event.event_type)
        if event_type not in LEDGER_EVENT_TYPES:
            raise TraceabilityValidationError(
                "UNSUPPORTED_EVENT_TYPE",
                "El tipo de evento industrial no está soportado por el ledger.",
            )

        input_ids = {int(line.batch_id) for line in inputs}
        output_ids = {int(line.batch_id) for line in outputs}
        if input_ids & output_ids:
            raise TraceabilityValidationError(
                "SELF_REFERENTIAL_EVENT",
                "Un mismo lote no puede ser entrada y salida del mismo evento.",
            )

        if event_type == "RECEIPT":
            if inputs or not outputs:
                raise TraceabilityValidationError(
                    "INVALID_RECEIPT_SHAPE",
                    "Una recepción debe crear al menos una salida y no consumir lotes.",
                )
            for line in outputs:
                batch = batches[int(line.batch_id)]
                if batch.stage not in {"RECEIPT", "RAW_MATERIAL"}:
                    raise TraceabilityValidationError(
                        "INVALID_RECEIPT_STAGE",
                        "Una recepción sólo puede originar lotes de recepción o materia prima.",
                    )
                if batch.source_lote_id is None:
                    raise TraceabilityValidationError(
                        "RECEIPT_WITHOUT_SOURCE_PLOT",
                        (
                            "La materia prima recibida debe estar vinculada a un "
                            "lote/parcela de origen antes de contabilizar stock."
                        ),
                    )
            return

        if event_type == "ADJUSTMENT":
            if not inputs or outputs:
                raise TraceabilityValidationError(
                    "INVALID_ADJUSTMENT_SHAPE",
                    (
                        "P1B sólo admite ajustes negativos documentados: deben "
                        "consumir stock y no crear material nuevo."
                    ),
                )
            return

        if not inputs or not outputs:
            raise TraceabilityValidationError(
                "INVALID_TRANSFORMATION_SHAPE",
                "La operación industrial debe tener entradas y salidas.",
            )

        input_totals = _line_totals(inputs)
        output_totals = _line_totals(outputs)
        if set(output_totals) - set(input_totals):
            raise TraceabilityValidationError(
                "UNIT_CONVERSION_REQUIRED",
                (
                    "La transformación intenta producir una unidad física que no "
                    "existe en las entradas. Debe definirse una conversión "
                    "documentada antes de automatizar ese proceso."
                ),
            )
        for unit, output_quantity in output_totals.items():
            if output_quantity > input_totals.get(unit, ZERO):
                raise TraceabilityValidationError(
                    "OUTPUT_EXCEEDS_INPUT",
                    (
                        f"La salida {output_quantity} {unit} supera la entrada "
                        f"{input_totals.get(unit, ZERO)} {unit}."
                    ),
                )

    def post_event(
        self,
        *,
        organization_id: int | str,
        event_id: int,
        actor: AuditActor,
        request_context: AuditRequestContext | None = None,
    ) -> EventPostingResult:
        org_id = _normalize_organization_id(organization_id)
        self._validate_actor_scope(actor, org_id)
        session = self._session_factory()
        if session is None:
            raise TraceabilityPersistenceError(
                "Servicio de base de datos no disponible."
            )

        try:
            set_tenant_db_context(session, org_id)
            event = session.execute(
                select(TraceabilityEvent)
                .where(
                    TraceabilityEvent.organization_id == org_id,
                    TraceabilityEvent.id == int(event_id),
                )
                .with_for_update()
            ).scalar_one_or_none()
            if event is None:
                raise TraceabilityNotFoundError(
                    "El evento industrial no existe en el tenant activo."
                )
            if event.status != "DRAFT":
                raise TraceabilityStateError(
                    "Sólo los eventos DRAFT pueden contabilizarse."
                )

            inputs = list(
                session.execute(
                    select(TraceabilityEventInput)
                    .where(
                        TraceabilityEventInput.organization_id == org_id,
                        TraceabilityEventInput.event_id == event.id,
                    )
                    .order_by(TraceabilityEventInput.id)
                ).scalars().all()
            )
            outputs = list(
                session.execute(
                    select(TraceabilityEventOutput)
                    .where(
                        TraceabilityEventOutput.organization_id == org_id,
                        TraceabilityEventOutput.event_id == event.id,
                    )
                    .order_by(TraceabilityEventOutput.id)
                ).scalars().all()
            )

            batches = self._lock_batches(
                session,
                organization_id=org_id,
                batch_ids=[line.batch_id for line in [*inputs, *outputs]],
            )
            self._validate_lines_against_batches(
                batches=batches,
                inputs=inputs,
                outputs=outputs,
            )
            self._validate_event_shape(event, inputs, outputs, batches)

            for line in inputs:
                batch = batches[int(line.batch_id)]
                balance = self._balance_for_batch(
                    session,
                    organization_id=org_id,
                    batch=batch,
                )
                requested = _qty(line.quantity)
                if requested > balance.available:
                    raise TraceabilityValidationError(
                        "INSUFFICIENT_BATCH_STOCK",
                        (
                            f"El lote {batch.code} dispone de {balance.available} "
                            f"{batch.unit} y se intentan consumir {requested} {batch.unit}."
                        ),
                    )
                produced_at = self._latest_producer_time(
                    session,
                    organization_id=org_id,
                    batch_id=batch.id,
                )
                if (
                    produced_at is not None
                    and _as_utc(event.occurred_at) < _as_utc(produced_at)
                ):
                    raise TraceabilityValidationError(
                        "EVENT_BEFORE_INPUT_PRODUCTION",
                        "El evento no puede ocurrir antes de la producción de una de sus entradas.",
                    )

            for line in outputs:
                if self._posted_producer_count(
                    session,
                    organization_id=org_id,
                    batch_id=int(line.batch_id),
                ):
                    raise TraceabilityValidationError(
                        "BATCH_ALREADY_HAS_PRODUCER",
                        (
                            f"El lote {batches[int(line.batch_id)].code} ya tiene "
                            "un evento productor contabilizado."
                        ),
                    )

            summaries = _unit_summaries(inputs, outputs)
            event.status = "POSTED"
            session.flush()

            record_audit_event(
                session,
                actor=actor,
                action=AuditAction.TRACEABILITY_EVENT_POST,
                entity_type="traceability_event",
                entity_id=event.id,
                outcome=AuditOutcome.SUCCESS,
                request_context=request_context,
                metadata={
                    "event_code": event.event_code,
                    "event_type": event.event_type,
                    "facility_reference": event.facility_reference,
                    "unit_balances": [
                        {
                            "unit": item.unit,
                            "input_quantity": str(item.input_quantity),
                            "output_quantity": str(item.output_quantity),
                            "loss_quantity": str(item.loss_quantity),
                            "yield_ratio": (
                                str(item.yield_ratio)
                                if item.yield_ratio is not None
                                else None
                            ),
                        }
                        for item in summaries
                    ],
                },
                before_data={"status": "DRAFT"},
                after_data={"status": "POSTED"},
            )

            result = EventPostingResult(
                organization_id=org_id,
                event_id=int(event.id),
                event_public_id=event.public_id,
                event_code=event.event_code,
                event_type=event.event_type,
                status="POSTED",
                unit_balances=summaries,
            )
            session.commit()
            return result
        except TraceabilityLedgerError:
            session.rollback()
            raise
        except SQLAlchemyError as exc:
            session.rollback()
            raise TraceabilityPersistenceError(
                "No fue posible contabilizar el evento industrial."
            ) from exc
        except Exception as exc:
            session.rollback()
            raise TraceabilityPersistenceError(
                "No fue posible completar la contabilización industrial."
            ) from exc
        finally:
            session.close()

    def dispatch_shipment(
        self,
        *,
        organization_id: int | str,
        shipment_id: int,
        actor: AuditActor,
        request_context: AuditRequestContext | None = None,
    ) -> ShipmentDispatchResult:
        org_id = _normalize_organization_id(organization_id)
        self._validate_actor_scope(actor, org_id)
        session = self._session_factory()
        if session is None:
            raise TraceabilityPersistenceError(
                "Servicio de base de datos no disponible."
            )

        try:
            set_tenant_db_context(session, org_id)
            shipment = session.execute(
                select(Shipment)
                .where(
                    Shipment.organization_id == org_id,
                    Shipment.id == int(shipment_id),
                )
                .with_for_update()
            ).scalar_one_or_none()
            if shipment is None:
                raise TraceabilityNotFoundError(
                    "El despacho no existe en el tenant activo."
                )
            if shipment.status not in {"DRAFT", "CONFIRMED"}:
                raise TraceabilityStateError(
                    "Sólo los despachos DRAFT o CONFIRMED pueden despacharse."
                )

            items = list(
                session.execute(
                    select(ShipmentItem)
                    .where(
                        ShipmentItem.organization_id == org_id,
                        ShipmentItem.shipment_id == shipment.id,
                    )
                    .order_by(ShipmentItem.id)
                ).scalars().all()
            )
            if not items:
                raise TraceabilityValidationError(
                    "EMPTY_SHIPMENT",
                    "El despacho debe contener al menos un lote industrial.",
                )

            batches = self._lock_batches(
                session,
                organization_id=org_id,
                batch_ids=[item.batch_id for item in items],
            )
            dispatch_time = _as_utc(
                shipment.shipped_at or datetime.now(timezone.utc)
            )
            quantities_by_unit = _line_totals(items)

            for item in items:
                batch = batches[int(item.batch_id)]
                if batch.status != "ACTIVE":
                    raise TraceabilityValidationError(
                        "BATCH_NOT_ACTIVE",
                        f"El lote industrial {batch.code} no está activo.",
                    )
                if str(item.unit) != str(batch.unit):
                    raise TraceabilityValidationError(
                        "BATCH_UNIT_MISMATCH",
                        (
                            f"El despacho usa {item.unit} para {batch.code}, "
                            f"pero el lote está contabilizado en {batch.unit}."
                        ),
                    )
                requested = _qty(item.quantity)
                if requested <= ZERO:
                    raise TraceabilityValidationError(
                        "NON_POSITIVE_QUANTITY",
                        "Las cantidades despachadas deben ser mayores que cero.",
                    )
                balance = self._balance_for_batch(
                    session,
                    organization_id=org_id,
                    batch=batch,
                )
                if requested > balance.available:
                    raise TraceabilityValidationError(
                        "INSUFFICIENT_BATCH_STOCK",
                        (
                            f"El lote {batch.code} dispone de {balance.available} "
                            f"{batch.unit} y se intentan despachar {requested} {batch.unit}."
                        ),
                    )
                produced_at = self._latest_producer_time(
                    session,
                    organization_id=org_id,
                    batch_id=batch.id,
                )
                if produced_at is None:
                    raise TraceabilityValidationError(
                        "SHIPMENT_WITHOUT_POSTED_ORIGIN",
                        "No se puede despachar material sin un evento productor contabilizado.",
                    )
                if dispatch_time < _as_utc(produced_at):
                    raise TraceabilityValidationError(
                        "SHIPMENT_BEFORE_PRODUCTION",
                        "El despacho no puede ocurrir antes de la producción del material.",
                    )

            previous_status = shipment.status
            shipment.status = "DISPATCHED"
            shipment.shipped_at = dispatch_time
            session.flush()

            record_audit_event(
                session,
                actor=actor,
                action=AuditAction.TRACEABILITY_SHIPMENT_DISPATCH,
                entity_type="shipment",
                entity_id=shipment.id,
                outcome=AuditOutcome.SUCCESS,
                request_context=request_context,
                metadata={
                    "shipment_code": shipment.shipment_code,
                    "sale_reference": shipment.sale_reference,
                    "buyer_reference": shipment.buyer_reference,
                    "destination_country": shipment.destination_country,
                    "quantities_by_unit": {
                        unit: str(quantity)
                        for unit, quantity in quantities_by_unit.items()
                    },
                },
                before_data={"status": previous_status},
                after_data={
                    "status": "DISPATCHED",
                    "shipped_at": dispatch_time.isoformat(),
                },
            )

            result = ShipmentDispatchResult(
                organization_id=org_id,
                shipment_id=int(shipment.id),
                shipment_public_id=shipment.public_id,
                shipment_code=shipment.shipment_code,
                status="DISPATCHED",
                shipped_at=dispatch_time,
                quantities_by_unit=tuple(sorted(quantities_by_unit.items())),
            )
            session.commit()
            return result
        except TraceabilityLedgerError:
            session.rollback()
            raise
        except SQLAlchemyError as exc:
            session.rollback()
            raise TraceabilityPersistenceError(
                "No fue posible contabilizar el despacho."
            ) from exc
        except Exception as exc:
            session.rollback()
            raise TraceabilityPersistenceError(
                "No fue posible completar el despacho."
            ) from exc
        finally:
            session.close()
