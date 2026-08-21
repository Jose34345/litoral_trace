"""Operational write layer for UX10-D chain-of-custody workflows.

This module creates tenant-scoped DRAFT objects only. Irreversible accounting is
still delegated to P1B ``TraceabilityLedgerService.post_event`` and
``dispatch_shipment`` so browser workflows cannot bypass stock, unit, temporal,
or concurrency controls.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models.lote import Lote
from litoral_trace.db.models.traceability import (
    Shipment,
    ShipmentItem,
    TraceabilityBatch,
    TraceabilityEvent,
    TraceabilityEventInput,
    TraceabilityEventOutput,
    TRACEABILITY_UNITS,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import AuditActor
from litoral_trace.services.traceability_ledger import (
    TraceabilityAuthorizationError as LedgerAuthorizationError,
    TraceabilityLedgerError,
    TraceabilityLedgerService,
    TraceabilityNotFoundError as LedgerNotFoundError,
    TraceabilityPersistenceError as LedgerPersistenceError,
    TraceabilityStateError as LedgerStateError,
    TraceabilityValidationError as LedgerValidationError,
)


QTY_QUANTUM = Decimal("0.000001")
ZERO = Decimal("0.000000")
PROCESS_EVENT_TYPES = frozenset({"TRANSFORMATION", "MIX", "SPLIT", "REPACK"})
OUTPUT_STAGES = frozenset({"INTERMEDIATE", "FINISHED_GOOD"})


class TraceabilityOperationError(RuntimeError):
    """Base safe error for the UX10-D operational write layer."""


class TraceabilityOperationAuthorizationError(TraceabilityOperationError):
    pass


class TraceabilityOperationNotFoundError(TraceabilityOperationError):
    pass


class TraceabilityOperationConflictError(TraceabilityOperationError):
    pass


class TraceabilityOperationPersistenceError(TraceabilityOperationError):
    pass


class TraceabilityOperationValidationError(TraceabilityOperationError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail)


@dataclass(frozen=True)
class ProcessInputDraft:
    batch_public_id: UUID
    quantity: Decimal


@dataclass(frozen=True)
class ProcessOutputDraft:
    code: str
    product_name: str
    stage: str
    unit: str
    quantity: Decimal


@dataclass(frozen=True)
class ShipmentItemDraft:
    batch_public_id: UUID
    quantity: Decimal


@dataclass(frozen=True)
class DraftEventResult:
    event_id: int
    event_public_id: UUID
    event_code: str
    event_type: str
    status: str
    output_batch_public_ids: tuple[UUID, ...]


@dataclass(frozen=True)
class DraftShipmentResult:
    shipment_id: int
    shipment_public_id: UUID
    shipment_code: str
    status: str


@dataclass(frozen=True)
class SourceLoteChoice:
    identifier: str
    producer: str
    product: str
    status: str


@dataclass(frozen=True)
class ActiveBatchChoice:
    public_id: UUID
    code: str
    product_name: str
    stage: str
    unit: str
    available: Decimal


@dataclass(frozen=True)
class DraftEventChoice:
    public_id: UUID
    code: str
    event_type: str
    occurred_at: datetime
    facility_reference: str | None
    input_count: int
    output_count: int


@dataclass(frozen=True)
class DraftShipmentChoice:
    public_id: UUID
    code: str
    sale_reference: str | None
    buyer_reference: str | None
    destination_country: str | None
    item_count: int


@dataclass(frozen=True)
class OperationsSnapshot:
    source_lotes: tuple[SourceLoteChoice, ...]
    active_batches: tuple[ActiveBatchChoice, ...]
    draft_events: tuple[DraftEventChoice, ...]
    draft_shipments: tuple[DraftShipmentChoice, ...]


def _qty(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise TraceabilityOperationValidationError(
            "INVALID_QUANTITY",
            "La cantidad ingresada no es válida.",
        ) from exc
    result = result.quantize(QTY_QUANTUM, rounding=ROUND_HALF_UP)
    if result <= ZERO:
        raise TraceabilityOperationValidationError(
            "NON_POSITIVE_QUANTITY",
            "Las cantidades deben ser mayores que cero.",
        )
    return result


def _text(value: Any, *, field: str, maximum: int, required: bool = True) -> str | None:
    normalized = str(value or "").strip()
    if required and not normalized:
        raise TraceabilityOperationValidationError(
            f"MISSING_{field.upper()}",
            f"El campo {field} es obligatorio.",
        )
    if len(normalized) > maximum:
        raise TraceabilityOperationValidationError(
            f"{field.upper()}_TOO_LONG",
            f"El campo {field} supera el máximo de {maximum} caracteres.",
        )
    return normalized or None


def _unit(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in TRACEABILITY_UNITS:
        raise TraceabilityOperationValidationError(
            "INVALID_UNIT",
            "La unidad debe ser M3, KG o TON.",
        )
    return normalized


def _stage(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in OUTPUT_STAGES:
        raise TraceabilityOperationValidationError(
            "INVALID_OUTPUT_STAGE",
            "Las salidas industriales deben ser INTERMEDIATE o FINISHED_GOOD.",
        )
    return normalized


def _actor_scope(actor: AuditActor, organization_id: int) -> None:
    if int(actor.organization_id) != int(organization_id):
        raise TraceabilityOperationAuthorizationError(
            "El actor autenticado no pertenece al tenant activo."
        )


class TraceabilityOperationService:
    """Create recoverable drafts and delegate irreversible transitions to P1B."""

    def __init__(self, *, session_factory=None) -> None:
        self._session_factory = session_factory or get_db_session

    def _session(self, organization_id: int) -> Session:
        session = self._session_factory()
        if session is None:
            raise TraceabilityOperationPersistenceError(
                "Servicio de base de datos no disponible."
            )
        set_tenant_db_context(session, int(organization_id))
        return session

    @staticmethod
    def _ensure_batch_codes_available(
        session: Session,
        *,
        organization_id: int,
        codes: tuple[str, ...],
    ) -> None:
        normalized = [code.lower() for code in codes]
        if len(set(normalized)) != len(normalized):
            raise TraceabilityOperationValidationError(
                "DUPLICATE_OUTPUT_BATCH_CODE",
                "Los códigos de lotes de salida no pueden repetirse.",
            )
        existing = session.execute(
            select(TraceabilityBatch.code).where(
                TraceabilityBatch.organization_id == organization_id,
                func.lower(TraceabilityBatch.code).in_(normalized),
            )
        ).scalars().all()
        if existing:
            raise TraceabilityOperationConflictError(
                "Ya existe un lote industrial con uno de los códigos indicados."
            )

    @staticmethod
    def _ensure_event_code_available(
        session: Session, *, organization_id: int, code: str
    ) -> None:
        existing = session.execute(
            select(TraceabilityEvent.id).where(
                TraceabilityEvent.organization_id == organization_id,
                func.lower(TraceabilityEvent.event_code) == code.lower(),
            )
        ).scalar_one_or_none()
        if existing is not None:
            raise TraceabilityOperationConflictError(
                "Ya existe un evento industrial con ese código."
            )

    @staticmethod
    def _ensure_shipment_code_available(
        session: Session, *, organization_id: int, code: str
    ) -> None:
        existing = session.execute(
            select(Shipment.id).where(
                Shipment.organization_id == organization_id,
                func.lower(Shipment.shipment_code) == code.lower(),
            )
        ).scalar_one_or_none()
        if existing is not None:
            raise TraceabilityOperationConflictError(
                "Ya existe un despacho con ese código."
            )

    def create_receipt_draft(
        self,
        *,
        organization_id: int,
        actor: AuditActor,
        source_identifier: str,
        event_code: str,
        batch_code: str,
        product_name: str | None,
        quantity: Any,
        unit: str,
        occurred_at: datetime,
        facility_reference: str | None = None,
        notes: str | None = None,
    ) -> DraftEventResult:
        organization_id = int(organization_id)
        _actor_scope(actor, organization_id)
        source_identifier = _text(source_identifier, field="origen", maximum=100) or ""
        event_code = _text(event_code, field="codigo_evento", maximum=120) or ""
        batch_code = _text(batch_code, field="codigo_lote", maximum=120) or ""
        product_name = _text(product_name, field="producto", maximum=160, required=False)
        facility_reference = _text(
            facility_reference, field="instalacion", maximum=160, required=False
        )
        notes = _text(notes, field="notas", maximum=2000, required=False)
        quantity = _qty(quantity)
        unit = _unit(unit)

        session = self._session(organization_id)
        try:
            source = session.execute(
                select(Lote).where(
                    Lote.organization_id == organization_id,
                    func.lower(Lote.identificador) == source_identifier.lower(),
                )
            ).scalar_one_or_none()
            if source is None:
                raise TraceabilityOperationNotFoundError(
                    "La parcela o rodal de origen no existe en la organización."
                )

            self._ensure_event_code_available(
                session, organization_id=organization_id, code=event_code
            )
            self._ensure_batch_codes_available(
                session, organization_id=organization_id, codes=(batch_code,)
            )

            batch = TraceabilityBatch(
                organization_id=organization_id,
                code=batch_code,
                product_name=product_name or source.producto_forestal,
                stage="RAW_MATERIAL",
                unit=unit,
                status="ACTIVE",
                source_lote_id=source.id,
                created_by_user_id=actor.user_id,
            )
            event = TraceabilityEvent(
                organization_id=organization_id,
                event_code=event_code,
                event_type="RECEIPT",
                status="DRAFT",
                occurred_at=occurred_at,
                facility_reference=facility_reference,
                notes=notes,
                created_by_user_id=actor.user_id,
            )
            session.add_all([batch, event])
            session.flush()
            session.add(
                TraceabilityEventOutput(
                    organization_id=organization_id,
                    event_id=event.id,
                    batch_id=batch.id,
                    quantity=quantity,
                    unit=unit,
                )
            )
            session.commit()
            return DraftEventResult(
                event_id=int(event.id),
                event_public_id=event.public_id,
                event_code=event.event_code,
                event_type=event.event_type,
                status=event.status,
                output_batch_public_ids=(batch.public_id,),
            )
        except TraceabilityOperationError:
            session.rollback()
            raise
        except IntegrityError as exc:
            session.rollback()
            raise TraceabilityOperationConflictError(
                "No fue posible crear el borrador porque uno de sus códigos ya existe."
            ) from exc
        except SQLAlchemyError as exc:
            session.rollback()
            raise TraceabilityOperationPersistenceError(
                "No fue posible guardar el borrador de recepción."
            ) from exc
        finally:
            session.close()

    def create_process_draft(
        self,
        *,
        organization_id: int,
        actor: AuditActor,
        event_code: str,
        event_type: str,
        occurred_at: datetime,
        inputs: tuple[ProcessInputDraft, ...],
        outputs: tuple[ProcessOutputDraft, ...],
        facility_reference: str | None = None,
        notes: str | None = None,
    ) -> DraftEventResult:
        organization_id = int(organization_id)
        _actor_scope(actor, organization_id)
        event_code = _text(event_code, field="codigo_evento", maximum=120) or ""
        event_type = str(event_type or "").strip().upper()
        if event_type not in PROCESS_EVENT_TYPES:
            raise TraceabilityOperationValidationError(
                "INVALID_PROCESS_TYPE",
                "El proceso debe ser TRANSFORMATION, MIX, SPLIT o REPACK.",
            )
        if not inputs or not outputs:
            raise TraceabilityOperationValidationError(
                "EMPTY_PROCESS",
                "El proceso debe tener al menos una entrada y una salida.",
            )
        if event_type == "MIX" and len(inputs) < 2:
            raise TraceabilityOperationValidationError(
                "MIX_REQUIRES_MULTIPLE_INPUTS",
                "Una mezcla debe documentar al menos dos lotes de entrada.",
            )
        if event_type == "SPLIT" and len(outputs) < 2:
            raise TraceabilityOperationValidationError(
                "SPLIT_REQUIRES_MULTIPLE_OUTPUTS",
                "Una división debe documentar al menos dos lotes de salida.",
            )
        facility_reference = _text(
            facility_reference, field="instalacion", maximum=160, required=False
        )
        notes = _text(notes, field="notas", maximum=2000, required=False)

        input_ids = [item.batch_public_id for item in inputs]
        if len(set(input_ids)) != len(input_ids):
            raise TraceabilityOperationValidationError(
                "DUPLICATE_INPUT_BATCH",
                "Un lote de entrada no puede repetirse dentro del mismo proceso.",
            )

        normalized_outputs = tuple(
            ProcessOutputDraft(
                code=_text(item.code, field="codigo_lote", maximum=120) or "",
                product_name=_text(item.product_name, field="producto", maximum=160) or "",
                stage=_stage(item.stage),
                unit=_unit(item.unit),
                quantity=_qty(item.quantity),
            )
            for item in outputs
        )
        normalized_inputs = tuple(
            ProcessInputDraft(
                batch_public_id=item.batch_public_id,
                quantity=_qty(item.quantity),
            )
            for item in inputs
        )

        session = self._session(organization_id)
        try:
            self._ensure_event_code_available(
                session, organization_id=organization_id, code=event_code
            )
            self._ensure_batch_codes_available(
                session,
                organization_id=organization_id,
                codes=tuple(item.code for item in normalized_outputs),
            )

            input_batches = session.execute(
                select(TraceabilityBatch).where(
                    TraceabilityBatch.organization_id == organization_id,
                    TraceabilityBatch.public_id.in_(input_ids),
                )
            ).scalars().all()
            by_public_id = {batch.public_id: batch for batch in input_batches}
            if len(by_public_id) != len(input_ids):
                raise TraceabilityOperationNotFoundError(
                    "Uno o más lotes de entrada no existen en la organización."
                )
            for batch in input_batches:
                if batch.status != "ACTIVE":
                    raise TraceabilityOperationValidationError(
                        "INPUT_BATCH_NOT_ACTIVE",
                        f"El lote {batch.code} no está activo.",
                    )

            event = TraceabilityEvent(
                organization_id=organization_id,
                event_code=event_code,
                event_type=event_type,
                status="DRAFT",
                occurred_at=occurred_at,
                facility_reference=facility_reference,
                notes=notes,
                created_by_user_id=actor.user_id,
            )
            session.add(event)
            output_batches: list[TraceabilityBatch] = []
            for output in normalized_outputs:
                batch = TraceabilityBatch(
                    organization_id=organization_id,
                    code=output.code,
                    product_name=output.product_name,
                    stage=output.stage,
                    unit=output.unit,
                    status="ACTIVE",
                    source_lote_id=None,
                    created_by_user_id=actor.user_id,
                )
                output_batches.append(batch)
                session.add(batch)
            session.flush()

            for item in normalized_inputs:
                batch = by_public_id[item.batch_public_id]
                session.add(
                    TraceabilityEventInput(
                        organization_id=organization_id,
                        event_id=event.id,
                        batch_id=batch.id,
                        quantity=item.quantity,
                        unit=batch.unit,
                    )
                )
            for item, batch in zip(normalized_outputs, output_batches, strict=True):
                session.add(
                    TraceabilityEventOutput(
                        organization_id=organization_id,
                        event_id=event.id,
                        batch_id=batch.id,
                        quantity=item.quantity,
                        unit=item.unit,
                    )
                )
            session.commit()
            return DraftEventResult(
                event_id=int(event.id),
                event_public_id=event.public_id,
                event_code=event.event_code,
                event_type=event.event_type,
                status=event.status,
                output_batch_public_ids=tuple(batch.public_id for batch in output_batches),
            )
        except TraceabilityOperationError:
            session.rollback()
            raise
        except IntegrityError as exc:
            session.rollback()
            raise TraceabilityOperationConflictError(
                "No fue posible crear el proceso porque uno de sus códigos ya existe."
            ) from exc
        except SQLAlchemyError as exc:
            session.rollback()
            raise TraceabilityOperationPersistenceError(
                "No fue posible guardar el borrador del proceso."
            ) from exc
        finally:
            session.close()

    def create_shipment_draft(
        self,
        *,
        organization_id: int,
        actor: AuditActor,
        shipment_code: str,
        items: tuple[ShipmentItemDraft, ...],
        sale_reference: str | None = None,
        buyer_reference: str | None = None,
        destination_country: str | None = None,
    ) -> DraftShipmentResult:
        organization_id = int(organization_id)
        _actor_scope(actor, organization_id)
        shipment_code = _text(shipment_code, field="codigo_despacho", maximum=120) or ""
        sale_reference = _text(
            sale_reference, field="referencia_venta", maximum=160, required=False
        )
        buyer_reference = _text(
            buyer_reference, field="comprador", maximum=160, required=False
        )
        destination_country = str(destination_country or "").strip().upper() or None
        if destination_country is not None and (
            len(destination_country) != 2 or not destination_country.isalpha()
        ):
            raise TraceabilityOperationValidationError(
                "INVALID_DESTINATION_COUNTRY",
                "El país de destino debe informarse con un código ISO de dos letras.",
            )
        if not items:
            raise TraceabilityOperationValidationError(
                "EMPTY_SHIPMENT",
                "El despacho debe contener al menos un lote industrial.",
            )
        ids = [item.batch_public_id for item in items]
        if len(set(ids)) != len(ids):
            raise TraceabilityOperationValidationError(
                "DUPLICATE_SHIPMENT_BATCH",
                "Un mismo lote no puede repetirse dentro del despacho.",
            )
        normalized_items = tuple(
            ShipmentItemDraft(
                batch_public_id=item.batch_public_id,
                quantity=_qty(item.quantity),
            )
            for item in items
        )

        session = self._session(organization_id)
        try:
            self._ensure_shipment_code_available(
                session, organization_id=organization_id, code=shipment_code
            )
            batches = session.execute(
                select(TraceabilityBatch).where(
                    TraceabilityBatch.organization_id == organization_id,
                    TraceabilityBatch.public_id.in_(ids),
                )
            ).scalars().all()
            by_public_id = {batch.public_id: batch for batch in batches}
            if len(by_public_id) != len(ids):
                raise TraceabilityOperationNotFoundError(
                    "Uno o más lotes del despacho no existen en la organización."
                )
            for batch in batches:
                if batch.status != "ACTIVE":
                    raise TraceabilityOperationValidationError(
                        "SHIPMENT_BATCH_NOT_ACTIVE",
                        f"El lote {batch.code} no está activo.",
                    )

            shipment = Shipment(
                organization_id=organization_id,
                shipment_code=shipment_code,
                sale_reference=sale_reference,
                buyer_reference=buyer_reference,
                destination_country=destination_country,
                shipped_at=None,
                status="DRAFT",
                created_by_user_id=actor.user_id,
            )
            session.add(shipment)
            session.flush()
            for item in normalized_items:
                batch = by_public_id[item.batch_public_id]
                session.add(
                    ShipmentItem(
                        organization_id=organization_id,
                        shipment_id=shipment.id,
                        batch_id=batch.id,
                        quantity=item.quantity,
                        unit=batch.unit,
                    )
                )
            session.commit()
            return DraftShipmentResult(
                shipment_id=int(shipment.id),
                shipment_public_id=shipment.public_id,
                shipment_code=shipment.shipment_code,
                status=shipment.status,
            )
        except TraceabilityOperationError:
            session.rollback()
            raise
        except IntegrityError as exc:
            session.rollback()
            raise TraceabilityOperationConflictError(
                "No fue posible crear el despacho porque su código ya existe."
            ) from exc
        except SQLAlchemyError as exc:
            session.rollback()
            raise TraceabilityOperationPersistenceError(
                "No fue posible guardar el borrador del despacho."
            ) from exc
        finally:
            session.close()

    def resolve_event_id(self, *, organization_id: int, public_id: UUID) -> int:
        session = self._session(int(organization_id))
        try:
            value = session.execute(
                select(TraceabilityEvent.id).where(
                    TraceabilityEvent.organization_id == int(organization_id),
                    TraceabilityEvent.public_id == public_id,
                )
            ).scalar_one_or_none()
            if value is None:
                raise TraceabilityOperationNotFoundError(
                    "El evento industrial no existe en la organización."
                )
            return int(value)
        except TraceabilityOperationError:
            raise
        except SQLAlchemyError as exc:
            raise TraceabilityOperationPersistenceError(
                "No fue posible consultar el evento industrial."
            ) from exc
        finally:
            session.close()

    def resolve_shipment_id(self, *, organization_id: int, public_id: UUID) -> int:
        session = self._session(int(organization_id))
        try:
            value = session.execute(
                select(Shipment.id).where(
                    Shipment.organization_id == int(organization_id),
                    Shipment.public_id == public_id,
                )
            ).scalar_one_or_none()
            if value is None:
                raise TraceabilityOperationNotFoundError(
                    "El despacho no existe en la organización."
                )
            return int(value)
        except TraceabilityOperationError:
            raise
        except SQLAlchemyError as exc:
            raise TraceabilityOperationPersistenceError(
                "No fue posible consultar el despacho."
            ) from exc
        finally:
            session.close()

    def post_event(
        self,
        *,
        organization_id: int,
        event_public_id: UUID,
        actor: AuditActor,
        request_context=None,
    ):
        event_id = self.resolve_event_id(
            organization_id=organization_id, public_id=event_public_id
        )
        try:
            return TraceabilityLedgerService(
                session_factory=self._session_factory
            ).post_event(
                organization_id=organization_id,
                event_id=event_id,
                actor=actor,
                request_context=request_context,
            )
        except LedgerAuthorizationError as exc:
            raise TraceabilityOperationAuthorizationError(str(exc)) from exc
        except LedgerNotFoundError as exc:
            raise TraceabilityOperationNotFoundError(str(exc)) from exc
        except LedgerValidationError as exc:
            raise TraceabilityOperationValidationError(exc.code, exc.detail) from exc
        except LedgerStateError as exc:
            raise TraceabilityOperationConflictError(str(exc)) from exc
        except LedgerPersistenceError as exc:
            raise TraceabilityOperationPersistenceError(str(exc)) from exc
        except TraceabilityLedgerError as exc:
            raise TraceabilityOperationError(str(exc)) from exc

    def dispatch_shipment(
        self,
        *,
        organization_id: int,
        shipment_public_id: UUID,
        actor: AuditActor,
        request_context=None,
    ):
        shipment_id = self.resolve_shipment_id(
            organization_id=organization_id, public_id=shipment_public_id
        )
        try:
            return TraceabilityLedgerService(
                session_factory=self._session_factory
            ).dispatch_shipment(
                organization_id=organization_id,
                shipment_id=shipment_id,
                actor=actor,
                request_context=request_context,
            )
        except LedgerAuthorizationError as exc:
            raise TraceabilityOperationAuthorizationError(str(exc)) from exc
        except LedgerNotFoundError as exc:
            raise TraceabilityOperationNotFoundError(str(exc)) from exc
        except LedgerValidationError as exc:
            raise TraceabilityOperationValidationError(exc.code, exc.detail) from exc
        except LedgerStateError as exc:
            raise TraceabilityOperationConflictError(str(exc)) from exc
        except LedgerPersistenceError as exc:
            raise TraceabilityOperationPersistenceError(str(exc)) from exc
        except TraceabilityLedgerError as exc:
            raise TraceabilityOperationError(str(exc)) from exc

    def snapshot(self, *, organization_id: int) -> OperationsSnapshot:
        organization_id = int(organization_id)
        session = self._session(organization_id)
        try:
            source_rows = session.execute(
                select(Lote)
                .where(Lote.organization_id == organization_id)
                .order_by(func.lower(Lote.identificador))
                .limit(250)
            ).scalars().all()

            produced = (
                select(func.coalesce(func.sum(TraceabilityEventOutput.quantity), 0))
                .join(TraceabilityEvent, TraceabilityEvent.id == TraceabilityEventOutput.event_id)
                .where(
                    TraceabilityEventOutput.organization_id == organization_id,
                    TraceabilityEventOutput.batch_id == TraceabilityBatch.id,
                    TraceabilityEvent.organization_id == organization_id,
                    TraceabilityEvent.status == "POSTED",
                )
                .correlate(TraceabilityBatch)
                .scalar_subquery()
            )
            consumed = (
                select(func.coalesce(func.sum(TraceabilityEventInput.quantity), 0))
                .join(TraceabilityEvent, TraceabilityEvent.id == TraceabilityEventInput.event_id)
                .where(
                    TraceabilityEventInput.organization_id == organization_id,
                    TraceabilityEventInput.batch_id == TraceabilityBatch.id,
                    TraceabilityEvent.organization_id == organization_id,
                    TraceabilityEvent.status == "POSTED",
                )
                .correlate(TraceabilityBatch)
                .scalar_subquery()
            )
            dispatched = (
                select(func.coalesce(func.sum(ShipmentItem.quantity), 0))
                .join(Shipment, Shipment.id == ShipmentItem.shipment_id)
                .where(
                    ShipmentItem.organization_id == organization_id,
                    ShipmentItem.batch_id == TraceabilityBatch.id,
                    Shipment.organization_id == organization_id,
                    Shipment.status == "DISPATCHED",
                )
                .correlate(TraceabilityBatch)
                .scalar_subquery()
            )
            batch_rows = session.execute(
                select(
                    TraceabilityBatch,
                    (produced - consumed - dispatched).label("available"),
                )
                .where(
                    TraceabilityBatch.organization_id == organization_id,
                    TraceabilityBatch.status == "ACTIVE",
                )
                .order_by(func.lower(TraceabilityBatch.code))
                .limit(300)
            ).all()

            draft_events = session.execute(
                select(TraceabilityEvent)
                .where(
                    TraceabilityEvent.organization_id == organization_id,
                    TraceabilityEvent.status == "DRAFT",
                )
                .order_by(TraceabilityEvent.occurred_at.desc())
                .limit(50)
            ).scalars().all()
            draft_shipments = session.execute(
                select(Shipment)
                .where(
                    Shipment.organization_id == organization_id,
                    Shipment.status.in_(("DRAFT", "CONFIRMED")),
                )
                .order_by(Shipment.created_at.desc())
                .limit(50)
            ).scalars().all()

            return OperationsSnapshot(
                source_lotes=tuple(
                    SourceLoteChoice(
                        identifier=row.identificador,
                        producer=row.productor_id,
                        product=row.producto_forestal,
                        status=row.estatus,
                    )
                    for row in source_rows
                ),
                active_batches=tuple(
                    ActiveBatchChoice(
                        public_id=batch.public_id,
                        code=batch.code,
                        product_name=batch.product_name,
                        stage=batch.stage,
                        unit=batch.unit,
                        available=Decimal(str(available or 0)).quantize(QTY_QUANTUM),
                    )
                    for batch, available in batch_rows
                ),
                draft_events=tuple(
                    DraftEventChoice(
                        public_id=event.public_id,
                        code=event.event_code,
                        event_type=event.event_type,
                        occurred_at=event.occurred_at,
                        facility_reference=event.facility_reference,
                        input_count=len(event.inputs),
                        output_count=len(event.outputs),
                    )
                    for event in draft_events
                ),
                draft_shipments=tuple(
                    DraftShipmentChoice(
                        public_id=shipment.public_id,
                        code=shipment.shipment_code,
                        sale_reference=shipment.sale_reference,
                        buyer_reference=shipment.buyer_reference,
                        destination_country=shipment.destination_country,
                        item_count=len(shipment.items),
                    )
                    for shipment in draft_shipments
                ),
            )
        except SQLAlchemyError as exc:
            raise TraceabilityOperationPersistenceError(
                "No fue posible cargar el workspace operativo."
            ) from exc
        finally:
            session.close()
