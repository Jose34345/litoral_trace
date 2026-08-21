"""Server-rendered UX10-D chain-of-custody operations workspace."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse

from litoral_trace.auth.rbac import Permission, has_permission
from litoral_trace.services.audit import (
    build_audit_actor_from_user,
    build_request_audit_context,
)
from litoral_trace.services.traceability_operations import (
    OperationsSnapshot,
    ProcessInputDraft,
    ProcessOutputDraft,
    ShipmentItemDraft,
    TraceabilityOperationAuthorizationError,
    TraceabilityOperationConflictError,
    TraceabilityOperationError,
    TraceabilityOperationNotFoundError,
    TraceabilityOperationPersistenceError,
    TraceabilityOperationService,
    TraceabilityOperationValidationError,
)
from litoral_trace.web.csrf import enforce_csrf, get_csrf_browser_nonce
from litoral_trace.web.runtime import (
    get_html_route_user,
    render_access_denied,
    render_csrf_failure,
    render_web_template,
)


router = APIRouter(tags=["Frontend B2B"])
_ARGENTINA_TZ = ZoneInfo("America/Argentina/Cordoba")
_EVENT_LABELS = {
    "RECEIPT": "Recepción",
    "TRANSFORMATION": "Transformación",
    "MIX": "Mezcla",
    "SPLIT": "División",
    "REPACK": "Reempaque",
}
_STAGE_LABELS = {
    "RAW_MATERIAL": "Materia prima",
    "INTERMEDIATE": "Producto intermedio",
    "FINISHED_GOOD": "Producto terminado",
}
_RESULT_MESSAGES = {
    "receipt-draft": ("success", "Recepción guardada", "La recepción quedó como borrador recuperable."),
    "receipt-posted": ("success", "Recepción contabilizada", "La recepción fue contabilizada por el ledger y el stock quedó disponible."),
    "process-draft": ("success", "Proceso guardado", "El proceso industrial quedó como borrador recuperable."),
    "process-posted": ("success", "Proceso contabilizado", "El proceso superó los controles de stock, unidad y secuencia temporal."),
    "event-posted": ("success", "Movimiento contabilizado", "El borrador fue contabilizado por el ledger."),
    "shipment-draft": ("success", "Despacho guardado", "El despacho quedó como borrador y todavía no consume stock."),
    "shipment-dispatched": ("success", "Despacho confirmado", "El despacho consumió stock mediante el ledger y ya puede reconstruirse desde Trazabilidad."),
}


def _service() -> TraceabilityOperationService:
    return TraceabilityOperationService()


def _number(value: Decimal) -> str:
    rendered = f"{value:.6f}".rstrip("0").rstrip(".")
    return (rendered or "0").replace(".", ",")


def _parse_uuid(value: str, *, field: str) -> UUID:
    try:
        return UUID(str(value).strip())
    except (ValueError, TypeError, AttributeError) as exc:
        raise TraceabilityOperationValidationError(
            "INVALID_REFERENCE",
            f"La referencia de {field} no es válida.",
        ) from exc


def _parse_local_datetime(value: str) -> datetime:
    normalized = str(value or "").strip()
    if not normalized:
        raise TraceabilityOperationValidationError(
            "MISSING_OCCURRED_AT",
            "La fecha y hora de la operación son obligatorias.",
        )
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise TraceabilityOperationValidationError(
            "INVALID_OCCURRED_AT",
            "La fecha y hora de la operación no tienen un formato válido.",
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_ARGENTINA_TZ)
    return parsed.astimezone(timezone.utc)


def _parse_process_inputs(form) -> tuple[ProcessInputDraft, ...]:
    batch_values = list(form.getlist("input_batch"))
    quantity_values = list(form.getlist("input_quantity"))
    if len(batch_values) != len(quantity_values):
        raise TraceabilityOperationValidationError(
            "MALFORMED_INPUT_LINES",
            "Las líneas de entrada del proceso están incompletas.",
        )
    result: list[ProcessInputDraft] = []
    for batch_value, quantity_value in zip(batch_values, quantity_values, strict=True):
        batch_value = str(batch_value or "").strip()
        quantity_value = str(quantity_value or "").strip()
        if not batch_value and not quantity_value:
            continue
        if not batch_value or not quantity_value:
            raise TraceabilityOperationValidationError(
                "INCOMPLETE_INPUT_LINE",
                "Cada entrada debe indicar lote y cantidad.",
            )
        result.append(
            ProcessInputDraft(
                batch_public_id=_parse_uuid(batch_value, field="lote de entrada"),
                quantity=Decimal(quantity_value.replace(",", ".")),
            )
        )
    return tuple(result)


def _parse_process_outputs(form) -> tuple[ProcessOutputDraft, ...]:
    codes = list(form.getlist("output_code"))
    products = list(form.getlist("output_product"))
    stages = list(form.getlist("output_stage"))
    units = list(form.getlist("output_unit"))
    quantities = list(form.getlist("output_quantity"))
    widths = {len(codes), len(products), len(stages), len(units), len(quantities)}
    if len(widths) != 1:
        raise TraceabilityOperationValidationError(
            "MALFORMED_OUTPUT_LINES",
            "Las líneas de salida del proceso están incompletas.",
        )
    result: list[ProcessOutputDraft] = []
    for code, product, stage, unit, quantity in zip(
        codes, products, stages, units, quantities, strict=True
    ):
        values = [str(value or "").strip() for value in (code, product, stage, unit, quantity)]
        if not any(values):
            continue
        if not all(values):
            raise TraceabilityOperationValidationError(
                "INCOMPLETE_OUTPUT_LINE",
                "Cada salida debe indicar código, producto, etapa, unidad y cantidad.",
            )
        result.append(
            ProcessOutputDraft(
                code=values[0],
                product_name=values[1],
                stage=values[2],
                unit=values[3],
                quantity=Decimal(values[4].replace(",", ".")),
            )
        )
    return tuple(result)


def _parse_shipment_items(form) -> tuple[ShipmentItemDraft, ...]:
    batch_values = list(form.getlist("shipment_batch"))
    quantity_values = list(form.getlist("shipment_quantity"))
    if len(batch_values) != len(quantity_values):
        raise TraceabilityOperationValidationError(
            "MALFORMED_SHIPMENT_LINES",
            "Las líneas del despacho están incompletas.",
        )
    result: list[ShipmentItemDraft] = []
    for batch_value, quantity_value in zip(batch_values, quantity_values, strict=True):
        batch_value = str(batch_value or "").strip()
        quantity_value = str(quantity_value or "").strip()
        if not batch_value and not quantity_value:
            continue
        if not batch_value or not quantity_value:
            raise TraceabilityOperationValidationError(
                "INCOMPLETE_SHIPMENT_LINE",
                "Cada línea del despacho debe indicar lote y cantidad.",
            )
        result.append(
            ShipmentItemDraft(
                batch_public_id=_parse_uuid(batch_value, field="lote del despacho"),
                quantity=Decimal(quantity_value.replace(",", ".")),
            )
        )
    return tuple(result)


def _safe_error(exc: Exception) -> dict[str, str]:
    if isinstance(exc, TraceabilityOperationValidationError):
        return {"code": exc.code, "title": "Operación no válida", "message": exc.detail}
    if isinstance(exc, TraceabilityOperationNotFoundError):
        return {"code": "TRACEABILITY_NOT_FOUND", "title": "Referencia no encontrada", "message": str(exc)}
    if isinstance(exc, TraceabilityOperationConflictError):
        return {"code": "TRACEABILITY_CONFLICT", "title": "Operación en conflicto", "message": str(exc)}
    if isinstance(exc, TraceabilityOperationAuthorizationError):
        return {"code": "TRACEABILITY_DENIED", "title": "Operación no autorizada", "message": "La operación no está permitida para la organización autenticada."}
    if isinstance(exc, TraceabilityOperationPersistenceError):
        return {"code": "TRACEABILITY_SERVICE_UNAVAILABLE", "title": "Servicio no disponible", "message": "No fue posible completar la operación en este momento."}
    return {"code": "TRACEABILITY_OPERATION_ERROR", "title": "No se pudo completar la operación", "message": "La operación fue rechazada de forma segura."}


def _status_for_error(exc: Exception) -> int:
    if isinstance(exc, TraceabilityOperationValidationError):
        return status.HTTP_422_UNPROCESSABLE_CONTENT
    if isinstance(exc, TraceabilityOperationNotFoundError):
        return status.HTTP_404_NOT_FOUND
    if isinstance(exc, TraceabilityOperationConflictError):
        return status.HTTP_409_CONFLICT
    if isinstance(exc, TraceabilityOperationAuthorizationError):
        return status.HTTP_403_FORBIDDEN
    if isinstance(exc, TraceabilityOperationPersistenceError):
        return status.HTTP_503_SERVICE_UNAVAILABLE
    return status.HTTP_400_BAD_REQUEST


def _present_snapshot(
    snapshot: OperationsSnapshot | None,
    *,
    can_dispatch: bool,
    result_code: str | None = None,
    error: dict[str, str] | None = None,
) -> dict:
    message = None
    if result_code in _RESULT_MESSAGES:
        level, title, text = _RESULT_MESSAGES[result_code]
        message = {"level": level, "title": title, "message": text}
    if snapshot is None:
        return {
            "source_lotes": (),
            "active_batches": (),
            "draft_events": (),
            "draft_shipments": (),
            "can_dispatch": can_dispatch,
            "message": message,
            "error": error,
        }
    return {
        "source_lotes": snapshot.source_lotes,
        "active_batches": tuple(
            {
                "public_id": str(batch.public_id),
                "code": batch.code,
                "product_name": batch.product_name,
                "stage": batch.stage,
                "stage_label": _STAGE_LABELS.get(batch.stage, batch.stage),
                "unit": batch.unit,
                "available": f"{_number(batch.available)} {batch.unit}",
                "available_positive": batch.available > 0,
            }
            for batch in snapshot.active_batches
        ),
        "draft_events": tuple(
            {
                "public_id": str(event.public_id),
                "code": event.code,
                "type": event.event_type,
                "type_label": _EVENT_LABELS.get(event.event_type, event.event_type),
                "occurred_at": event.occurred_at.astimezone(_ARGENTINA_TZ).strftime("%d/%m/%Y %H:%M"),
                "facility": event.facility_reference or "—",
                "input_count": event.input_count,
                "output_count": event.output_count,
            }
            for event in snapshot.draft_events
        ),
        "draft_shipments": tuple(
            {
                "public_id": str(shipment.public_id),
                "code": shipment.code,
                "sale_reference": shipment.sale_reference or "—",
                "buyer_reference": shipment.buyer_reference or "—",
                "destination_country": shipment.destination_country or "—",
                "item_count": shipment.item_count,
            }
            for shipment in snapshot.draft_shipments
        ),
        "can_dispatch": can_dispatch,
        "message": message,
        "error": error,
    }


def _render(
    request: Request,
    *,
    user,
    result_code: str | None = None,
    error: dict[str, str] | None = None,
    status_code: int = status.HTTP_200_OK,
) -> HTMLResponse:
    try:
        snapshot = _service().snapshot(organization_id=user.organization_id)
    except TraceabilityOperationError as exc:
        snapshot = None
        if error is None:
            error = _safe_error(exc)
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return render_web_template(
        request,
        "traceability_operations.html",
        user=user,
        context={
            "operations_view": _present_snapshot(
                snapshot,
                can_dispatch=has_permission(user, Permission.TRACEABILITY_DISPATCH),
                result_code=result_code,
                error=error,
            )
        },
        status_code=status_code,
    )


async def _csrf_or_response(request: Request, user):
    browser_nonce = get_csrf_browser_nonce(request)
    try:
        await enforce_csrf(
            request,
            user=user,
            browser_nonce=browser_nonce,
            require_browser_binding=True,
        )
    except HTTPException:
        return render_csrf_failure()
    return None


def _redirect_result(code: str) -> RedirectResponse:
    return RedirectResponse(
        url=f"/operations?{urlencode({'result': code})}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.get("/operations", response_class=HTMLResponse, include_in_schema=False)
async def render_traceability_operations(request: Request) -> HTMLResponse:
    user, denied = get_html_route_user(
        request,
        required_permission=Permission.TRACEABILITY_OPERATE,
    )
    if denied is not None:
        return denied
    return _render(
        request,
        user=user,
        result_code=request.query_params.get("result"),
    )


@router.post("/operations/receipts", include_in_schema=False)
async def create_receipt_operation(request: Request):
    user, denied = get_html_route_user(
        request,
        required_permission=Permission.TRACEABILITY_OPERATE,
    )
    if denied is not None:
        return denied
    csrf_response = await _csrf_or_response(request, user)
    if csrf_response is not None:
        return csrf_response
    form = await request.form()
    actor = build_audit_actor_from_user(user)
    service = _service()
    try:
        result = service.create_receipt_draft(
            organization_id=user.organization_id,
            actor=actor,
            source_identifier=str(form.get("source_identifier", "")),
            event_code=str(form.get("event_code", "")),
            batch_code=str(form.get("batch_code", "")),
            product_name=str(form.get("product_name", "")),
            quantity=str(form.get("quantity", "")),
            unit=str(form.get("unit", "M3")),
            occurred_at=_parse_local_datetime(str(form.get("occurred_at", ""))),
            facility_reference=str(form.get("facility_reference", "")),
            notes=str(form.get("notes", "")),
        )
        if str(form.get("commit_mode", "draft")) == "post":
            service.post_event(
                organization_id=user.organization_id,
                event_public_id=result.event_public_id,
                actor=actor,
                request_context=build_request_audit_context(request),
            )
            return _redirect_result("receipt-posted")
        return _redirect_result("receipt-draft")
    except TraceabilityOperationError as exc:
        return _render(
            request,
            user=user,
            error=_safe_error(exc),
            status_code=_status_for_error(exc),
        )
    except (InvalidOperation, ValueError):
        exc = TraceabilityOperationValidationError(
            "INVALID_QUANTITY", "Una de las cantidades ingresadas no es válida."
        )
        return _render(request, user=user, error=_safe_error(exc), status_code=422)


@router.post("/operations/processes", include_in_schema=False)
async def create_process_operation(request: Request):
    user, denied = get_html_route_user(
        request,
        required_permission=Permission.TRACEABILITY_OPERATE,
    )
    if denied is not None:
        return denied
    csrf_response = await _csrf_or_response(request, user)
    if csrf_response is not None:
        return csrf_response
    form = await request.form()
    actor = build_audit_actor_from_user(user)
    service = _service()
    try:
        result = service.create_process_draft(
            organization_id=user.organization_id,
            actor=actor,
            event_code=str(form.get("event_code", "")),
            event_type=str(form.get("event_type", "")),
            occurred_at=_parse_local_datetime(str(form.get("occurred_at", ""))),
            inputs=_parse_process_inputs(form),
            outputs=_parse_process_outputs(form),
            facility_reference=str(form.get("facility_reference", "")),
            notes=str(form.get("notes", "")),
        )
        if str(form.get("commit_mode", "draft")) == "post":
            service.post_event(
                organization_id=user.organization_id,
                event_public_id=result.event_public_id,
                actor=actor,
                request_context=build_request_audit_context(request),
            )
            return _redirect_result("process-posted")
        return _redirect_result("process-draft")
    except TraceabilityOperationError as exc:
        return _render(request, user=user, error=_safe_error(exc), status_code=_status_for_error(exc))
    except (InvalidOperation, ValueError):
        exc = TraceabilityOperationValidationError(
            "INVALID_QUANTITY", "Una de las cantidades ingresadas no es válida."
        )
        return _render(request, user=user, error=_safe_error(exc), status_code=422)


@router.post("/operations/events/{event_public_id}/post", include_in_schema=False)
async def post_existing_event_operation(request: Request, event_public_id: UUID):
    user, denied = get_html_route_user(
        request,
        required_permission=Permission.TRACEABILITY_OPERATE,
    )
    if denied is not None:
        return denied
    csrf_response = await _csrf_or_response(request, user)
    if csrf_response is not None:
        return csrf_response
    try:
        _service().post_event(
            organization_id=user.organization_id,
            event_public_id=event_public_id,
            actor=build_audit_actor_from_user(user),
            request_context=build_request_audit_context(request),
        )
        return _redirect_result("event-posted")
    except TraceabilityOperationError as exc:
        return _render(request, user=user, error=_safe_error(exc), status_code=_status_for_error(exc))


@router.post("/operations/shipments", include_in_schema=False)
async def create_shipment_operation(request: Request):
    user, denied = get_html_route_user(
        request,
        required_permission=Permission.TRACEABILITY_OPERATE,
    )
    if denied is not None:
        return denied
    csrf_response = await _csrf_or_response(request, user)
    if csrf_response is not None:
        return csrf_response
    form = await request.form()
    actor = build_audit_actor_from_user(user)
    service = _service()
    try:
        result = service.create_shipment_draft(
            organization_id=user.organization_id,
            actor=actor,
            shipment_code=str(form.get("shipment_code", "")),
            sale_reference=str(form.get("sale_reference", "")),
            buyer_reference=str(form.get("buyer_reference", "")),
            destination_country=str(form.get("destination_country", "")),
            items=_parse_shipment_items(form),
        )
        if str(form.get("commit_mode", "draft")) == "dispatch":
            if not has_permission(user, Permission.TRACEABILITY_DISPATCH):
                return render_access_denied()
            service.dispatch_shipment(
                organization_id=user.organization_id,
                shipment_public_id=result.shipment_public_id,
                actor=actor,
                request_context=build_request_audit_context(request),
            )
            return _redirect_result("shipment-dispatched")
        return _redirect_result("shipment-draft")
    except TraceabilityOperationError as exc:
        return _render(request, user=user, error=_safe_error(exc), status_code=_status_for_error(exc))
    except (InvalidOperation, ValueError):
        exc = TraceabilityOperationValidationError(
            "INVALID_QUANTITY", "Una de las cantidades ingresadas no es válida."
        )
        return _render(request, user=user, error=_safe_error(exc), status_code=422)


@router.post("/operations/shipments/{shipment_public_id}/dispatch", include_in_schema=False)
async def dispatch_existing_shipment_operation(request: Request, shipment_public_id: UUID):
    user, denied = get_html_route_user(
        request,
        required_permission=Permission.TRACEABILITY_DISPATCH,
    )
    if denied is not None:
        return denied
    csrf_response = await _csrf_or_response(request, user)
    if csrf_response is not None:
        return csrf_response
    try:
        _service().dispatch_shipment(
            organization_id=user.organization_id,
            shipment_public_id=shipment_public_id,
            actor=build_audit_actor_from_user(user),
            request_context=build_request_audit_context(request),
        )
        return _redirect_result("shipment-dispatched")
    except TraceabilityOperationError as exc:
        return _render(request, user=user, error=_safe_error(exc), status_code=_status_for_error(exc))
