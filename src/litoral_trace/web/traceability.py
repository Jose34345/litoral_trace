"""Server-rendered P1D traceability workspace.

The browser view deliberately reuses the P1C reverse-lineage service. It does
not calculate, persist, or mutate genealogy independently from the API layer.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from fastapi import APIRouter, Request, status
from fastapi.responses import HTMLResponse

from litoral_trace.auth.rbac import Permission
from litoral_trace.db.tenant import get_tenant_scoped_db_session
from litoral_trace.services.traceability_lineage import (
    TraceabilityLineageNotFoundError,
    TraceabilityLineageService,
    TraceabilityLineageValidationError,
)
from litoral_trace.web.runtime import (
    get_html_route_user,
    render_web_template,
)


router = APIRouter(tags=["Frontend B2B"])

_UNIT_LABELS = {
    "M3": "m³",
    "KG": "kg",
    "TON": "t",
}

_EVENT_LABELS = {
    "RECEIPT": "Ingreso",
    "TRANSFORMATION": "Transformación",
    "MIX": "Mezcla",
    "SPLIT": "División",
    "REPACK": "Reempaque",
    "ADJUSTMENT": "Ajuste",
}

_SHIPMENT_STATUS_LABELS = {
    "DRAFT": "Borrador",
    "CONFIRMED": "Confirmado",
    "DISPATCHED": "Despachado",
    "CANCELLED": "Cancelado",
}


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _number(value: Any, *, maximum_decimals: int = 6) -> str:
    number = _decimal(value)
    raw = f"{number:.{maximum_decimals}f}".rstrip("0").rstrip(".")
    if raw in {"", "-0"}:
        raw = "0"
    return raw.replace(".", ",")


def _unit(unit: Any) -> str:
    normalized = str(unit or "").upper()
    return _UNIT_LABELS.get(normalized, normalized or "—")


def _quantity(value: Any, unit: Any) -> str:
    return f"{_number(value)} {_unit(unit)}"


def _percentage(value: Any) -> str:
    percentage = _decimal(value) * Decimal("100")
    return f"{_number(percentage, maximum_decimals=2)}%"


def _date_time(value: Any) -> str:
    if not value:
        return "—"
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return str(value)
    return parsed.strftime("%d/%m/%Y %H:%M")


def _text(value: Any) -> str:
    if value is None:
        return "—"
    normalized = str(value).strip()
    return normalized or "—"


def _error(*, code: str, title: str, message: str) -> dict[str, str]:
    return {
        "code": code,
        "title": title,
        "message": message,
    }


def _present_source(source: dict[str, Any]) -> dict[str, Any]:
    lote = source.get("lote") or {}
    hectares = lote.get("hectareas")
    latitude = lote.get("latitud")
    longitude = lote.get("longitud")

    coordinates = "—"
    if latitude is not None and longitude is not None:
        try:
            coordinates = f"{float(latitude):.6f}, {float(longitude):.6f}"
        except (TypeError, ValueError):
            coordinates = f"{latitude}, {longitude}"

    return {
        "identifier": _text(lote.get("identificador")),
        "producer": _text(lote.get("productor_id")),
        "product": _text(lote.get("producto_forestal")),
        "status": _text(lote.get("estatus")),
        "hectares": (
            f"{_number(hectares, maximum_decimals=2)} ha"
            if hectares is not None
            else "—"
        ),
        "coordinates": coordinates,
        "has_polygon": bool(lote.get("polygon_wkt")),
        "quantity": _quantity(
            source.get("attributed_shipment_quantity"),
            source.get("unit"),
        ),
        "share": _percentage(source.get("share_of_shipped_unit")),
    }


def _present_event(event: dict[str, Any]) -> dict[str, Any]:
    reconciliation = event.get("reconciliation") or {}
    unit = reconciliation.get("unit")

    def edge_view(edge: dict[str, Any]) -> dict[str, str]:
        return {
            "batch_code": _text(edge.get("batch_code")),
            "quantity": _quantity(edge.get("quantity"), edge.get("unit")),
        }

    return {
        "code": _text(event.get("event_code")),
        "type": _text(event.get("event_type")),
        "type_label": _EVENT_LABELS.get(
            str(event.get("event_type") or "").upper(),
            _text(event.get("event_type")),
        ),
        "status": _text(event.get("status")),
        "occurred_at": _date_time(event.get("occurred_at")),
        "facility": _text(event.get("facility_reference")),
        "inputs": [edge_view(edge) for edge in event.get("inputs") or []],
        "outputs": [edge_view(edge) for edge in event.get("outputs") or []],
        "input_quantity": _quantity(reconciliation.get("input_quantity"), unit),
        "output_quantity": _quantity(reconciliation.get("output_quantity"), unit),
        "loss_quantity": (
            _quantity(reconciliation.get("loss_quantity"), unit)
            if reconciliation.get("loss_quantity") is not None
            else "—"
        ),
        "yield": (
            _percentage(reconciliation.get("yield_ratio"))
            if reconciliation.get("yield_ratio") is not None
            else "—"
        ),
    }


def _present_item(item: dict[str, Any]) -> dict[str, Any]:
    batch = item.get("batch") or {}
    contributions = []
    for source in item.get("source_contributions") or []:
        lote = source.get("lote") or {}
        contributions.append(
            {
                "identifier": _text(lote.get("identificador")),
                "producer": _text(lote.get("productor_id")),
                "quantity": _quantity(
                    source.get("attributed_shipment_quantity"),
                    source.get("unit"),
                ),
                "share": _percentage(source.get("share_of_shipment_item")),
            }
        )

    return {
        "batch_code": _text(batch.get("code")),
        "product": _text(batch.get("product_name")),
        "stage": _text(batch.get("stage")),
        "status": _text(batch.get("status")),
        "shipped_quantity": _quantity(item.get("shipped_quantity"), item.get("unit")),
        "attributed_quantity": _quantity(item.get("attributed_quantity"), item.get("unit")),
        "unresolved_quantity": _quantity(item.get("unresolved_quantity"), item.get("unit")),
        "complete": bool(item.get("complete")),
        "contributions": contributions,
    }


def build_traceability_view(
    *,
    query: str = "",
    payload: dict[str, Any] | None = None,
    error: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build a presentation-only model from one P1C lineage payload."""

    normalized_query = (query or "").strip()
    view: dict[str, Any] = {
        "query": normalized_query,
        "searched": bool(normalized_query),
        "error": error,
        "result": None,
    }

    if payload is None:
        return view

    shipment = payload.get("shipment") or {}
    totals = payload.get("unit_totals") or []
    sources = [_present_source(source) for source in payload.get("source_lotes") or []]
    events = [_present_event(event) for event in payload.get("events") or []]
    items = [_present_item(item) for item in payload.get("items") or []]
    issues = [
        {
            "code": _text(issue.get("code")),
            "message": _text(issue.get("message")),
        }
        for issue in payload.get("issues") or []
    ]

    primary_total = totals[0] if len(totals) == 1 else None
    complete = bool(payload.get("complete"))

    view["result"] = {
        "complete": complete,
        "state_label": "Genealogía cerrada" if complete else "Cadena incompleta",
        "state_description": (
            "Todo el volumen despachado quedó atribuido a orígenes trazables."
            if complete
            else "Existe volumen sin resolver o una inconsistencia que requiere revisión."
        ),
        "shipment": {
            "code": _text(shipment.get("shipment_code")),
            "sale_reference": _text(shipment.get("sale_reference")),
            "buyer_reference": _text(shipment.get("buyer_reference")),
            "destination_country": _text(shipment.get("destination_country")),
            "shipped_at": _date_time(shipment.get("shipped_at")),
            "status": _text(shipment.get("status")),
            "status_label": _SHIPMENT_STATUS_LABELS.get(
                str(shipment.get("status") or "").upper(),
                _text(shipment.get("status")),
            ),
            "lineage_state": _text(shipment.get("lineage_state")),
            "lineage_state_label": (
                "Final"
                if shipment.get("lineage_state") == "FINAL"
                else "Previsualización"
            ),
        },
        "allocation_method": _text(payload.get("allocation_method")),
        "totals": [
            {
                "unit": _unit(total.get("unit")),
                "shipped": _quantity(total.get("shipped_quantity"), total.get("unit")),
                "attributed": _quantity(total.get("attributed_quantity"), total.get("unit")),
                "unresolved": _quantity(total.get("unresolved_quantity"), total.get("unit")),
            }
            for total in totals
        ],
        "primary_total": (
            {
                "shipped": _quantity(
                    primary_total.get("shipped_quantity"),
                    primary_total.get("unit"),
                ),
                "attributed": _quantity(
                    primary_total.get("attributed_quantity"),
                    primary_total.get("unit"),
                ),
                "unresolved": _quantity(
                    primary_total.get("unresolved_quantity"),
                    primary_total.get("unit"),
                ),
            }
            if primary_total is not None
            else None
        ),
        "source_count": len(sources),
        "event_count": len(events),
        "sources": sources,
        "events": events,
        "items": items,
        "issues": issues,
    }
    return view


def _render(
    request: Request,
    *,
    user: Any,
    view: dict[str, Any],
    status_code: int = status.HTTP_200_OK,
) -> HTMLResponse:
    return render_web_template(
        request,
        "traceability.html",
        user=user,
        context={"traceability_view": view},
        status_code=status_code,
    )


@router.get(
    "/traceability",
    response_class=HTMLResponse,
    include_in_schema=False,
    name="render_traceability_view",
)
async def render_traceability_view(
    request: Request,
    shipment_code: str | None = None,
) -> HTMLResponse:
    """Search one tenant shipment and render its reverse genealogy."""

    user, denied_response = get_html_route_user(
        request,
        required_permission=Permission.LOTE_READ,
    )
    if denied_response is not None:
        return denied_response

    normalized_code = (shipment_code or "").strip()
    if not normalized_code:
        return _render(
            request,
            user=user,
            view=build_traceability_view(query=""),
        )

    if len(normalized_code) > 120:
        return _render(
            request,
            user=user,
            view=build_traceability_view(
                query=normalized_code,
                error=_error(
                    code="SHIPMENT_CODE_TOO_LONG",
                    title="Código inválido",
                    message="El código de despacho no puede superar 120 caracteres.",
                ),
            ),
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        )

    try:
        session = get_tenant_scoped_db_session(user.organization_id)
    except Exception:
        session = None

    if session is None:
        return _render(
            request,
            user=user,
            view=build_traceability_view(
                query=normalized_code,
                error=_error(
                    code="TRACEABILITY_SERVICE_UNAVAILABLE",
                    title="Trazabilidad no disponible",
                    message="No fue posible consultar la genealogía en este momento.",
                ),
            ),
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    try:
        payload = TraceabilityLineageService(
            session=session,
            organization_id=user.organization_id,
        ).trace_shipment(normalized_code)
    except TraceabilityLineageNotFoundError as exc:
        return _render(
            request,
            user=user,
            view=build_traceability_view(
                query=normalized_code,
                error=_error(
                    code=exc.code,
                    title="Despacho no encontrado",
                    message=str(exc),
                ),
            ),
            status_code=status.HTTP_404_NOT_FOUND,
        )
    except TraceabilityLineageValidationError as exc:
        return _render(
            request,
            user=user,
            view=build_traceability_view(
                query=normalized_code,
                error=_error(
                    code=exc.code,
                    title="Consulta inválida",
                    message=str(exc),
                ),
            ),
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        )
    except Exception:
        try:
            session.rollback()
        except Exception:
            pass
        return _render(
            request,
            user=user,
            view=build_traceability_view(
                query=normalized_code,
                error=_error(
                    code="TRACEABILITY_QUERY_FAILED",
                    title="Consulta no disponible",
                    message="La genealogía no pudo reconstruirse en este momento.",
                ),
            ),
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    finally:
        try:
            session.close()
        except Exception:
            pass

    return _render(
        request,
        user=user,
        view=build_traceability_view(
            query=normalized_code,
            payload=payload,
        ),
    )
