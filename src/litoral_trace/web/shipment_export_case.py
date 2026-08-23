"""Server-rendered Corrientes + ARCA export-case workspace."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse

from litoral_trace.auth.rbac import Permission, has_permission
from litoral_trace.db.tenant import get_tenant_scoped_db_session, set_tenant_db_context
from litoral_trace.services.shipment_export_case import (
    ShipmentExportCaseError,
    ShipmentExportCaseNotFoundError,
    ShipmentExportCasePersistenceError,
    ShipmentExportCaseService,
    ShipmentExportCaseValidationError,
)
from litoral_trace.web.csrf import enforce_csrf, get_csrf_browser_nonce
from litoral_trace.web.runtime import (
    get_html_route_user,
    render_csrf_failure,
    render_web_template,
)


router = APIRouter(tags=["Frontend B2B"])

_RESULT_MESSAGES = {
    "saved": {
        "title": "Expediente actualizado",
        "message": "Las referencias Corrientes/ARCA/SIM quedaron asociadas al despacho. El ledger de trazabilidad no fue modificado.",
    }
}


def _safe_error(exc: Exception) -> dict[str, str]:
    if isinstance(exc, ShipmentExportCaseValidationError):
        return {"title": "Datos no válidos", "message": exc.detail}
    if isinstance(exc, ShipmentExportCaseNotFoundError):
        return {"title": "Despacho no encontrado", "message": str(exc)}
    if isinstance(exc, ShipmentExportCasePersistenceError):
        return {
            "title": "Servicio no disponible",
            "message": "No fue posible guardar o consultar el expediente exportador en este momento.",
        }
    return {
        "title": "No se pudo completar la operación",
        "message": "La solicitud fue rechazada de forma segura.",
    }


def _status_for_error(exc: Exception) -> int:
    if isinstance(exc, ShipmentExportCaseValidationError):
        return status.HTTP_422_UNPROCESSABLE_CONTENT
    if isinstance(exc, ShipmentExportCaseNotFoundError):
        return status.HTTP_404_NOT_FOUND
    if isinstance(exc, ShipmentExportCasePersistenceError):
        return status.HTTP_503_SERVICE_UNAVAILABLE
    return status.HTTP_400_BAD_REQUEST


def _parse_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ShipmentExportCaseValidationError(
            "INVALID_CUSTOMS_OFFICIALIZED_AT",
            "La fecha de oficialización aduanera no tiene un formato válido.",
        ) from exc


def _view_payload(readiness) -> dict[str, Any]:
    case = readiness.export_case
    return {
        "shipment_code": readiness.shipment_code,
        "shipment_public_id": str(readiness.shipment_public_id),
        "state": readiness.state,
        "ready": readiness.ready,
        "origin_profile": readiness.origin_profile,
        "requirements": tuple(
            {
                "key": row.key,
                "label": row.label,
                "satisfied": row.satisfied,
                "source": row.source,
            }
            for row in readiness.requirements
        ),
        "missing": readiness.missing,
        "evidence_types": readiness.evidence_types,
        "case": (
            {
                "origin_profile": case.origin_profile,
                "export_invoice_number": case.export_invoice_number or "",
                "export_invoice_cae": case.export_invoice_cae or "",
                "customs_destination_id": case.customs_destination_id or "",
                "customs_subregime": case.customs_subregime or "",
                "customs_officialized_at": (
                    case.customs_officialized_at.isoformat(timespec="minutes")
                    if case.customs_officialized_at
                    else ""
                ),
                "notes": case.notes or "",
            }
            if case is not None
            else None
        ),
    }


def _render(
    request: Request,
    *,
    user,
    query: str = "",
    readiness=None,
    error: dict[str, str] | None = None,
    result_code: str | None = None,
    status_code: int = status.HTTP_200_OK,
) -> HTMLResponse:
    return render_web_template(
        request,
        "shipment_export_case.html",
        user=user,
        context={
            "export_case_query": query,
            "export_case_view": _view_payload(readiness) if readiness is not None else None,
            "export_case_error": error,
            "export_case_message": _RESULT_MESSAGES.get(result_code or ""),
            "export_case_can_manage": has_permission(
                user, Permission.TRACEABILITY_EVIDENCE
            ),
        },
        status_code=status_code,
    )


async def _csrf_or_response(request: Request, user):
    try:
        await enforce_csrf(
            request,
            user=user,
            browser_nonce=get_csrf_browser_nonce(request),
            require_browser_binding=True,
        )
    except HTTPException:
        return render_csrf_failure()
    return None


@router.get(
    "/export-case",
    response_class=HTMLResponse,
    include_in_schema=False,
    name="render_shipment_export_case",
)
async def render_shipment_export_case(
    request: Request,
    shipment_code: str | None = None,
) -> HTMLResponse:
    user, denied = get_html_route_user(
        request,
        required_permission=Permission.LOTE_READ,
    )
    if denied is not None:
        return denied

    code = str(shipment_code or "").strip()
    if not code:
        return _render(request, user=user)

    session = get_tenant_scoped_db_session(user.organization_id)
    if session is None:
        return _render(
            request,
            user=user,
            query=code,
            error=_safe_error(ShipmentExportCasePersistenceError("db")),
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    try:
        readiness = ShipmentExportCaseService(
            session=session,
            organization_id=user.organization_id,
        ).readiness(code)
        return _render(
            request,
            user=user,
            query=code,
            readiness=readiness,
            result_code=request.query_params.get("result"),
        )
    except ShipmentExportCaseError as exc:
        return _render(
            request,
            user=user,
            query=code,
            error=_safe_error(exc),
            status_code=_status_for_error(exc),
        )
    finally:
        session.close()


@router.post(
    "/export-case",
    include_in_schema=False,
    name="update_shipment_export_case",
)
async def update_shipment_export_case(request: Request):
    user, denied = get_html_route_user(
        request,
        required_permission=Permission.TRACEABILITY_EVIDENCE,
    )
    if denied is not None:
        return denied
    csrf_response = await _csrf_or_response(request, user)
    if csrf_response is not None:
        return csrf_response

    form = await request.form()
    code = str(form.get("shipment_code", "")).strip()
    session = get_tenant_scoped_db_session(user.organization_id)
    if session is None:
        return _render(
            request,
            user=user,
            query=code,
            error=_safe_error(ShipmentExportCasePersistenceError("db")),
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    try:
        service = ShipmentExportCaseService(
            session=session,
            organization_id=user.organization_id,
        )
        service.upsert_case(
            shipment_code=code,
            origin_profile=str(form.get("origin_profile", "")),
            export_invoice_number=form.get("export_invoice_number"),
            export_invoice_cae=form.get("export_invoice_cae"),
            customs_destination_id=form.get("customs_destination_id"),
            customs_subregime=form.get("customs_subregime"),
            customs_officialized_at=_parse_datetime(
                form.get("customs_officialized_at")
            ),
            notes=form.get("notes"),
            actor_user_id=user.user_id,
        )
        set_tenant_db_context(session, user.organization_id)
        # Evaluate immediately after the write so validation errors remain visible
        # before redirecting. This query never changes ledger/stock.
        service.readiness(code)
        return RedirectResponse(
            url=f"/export-case?{urlencode({'shipment_code': code, 'result': 'saved'})}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    except ShipmentExportCaseError as exc:
        try:
            session.rollback()
        except Exception:
            pass
        try:
            set_tenant_db_context(session, user.organization_id)
            readiness = service.readiness(code)
        except Exception:
            readiness = None
        return _render(
            request,
            user=user,
            query=code,
            readiness=readiness,
            error=_safe_error(exc),
            status_code=_status_for_error(exc),
        )
    finally:
        session.close()
