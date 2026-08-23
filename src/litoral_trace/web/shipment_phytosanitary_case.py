"""Server-rendered SENASA/CERT-POV/ePhyto shipment workspace."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse

from litoral_trace.auth.rbac import Permission, has_permission
from litoral_trace.db.tenant import get_tenant_scoped_db_session, set_tenant_db_context
from litoral_trace.services.shipment_phytosanitary_case import (
    ShipmentPhytosanitaryCaseError,
    ShipmentPhytosanitaryCaseNotFoundError,
    ShipmentPhytosanitaryCasePersistenceError,
    ShipmentPhytosanitaryCaseService,
    ShipmentPhytosanitaryCaseValidationError,
)
from litoral_trace.web.csrf import enforce_csrf, get_csrf_browser_nonce
from litoral_trace.web.runtime import get_html_route_user, render_csrf_failure, render_web_template


router = APIRouter(tags=["Frontend B2B"])

_RESULT_MESSAGES = {
    "saved": {
        "title": "Evaluación fitosanitaria actualizada",
        "message": "Las referencias SENASA/CERT-POV/ePhyto quedaron asociadas al despacho. Litoral Trace no emitió ni modificó ningún certificado oficial.",
    }
}


def _safe_error(exc: Exception) -> dict[str, str]:
    if isinstance(exc, ShipmentPhytosanitaryCaseValidationError):
        return {"title": "Datos no válidos", "message": exc.detail}
    if isinstance(exc, ShipmentPhytosanitaryCaseNotFoundError):
        return {"title": "Despacho no encontrado", "message": str(exc)}
    if isinstance(exc, ShipmentPhytosanitaryCasePersistenceError):
        return {
            "title": "Servicio no disponible",
            "message": "No fue posible guardar o consultar la evaluación fitosanitaria en este momento.",
        }
    return {
        "title": "No se pudo completar la operación",
        "message": "La solicitud fue rechazada de forma segura.",
    }


def _status_for_error(exc: Exception) -> int:
    if isinstance(exc, ShipmentPhytosanitaryCaseValidationError):
        return status.HTTP_422_UNPROCESSABLE_CONTENT
    if isinstance(exc, ShipmentPhytosanitaryCaseNotFoundError):
        return status.HTTP_404_NOT_FOUND
    if isinstance(exc, ShipmentPhytosanitaryCasePersistenceError):
        return status.HTTP_503_SERVICE_UNAVAILABLE
    return status.HTTP_400_BAD_REQUEST


def _parse_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ShipmentPhytosanitaryCaseValidationError(
            "INVALID_REQUIREMENTS_CHECKED_AT",
            "La fecha de evaluación de requisitos no tiene un formato válido.",
        ) from exc


def _view_payload(readiness) -> dict[str, Any]:
    case = readiness.phytosanitary_case
    return {
        "shipment_code": readiness.shipment_code,
        "shipment_public_id": str(readiness.shipment_public_id),
        "state": readiness.state,
        "ready": readiness.ready,
        "certification_mode": readiness.certification_mode,
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
        "evidence_subject": f"SHIPMENT|{readiness.shipment_public_id}",
        "case": (
            {
                "certification_mode": case.certification_mode,
                "requirements_reference": case.requirements_reference or "",
                "requirements_checked_at": (
                    case.requirements_checked_at.isoformat(timespec="minutes")
                    if case.requirements_checked_at
                    else ""
                ),
                "cert_pov_reference": case.cert_pov_reference or "",
                "certificate_number": case.certificate_number or "",
                "ephyto_reference": case.ephyto_reference or "",
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
        "shipment_phytosanitary_case.html",
        user=user,
        context={
            "phytosanitary_query": query,
            "phytosanitary_view": _view_payload(readiness) if readiness is not None else None,
            "phytosanitary_error": error,
            "phytosanitary_message": _RESULT_MESSAGES.get(result_code or ""),
            "phytosanitary_can_manage": has_permission(
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
    "/phytosanitary-case",
    response_class=HTMLResponse,
    include_in_schema=False,
    name="render_shipment_phytosanitary_case",
)
async def render_shipment_phytosanitary_case(
    request: Request, shipment_code: str | None = None
) -> HTMLResponse:
    user, denied = get_html_route_user(request, required_permission=Permission.LOTE_READ)
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
            error=_safe_error(ShipmentPhytosanitaryCasePersistenceError("db")),
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    try:
        readiness = ShipmentPhytosanitaryCaseService(
            session=session, organization_id=user.organization_id
        ).readiness(code)
        return _render(
            request,
            user=user,
            query=code,
            readiness=readiness,
            result_code=request.query_params.get("result"),
        )
    except ShipmentPhytosanitaryCaseError as exc:
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
    "/phytosanitary-case",
    include_in_schema=False,
    name="update_shipment_phytosanitary_case",
)
async def update_shipment_phytosanitary_case(request: Request):
    user, denied = get_html_route_user(
        request, required_permission=Permission.TRACEABILITY_EVIDENCE
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
            error=_safe_error(ShipmentPhytosanitaryCasePersistenceError("db")),
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    service = ShipmentPhytosanitaryCaseService(
        session=session, organization_id=user.organization_id
    )
    try:
        service.upsert_case(
            shipment_code=code,
            certification_mode=str(form.get("certification_mode", "")),
            requirements_reference=form.get("requirements_reference"),
            requirements_checked_at=_parse_datetime(form.get("requirements_checked_at")),
            cert_pov_reference=form.get("cert_pov_reference"),
            certificate_number=form.get("certificate_number"),
            ephyto_reference=form.get("ephyto_reference"),
            notes=form.get("notes"),
            actor_user_id=user.user_id,
        )
        set_tenant_db_context(session, user.organization_id)
        service.readiness(code)
        return RedirectResponse(
            url=f"/phytosanitary-case?{urlencode({'shipment_code': code, 'result': 'saved'})}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    except ShipmentPhytosanitaryCaseError as exc:
        try:
            session.rollback()
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
