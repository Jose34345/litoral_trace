"""Server-rendered local EUDR API V3 conformance workspace."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse

from litoral_trace.auth.rbac import Permission, has_permission
from litoral_trace.db.tenant import get_tenant_scoped_db_session, set_tenant_db_context
from litoral_trace.services.eudr_dds_candidate import (
    EudrDdsCandidateError,
    EudrDdsCandidateNotFoundError,
    EudrDdsCandidatePersistenceError,
    EudrDdsCandidateService,
    EudrDdsCandidateValidationError,
)
from litoral_trace.web.csrf import enforce_csrf, get_csrf_browser_nonce
from litoral_trace.web.runtime import get_html_route_user, render_csrf_failure, render_web_template


router = APIRouter(tags=["Frontend B2B"])

_RESULT_MESSAGES = {
    "saved": {
        "title": "Candidato EUDR actualizado",
        "message": (
            "Se guardaron los datos locales y se recalculó conformance. "
            "No se envió ninguna DDS a ACCEPTANCE ni a LIVE."
        ),
    }
}


def _safe_error(exc: Exception) -> dict[str, str]:
    if isinstance(exc, EudrDdsCandidateValidationError):
        return {"title": "Datos no válidos", "message": exc.detail}
    if isinstance(exc, EudrDdsCandidateNotFoundError):
        return {"title": "Despacho no encontrado", "message": str(exc)}
    if isinstance(exc, EudrDdsCandidatePersistenceError):
        return {
            "title": "Servicio no disponible",
            "message": "No fue posible guardar o consultar el candidato EUDR.",
        }
    return {
        "title": "No se pudo completar la operación",
        "message": "La solicitud fue rechazada de forma segura.",
    }


def _status_for_error(exc: Exception) -> int:
    if isinstance(exc, EudrDdsCandidateValidationError):
        return status.HTTP_422_UNPROCESSABLE_CONTENT
    if isinstance(exc, EudrDdsCandidateNotFoundError):
        return status.HTTP_404_NOT_FOUND
    if isinstance(exc, EudrDdsCandidatePersistenceError):
        return status.HTTP_503_SERVICE_UNAVAILABLE
    return status.HTTP_400_BAD_REQUEST


def _date(value: Any) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise EudrDdsCandidateValidationError(
            "INVALID_DATE",
            "Una fecha del candidato no tiene formato válido.",
        ) from exc


def _datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EudrDdsCandidateValidationError(
            "INVALID_DATETIME",
            "La fecha/hora de evaluación de riesgo no tiene formato válido.",
        ) from exc


def _view_payload(conformance) -> dict[str, Any]:
    candidate = conformance.candidate
    return {
        "shipment_code": conformance.shipment_code,
        "shipment_public_id": str(conformance.shipment_public_id),
        "state": conformance.state,
        "ready": conformance.ready,
        "missing": conformance.missing,
        "lineage_complete": conformance.lineage_complete,
        "requirements": tuple(
            {
                "key": item.key,
                "label": item.label,
                "satisfied": item.satisfied,
                "source": item.source,
                "detail": item.detail,
            }
            for item in conformance.requirements
        ),
        "plots": conformance.plots,
        "payload_sha256": conformance.payload_sha256,
        "target_environment": conformance.target_environment,
        "legal_effect": conformance.legal_effect,
        "candidate": (
            {
                "activity_type": candidate.activity_type,
                "commodity_profile": candidate.commodity_profile,
                "operator_name": candidate.operator_name or "",
                "operator_address": candidate.operator_address or "",
                "operator_country_code": candidate.operator_country_code or "",
                "operator_eori": candidate.operator_eori or "",
                "hs_code": candidate.hs_code or "",
                "trade_name": candidate.trade_name or "",
                "product_description": candidate.product_description or "",
                "common_species_name": candidate.common_species_name or "",
                "scientific_species_name": candidate.scientific_species_name or "",
                "net_mass_kg": candidate.net_mass_kg or "",
                "production_country_code": candidate.production_country_code or "",
                "production_date_from": (
                    candidate.production_date_from.isoformat()
                    if candidate.production_date_from
                    else ""
                ),
                "production_date_to": (
                    candidate.production_date_to.isoformat()
                    if candidate.production_date_to
                    else ""
                ),
                "relies_on_previous_dds": candidate.relies_on_previous_dds,
                "previous_dds_reference": candidate.previous_dds_reference or "",
                "previous_dds_verification": candidate.previous_dds_verification or "",
                "risk_conclusion": candidate.risk_conclusion,
                "risk_assessment_reference": candidate.risk_assessment_reference or "",
                "risk_assessed_at": (
                    candidate.risk_assessed_at.isoformat(timespec="minutes")
                    if candidate.risk_assessed_at
                    else ""
                ),
                "spec_profile": candidate.spec_profile,
                "spec_fingerprint_sha256": candidate.spec_fingerprint_sha256,
                "notes": candidate.notes or "",
            }
            if candidate is not None
            else None
        ),
    }


def _render(
    request: Request,
    *,
    user,
    query: str = "",
    conformance=None,
    error: dict[str, str] | None = None,
    result_code: str | None = None,
    status_code: int = status.HTTP_200_OK,
) -> HTMLResponse:
    return render_web_template(
        request,
        "eudr_dds_candidate.html",
        user=user,
        context={
            "eudr_query": query,
            "eudr_view": _view_payload(conformance) if conformance is not None else None,
            "eudr_error": error,
            "eudr_message": _RESULT_MESSAGES.get(result_code or ""),
            "eudr_can_manage": has_permission(user, Permission.TRACEABILITY_EVIDENCE),
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
    "/eudr-acceptance",
    response_class=HTMLResponse,
    include_in_schema=False,
    name="render_eudr_acceptance_candidate",
)
async def render_eudr_acceptance_candidate(
    request: Request,
    shipment_code: str | None = None,
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
            error=_safe_error(EudrDdsCandidatePersistenceError("db")),
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    try:
        conformance = EudrDdsCandidateService(
            session=session,
            organization_id=user.organization_id,
        ).conformance(code)
        return _render(
            request,
            user=user,
            query=code,
            conformance=conformance,
            result_code=request.query_params.get("result"),
        )
    except EudrDdsCandidateError as exc:
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
    "/eudr-acceptance",
    include_in_schema=False,
    name="update_eudr_acceptance_candidate",
)
async def update_eudr_acceptance_candidate(request: Request):
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
            error=_safe_error(EudrDdsCandidatePersistenceError("db")),
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    service = EudrDdsCandidateService(
        session=session,
        organization_id=user.organization_id,
    )
    try:
        service.upsert_candidate(
            shipment_code=code,
            activity_type=str(form.get("activity_type", "")),
            commodity_profile=str(form.get("commodity_profile", "")),
            operator_name=form.get("operator_name"),
            operator_address=form.get("operator_address"),
            operator_country_code=form.get("operator_country_code"),
            operator_eori=form.get("operator_eori"),
            hs_code=form.get("hs_code"),
            trade_name=form.get("trade_name"),
            product_description=form.get("product_description"),
            common_species_name=form.get("common_species_name"),
            scientific_species_name=form.get("scientific_species_name"),
            net_mass_kg=form.get("net_mass_kg"),
            production_country_code=form.get("production_country_code"),
            production_date_from=_date(form.get("production_date_from")),
            production_date_to=_date(form.get("production_date_to")),
            relies_on_previous_dds=str(form.get("relies_on_previous_dds", "")).lower()
            in {"1", "true", "on", "yes"},
            previous_dds_reference=form.get("previous_dds_reference"),
            previous_dds_verification=form.get("previous_dds_verification"),
            risk_conclusion=str(form.get("risk_conclusion", "UNASSESSED")),
            risk_assessment_reference=form.get("risk_assessment_reference"),
            risk_assessed_at=_datetime(form.get("risk_assessed_at")),
            notes=form.get("notes"),
            actor_user_id=user.user_id,
        )
        set_tenant_db_context(session, user.organization_id)
        service.conformance(code)
        return RedirectResponse(
            url=f"/eudr-acceptance?{urlencode({'shipment_code': code, 'result': 'saved'})}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    except EudrDdsCandidateError as exc:
        try:
            session.rollback()
            set_tenant_db_context(session, user.organization_id)
            conformance = service.conformance(code)
        except Exception:
            conformance = None
        return _render(
            request,
            user=user,
            query=code,
            conformance=conformance,
            error=_safe_error(exc),
            status_code=_status_for_error(exc),
        )
    finally:
        session.close()
