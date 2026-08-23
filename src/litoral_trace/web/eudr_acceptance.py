"""Server-rendered EUDR API V3 ACCEPTANCE transport workspace.

This browser surface is intentionally non-legal and exposes no credential
material. It never offers an automatic retry for SENT or TRANSPORT_ERROR.
"""
from __future__ import annotations

from typing import Any
from urllib.parse import urlencode
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select

from litoral_trace.auth.rbac import Permission, has_permission
from litoral_trace.config.eudr_acceptance import get_eudr_acceptance_settings
from litoral_trace.db.models import EudrAcceptanceAttempt, EudrDdsCandidate
from litoral_trace.db.tenant import get_tenant_scoped_db_session
from litoral_trace.services.eudr_acceptance_submission import (
    EudrAcceptanceAttemptView,
    EudrAcceptanceSubmissionError,
    EudrAcceptanceSubmissionPersistenceError,
    EudrAcceptanceSubmissionService,
)
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
    "prepared": {
        "title": "Solicitud ACCEPTANCE preparada",
        "message": "Se guardó el fingerprint del request V3. Todavía no se realizó ninguna llamada de red.",
    },
    "accepted": {
        "title": "ACCEPTANCE recibió la solicitud",
        "message": "El resultado pertenece al entorno de prueba y no tiene efecto legal EUDR.",
    },
    "rejected": {
        "title": "ACCEPTANCE rechazó la solicitud",
        "message": "El rechazo remoto quedó auditado. Corregí los datos antes de preparar una nueva solicitud.",
    },
    "transport_error": {
        "title": "Resultado de entrega incierto",
        "message": "No se reintentará automáticamente: primero debe reconciliarse el estado remoto para evitar duplicados.",
    },
}


def _attempt_payload(row: EudrAcceptanceAttemptView | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "public_id": str(row.public_id),
        "state": row.state,
        "environment": row.environment,
        "operation": row.operation,
        "operator_role": row.operator_role,
        "country_of_activity": row.country_of_activity,
        "border_cross_country": row.border_cross_country,
        "internal_reference_number": row.internal_reference_number,
        "candidate_payload_sha256": row.candidate_payload_sha256,
        "wire_contract_profile": row.wire_contract_profile,
        "wire_contract_sha256": row.wire_contract_sha256,
        "request_body_sha256": row.request_body_sha256,
        "envelope_sha256": row.envelope_sha256,
        "response_sha256": row.response_sha256,
        "remote_uuid": row.remote_uuid,
        "remote_status": row.remote_status,
        "http_status": row.http_status,
        "error_code": row.error_code,
        "error_summary": row.error_summary,
        "sent_at": row.sent_at.isoformat() if row.sent_at else None,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
        "legal_effect": row.legal_effect,
    }


def _conformance_payload(row) -> dict[str, Any]:
    return {
        "shipment_code": row.shipment_code,
        "state": row.state,
        "ready": row.ready,
        "missing": tuple(row.missing),
        "payload_sha256": row.payload_sha256,
        "candidate_public_id": str(row.candidate.public_id) if row.candidate else None,
    }


def _safe_error(exc: Exception) -> dict[str, str]:
    if isinstance(exc, EudrAcceptanceSubmissionError):
        return {"title": "ACCEPTANCE bloqueado", "message": exc.detail}
    if isinstance(exc, EudrDdsCandidateValidationError):
        return {"title": "Datos EUDR no válidos", "message": exc.detail}
    if isinstance(exc, EudrDdsCandidateNotFoundError):
        return {"title": "Despacho no encontrado", "message": str(exc)}
    if isinstance(exc, (EudrAcceptanceSubmissionPersistenceError, EudrDdsCandidatePersistenceError)):
        return {
            "title": "Servicio temporalmente no disponible",
            "message": "No fue posible consultar o actualizar el estado ACCEPTANCE.",
        }
    return {
        "title": "Operación rechazada de forma segura",
        "message": "No fue posible completar la operación ACCEPTANCE.",
    }


def _status_for_error(exc: Exception) -> int:
    if isinstance(exc, (EudrAcceptanceSubmissionPersistenceError, EudrDdsCandidatePersistenceError)):
        return status.HTTP_503_SERVICE_UNAVAILABLE
    if isinstance(exc, EudrDdsCandidateNotFoundError):
        return status.HTTP_404_NOT_FOUND
    if isinstance(exc, EudrAcceptanceSubmissionError):
        if exc.code in {
            "ACCEPTANCE_NETWORK_NOT_READY",
            "ACCEPTANCE_DELIVERY_UNCERTAIN",
            "ACCEPTANCE_RETRY_REQUIRES_EXPLICIT_OVERRIDE",
            "ACCEPTANCE_ATTEMPT_NOT_SENDABLE",
            "ACCEPTANCE_PREPARED_PAYLOAD_STALE",
        }:
            return status.HTTP_409_CONFLICT
        return status.HTTP_422_UNPROCESSABLE_CONTENT
    if isinstance(exc, EudrDdsCandidateValidationError):
        return status.HTTP_422_UNPROCESSABLE_CONTENT
    return status.HTTP_400_BAD_REQUEST


def _network_ready() -> bool:
    try:
        return get_eudr_acceptance_settings().network_ready
    except (RuntimeError, ValueError):
        return False


def _latest_attempt(session, *, organization_id: int, candidate_public_id: str | None):
    if not candidate_public_id:
        return None
    candidate = session.scalar(
        select(EudrDdsCandidate).where(
            EudrDdsCandidate.organization_id == organization_id,
            EudrDdsCandidate.public_id == UUID(candidate_public_id),
        )
    )
    if candidate is None:
        return None
    return session.scalar(
        select(EudrAcceptanceAttempt)
        .where(
            EudrAcceptanceAttempt.organization_id == organization_id,
            EudrAcceptanceAttempt.candidate_id == candidate.id,
        )
        .order_by(EudrAcceptanceAttempt.created_at.desc(), EudrAcceptanceAttempt.id.desc())
        .limit(1)
    )


def _render(
    request: Request,
    *,
    user,
    query: str = "",
    conformance=None,
    attempt: EudrAcceptanceAttemptView | None = None,
    error: dict[str, str] | None = None,
    result_code: str | None = None,
    status_code: int = status.HTTP_200_OK,
) -> HTMLResponse:
    conformance_payload = _conformance_payload(conformance) if conformance is not None else None
    attempt_payload = _attempt_payload(attempt)
    stale = bool(
        conformance_payload
        and attempt_payload
        and conformance_payload.get("payload_sha256")
        and attempt_payload.get("candidate_payload_sha256") != conformance_payload.get("payload_sha256")
    )
    return render_web_template(
        request,
        "eudr_acceptance_transport.html",
        user=user,
        context={
            "acceptance_query": query,
            "acceptance_conformance": conformance_payload,
            "acceptance_attempt": attempt_payload,
            "acceptance_attempt_stale": stale,
            "acceptance_network_ready": _network_ready(),
            "acceptance_can_manage": has_permission(user, Permission.TRACEABILITY_EVIDENCE),
            "acceptance_error": error,
            "acceptance_message": _RESULT_MESSAGES.get(result_code or ""),
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


def _load_workspace(session, *, organization_id: int, shipment_code: str):
    conformance = EudrDdsCandidateService(
        session=session,
        organization_id=organization_id,
    ).conformance(shipment_code)
    candidate_public_id = str(conformance.candidate.public_id) if conformance.candidate else None
    attempt_row = _latest_attempt(
        session,
        organization_id=organization_id,
        candidate_public_id=candidate_public_id,
    )
    attempt = None
    if attempt_row is not None:
        attempt = EudrAcceptanceSubmissionService(
            session=session,
            organization_id=organization_id,
        ).get_attempt(attempt_row.public_id)
    return conformance, attempt


@router.get(
    "/eudr-acceptance/transport",
    response_class=HTMLResponse,
    include_in_schema=False,
    name="render_eudr_acceptance_transport",
)
async def render_eudr_acceptance_transport(
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
            error={"title": "Servicio no disponible", "message": "La base de datos no está disponible temporalmente."},
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    try:
        conformance, attempt = _load_workspace(
            session,
            organization_id=user.organization_id,
            shipment_code=code,
        )
        return _render(
            request,
            user=user,
            query=code,
            conformance=conformance,
            attempt=attempt,
            result_code=request.query_params.get("result"),
        )
    except (EudrDdsCandidateError, EudrAcceptanceSubmissionError) as exc:
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
    "/eudr-acceptance/transport/prepare",
    include_in_schema=False,
    name="prepare_eudr_acceptance_transport",
)
async def prepare_eudr_acceptance_transport(request: Request):
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
            error={"title": "Servicio no disponible", "message": "La base de datos no está disponible temporalmente."},
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    try:
        EudrAcceptanceSubmissionService(
            session=session,
            organization_id=user.organization_id,
        ).prepare(
            shipment_code=code,
            operator_role="OPERATOR",
            country_of_activity=str(form.get("country_of_activity", "")),
            border_cross_country=form.get("border_cross_country"),
            internal_reference_number=form.get("internal_reference_number"),
            geo_location_confidential=False,
            actor_user_id=user.user_id,
        )
        return RedirectResponse(
            url=f"/eudr-acceptance/transport?{urlencode({'shipment_code': code, 'result': 'prepared'})}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    except (EudrDdsCandidateError, EudrAcceptanceSubmissionError) as exc:
        try:
            session.rollback()
            from litoral_trace.db.tenant import set_tenant_db_context

            set_tenant_db_context(session, user.organization_id)
            conformance, attempt = _load_workspace(
                session,
                organization_id=user.organization_id,
                shipment_code=code,
            )
        except Exception:
            conformance = None
            attempt = None
        return _render(
            request,
            user=user,
            query=code,
            conformance=conformance,
            attempt=attempt,
            error=_safe_error(exc),
            status_code=_status_for_error(exc),
        )
    finally:
        session.close()


@router.post(
    "/eudr-acceptance/transport/submit",
    include_in_schema=False,
    name="submit_eudr_acceptance_transport",
)
async def submit_eudr_acceptance_transport(request: Request):
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
    raw_attempt = str(form.get("attempt_public_id", "")).strip()
    try:
        attempt_public_id = UUID(raw_attempt)
    except ValueError:
        return _render(
            request,
            user=user,
            query=code,
            error={"title": "Intento no válido", "message": "El identificador del intento ACCEPTANCE no es válido."},
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        )

    session = get_tenant_scoped_db_session(user.organization_id)
    if session is None:
        return _render(
            request,
            user=user,
            query=code,
            error={"title": "Servicio no disponible", "message": "La base de datos no está disponible temporalmente."},
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    try:
        result = EudrAcceptanceSubmissionService(
            session=session,
            organization_id=user.organization_id,
        ).submit(
            attempt_public_id=attempt_public_id,
            shipment_code=code,
            actor_user_id=user.user_id,
            allow_retry_after_transport_error=False,
        )
        result_code = {
            "REMOTE_ACCEPTED": "accepted",
            "REMOTE_REJECTED": "rejected",
            "TRANSPORT_ERROR": "transport_error",
        }.get(result.state, "transport_error")
        return RedirectResponse(
            url=f"/eudr-acceptance/transport?{urlencode({'shipment_code': code, 'result': result_code})}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    except (EudrDdsCandidateError, EudrAcceptanceSubmissionError) as exc:
        try:
            session.rollback()
            from litoral_trace.db.tenant import set_tenant_db_context

            set_tenant_db_context(session, user.organization_id)
            conformance, attempt = _load_workspace(
                session,
                organization_id=user.organization_id,
                shipment_code=code,
            )
        except Exception:
            conformance = None
            attempt = None
        return _render(
            request,
            user=user,
            query=code,
            conformance=conformance,
            attempt=attempt,
            error=_safe_error(exc),
            status_code=_status_for_error(exc),
        )
    finally:
        session.close()
