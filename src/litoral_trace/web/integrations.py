"""Server-rendered P1-A workspace for integration staging and reconciliation."""
from __future__ import annotations

import json
from urllib.parse import urlencode
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import ValidationError

from litoral_trace.auth.rbac import Permission, has_permission
from litoral_trace.db.tenant import get_tenant_scoped_db_session
from litoral_trace.services.integrations.canonical import GenericErpPayload
from litoral_trace.services.integrations.core import (
    IntegrationConflictError,
    IntegrationCoreService,
    IntegrationError,
    IntegrationNotFoundError,
    IntegrationPersistenceError,
    IntegrationValidationError,
)
from litoral_trace.web.csrf import enforce_csrf, get_csrf_browser_nonce
from litoral_trace.web.runtime import (
    get_html_route_user,
    render_csrf_failure,
    render_web_template,
)


router = APIRouter(tags=["Frontend B2B"])

_RESULT_MESSAGES = {
    "connection-created": ("Conexión creada", "El ERP quedó configurado para staging. No se guardaron credenciales en la base."),
    "sync-complete": ("Sincronización recibida", "El payload fue almacenado en staging; ningún movimiento modificó stock o ledger."),
    "reconciled": ("Entidad reconciliada", "La referencia externa quedó vinculada explícitamente a un objeto de Litoral Trace."),
}

_SAMPLE_PAYLOAD = {
    "source_system": "ERP_CLIENTE",
    "suppliers": [
        {
            "external_id": "SUP-001",
            "name": "Proveedor Forestal Demo",
            "tax_id": "30-00000000-0",
            "country": "AR",
            "metadata": {},
        }
    ],
    "products": [
        {
            "external_id": "PROD-PINO",
            "code": "PINO-ASERRADO",
            "name": "Pino aserrado",
            "unit": "M3",
            "metadata": {},
        }
    ],
    "receipts": [],
    "shipments": [],
}


def _safe_error(exc: Exception) -> dict[str, str]:
    if isinstance(exc, IntegrationValidationError):
        return {"title": "Datos de integración no válidos", "message": exc.detail}
    if isinstance(exc, IntegrationNotFoundError):
        return {"title": "Referencia no encontrada", "message": str(exc)}
    if isinstance(exc, IntegrationConflictError):
        return {"title": "Integración en conflicto", "message": str(exc)}
    if isinstance(exc, IntegrationPersistenceError):
        return {
            "title": "Servicio no disponible",
            "message": "No fue posible persistir la integración en este momento.",
        }
    return {"title": "No se pudo completar la operación", "message": "La solicitud fue rechazada de forma segura."}


def _status_for_error(exc: Exception) -> int:
    if isinstance(exc, IntegrationValidationError):
        return status.HTTP_422_UNPROCESSABLE_CONTENT
    if isinstance(exc, IntegrationNotFoundError):
        return status.HTTP_404_NOT_FOUND
    if isinstance(exc, IntegrationConflictError):
        return status.HTTP_409_CONFLICT
    if isinstance(exc, IntegrationPersistenceError):
        return status.HTTP_503_SERVICE_UNAVAILABLE
    return status.HTTP_400_BAD_REQUEST


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


def _render(
    request: Request,
    *,
    user,
    result_code: str | None = None,
    error: dict[str, str] | None = None,
    status_code: int = 200,
    payload_text: str | None = None,
) -> HTMLResponse:
    session = get_tenant_scoped_db_session(user.organization_id)
    snapshot = None
    if session is None:
        error = error or {
            "title": "Servicio no disponible",
            "message": "No fue posible consultar las integraciones.",
        }
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    else:
        try:
            snapshot = IntegrationCoreService(
                session=session,
                organization_id=user.organization_id,
            ).snapshot()
        except IntegrationError as exc:
            error = error or _safe_error(exc)
            status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        finally:
            session.close()

    references = {}
    if snapshot is not None:
        references = {row.external_entity_id: row for row in snapshot.references}

    message = None
    if result_code in _RESULT_MESSAGES:
        title, text = _RESULT_MESSAGES[result_code]
        message = {"title": title, "message": text}

    entities = []
    if snapshot is not None:
        for row in snapshot.entities:
            normalized = row.normalized_json or {}
            label = (
                normalized.get("name")
                or normalized.get("shipment_code")
                or normalized.get("receipt_code")
                or normalized.get("code")
                or row.external_id
            )
            ref = references.get(row.id)
            entities.append(
                {
                    "public_id": str(row.public_id),
                    "entity_type": row.entity_type,
                    "external_id": row.external_id,
                    "label": str(label),
                    "status": row.status,
                    "conflict_reason": row.conflict_reason,
                    "target_type": ref.target_type if ref else None,
                    "target_reference": ref.target_reference if ref else None,
                }
            )

    return render_web_template(
        request,
        "integrations.html",
        user=user,
        context={
            "integration_view": {
                "connections": snapshot.connections if snapshot else (),
                "sync_runs": snapshot.sync_runs if snapshot else (),
                "entities": tuple(entities),
                "can_manage": has_permission(user, Permission.INTEGRATION_MANAGE),
                "message": message,
                "error": error,
                "payload_text": payload_text or json.dumps(_SAMPLE_PAYLOAD, ensure_ascii=False, indent=2),
                "idempotency_key": f"web-p1a-{uuid4().hex}",
            }
        },
        status_code=status_code,
    )


def _redirect(code: str) -> RedirectResponse:
    return RedirectResponse(
        url=f"/integrations?{urlencode({'result': code})}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.get("/integrations", response_class=HTMLResponse, include_in_schema=False)
async def integrations_workspace(request: Request) -> HTMLResponse:
    user, denied = get_html_route_user(
        request,
        required_permission=Permission.INTEGRATION_READ,
    )
    if denied is not None:
        return denied
    return _render(
        request,
        user=user,
        result_code=request.query_params.get("result"),
    )


@router.post("/integrations/connections", include_in_schema=False)
async def create_integration_connection(request: Request):
    user, denied = get_html_route_user(
        request,
        required_permission=Permission.INTEGRATION_MANAGE,
    )
    if denied is not None:
        return denied
    csrf_response = await _csrf_or_response(request, user)
    if csrf_response is not None:
        return csrf_response
    form = await request.form()
    session = get_tenant_scoped_db_session(user.organization_id)
    if session is None:
        return _render(request, user=user, error=_safe_error(IntegrationPersistenceError("db")), status_code=503)
    try:
        service = IntegrationCoreService(session=session, organization_id=user.organization_id)
        service.create_connection(
            name=str(form.get("name", "")),
            connector_type="GENERIC_ERP",
            secret_ref=str(form.get("secret_ref", "")) or None,
            config_json={"mode": "staging_only"},
        )
        return _redirect("connection-created")
    except IntegrationError as exc:
        return _render(request, user=user, error=_safe_error(exc), status_code=_status_for_error(exc))
    finally:
        session.close()


@router.post("/integrations/sync-json", include_in_schema=False)
async def stage_integration_json(request: Request):
    user, denied = get_html_route_user(
        request,
        required_permission=Permission.INTEGRATION_MANAGE,
    )
    if denied is not None:
        return denied
    csrf_response = await _csrf_or_response(request, user)
    if csrf_response is not None:
        return csrf_response
    form = await request.form()
    payload_text = str(form.get("payload_json", ""))
    try:
        connection_public_id = UUID(str(form.get("connection_public_id", "")).strip())
        payload = GenericErpPayload.model_validate_json(payload_text)
    except (ValueError, ValidationError) as exc:
        error = {
            "title": "JSON ERP no válido",
            "message": "El payload no cumple el contrato canónico de P1-A.",
        }
        return _render(request, user=user, error=error, status_code=422, payload_text=payload_text)

    session = get_tenant_scoped_db_session(user.organization_id)
    if session is None:
        return _render(request, user=user, error=_safe_error(IntegrationPersistenceError("db")), status_code=503, payload_text=payload_text)
    try:
        service = IntegrationCoreService(session=session, organization_id=user.organization_id)
        service.stage_generic_erp(
            connection_public_id=connection_public_id,
            payload=payload,
            idempotency_key=str(form.get("idempotency_key", "")),
        )
        return _redirect("sync-complete")
    except IntegrationError as exc:
        return _render(
            request,
            user=user,
            error=_safe_error(exc),
            status_code=_status_for_error(exc),
            payload_text=payload_text,
        )
    finally:
        session.close()


@router.post("/integrations/entities/{entity_public_id}/reconcile", include_in_schema=False)
async def reconcile_integration_entity(request: Request, entity_public_id: UUID):
    user, denied = get_html_route_user(
        request,
        required_permission=Permission.INTEGRATION_MANAGE,
    )
    if denied is not None:
        return denied
    csrf_response = await _csrf_or_response(request, user)
    if csrf_response is not None:
        return csrf_response
    form = await request.form()
    session = get_tenant_scoped_db_session(user.organization_id)
    if session is None:
        return _render(request, user=user, error=_safe_error(IntegrationPersistenceError("db")), status_code=503)
    try:
        IntegrationCoreService(
            session=session,
            organization_id=user.organization_id,
        ).reconcile_entity(
            entity_public_id=entity_public_id,
            target_type=str(form.get("target_type", "")),
            target_reference=str(form.get("target_reference", "")),
            user_id=user.user_id,
        )
        return _redirect("reconciled")
    except IntegrationError as exc:
        return _render(request, user=user, error=_safe_error(exc), status_code=_status_for_error(exc))
    finally:
        session.close()
