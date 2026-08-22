"""Interactive server-rendered Control de Salida Litoral Trace."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request, status
from fastapi.responses import HTMLResponse

from litoral_trace.auth.rbac import Permission
from litoral_trace.db.tenant import get_tenant_scoped_db_session
from litoral_trace.services.traceability_documentary_dossier import (
    build_documentary_dossier_bundle,
)
from litoral_trace.services.traceability_dossier import (
    OriginDossierError,
)
from litoral_trace.services.traceability_evidence_dossier import (
    project_documentary_evidence,
)
from litoral_trace.services.traceability_lineage import (
    TraceabilityLineageNotFoundError,
    TraceabilityLineageService,
    TraceabilityLineageValidationError,
)
from litoral_trace.services.traceability_release_control import (
    build_release_control_view,
)
from litoral_trace.web.runtime import (
    get_html_route_user,
    render_web_template,
)


router = APIRouter(tags=["Frontend B2B"])


def _render(
    request: Request,
    *,
    user: Any,
    query: str,
    control: dict[str, Any] | None = None,
    error: dict[str, str] | None = None,
    status_code: int = status.HTTP_200_OK,
) -> HTMLResponse:
    return render_web_template(
        request,
        "traceability_release_control.html",
        user=user,
        context={
            "release_query": query,
            "release_control": control,
            "release_error": error,
        },
        status_code=status_code,
    )


def _error(code: str, title: str, message: str) -> dict[str, str]:
    return {"code": code, "title": title, "message": message}


@router.get(
    "/release-control",
    response_class=HTMLResponse,
    include_in_schema=False,
    name="render_release_control",
)
async def render_release_control(
    request: Request,
    shipment_code: str | None = None,
) -> HTMLResponse:
    """Evaluate one shipment using factual operational release controls."""

    user, denied_response = get_html_route_user(
        request,
        required_permission=Permission.LOTE_READ,
    )
    if denied_response is not None:
        return denied_response

    normalized_code = (shipment_code or "").strip()
    if not normalized_code:
        return _render(request, user=user, query="")
    if len(normalized_code) > 120:
        return _render(
            request,
            user=user,
            query=normalized_code,
            error=_error(
                "SHIPMENT_CODE_TOO_LONG",
                "Código inválido",
                "El código de despacho no puede superar 120 caracteres.",
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
            query=normalized_code,
            error=_error(
                "RELEASE_CONTROL_UNAVAILABLE",
                "Control de salida no disponible",
                "No fue posible consultar el despacho en este momento.",
            ),
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    try:
        payload = TraceabilityLineageService(
            session=session,
            organization_id=user.organization_id,
        ).trace_shipment(normalized_code)
        evidence = project_documentary_evidence(
            session=session,
            organization_id=user.organization_id,
            lineage_payload=payload,
        )

        dossier_available = True
        dossier_error = None
        manifest_sha256 = None
        try:
            bundle = build_documentary_dossier_bundle(
                payload,
                documentary_evidence=evidence,
            )
            manifest_sha256 = bundle.manifest_sha256
        except OriginDossierError as exc:
            dossier_available = False
            dossier_error = str(exc)

        control = build_release_control_view(
            payload,
            documentary_evidence=evidence,
            manifest_sha256=manifest_sha256,
            dossier_available=dossier_available,
            dossier_error=dossier_error,
        )
    except TraceabilityLineageNotFoundError as exc:
        return _render(
            request,
            user=user,
            query=normalized_code,
            error=_error(exc.code, "Despacho no encontrado", str(exc)),
            status_code=status.HTTP_404_NOT_FOUND,
        )
    except TraceabilityLineageValidationError as exc:
        return _render(
            request,
            user=user,
            query=normalized_code,
            error=_error(exc.code, "No se puede evaluar el despacho", str(exc)),
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
            query=normalized_code,
            error=_error(
                "RELEASE_CONTROL_FAILED",
                "No se pudo completar el control",
                "Litoral Trace no pudo reconstruir todos los controles del despacho en este momento.",
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
        query=normalized_code,
        control=control,
    )
