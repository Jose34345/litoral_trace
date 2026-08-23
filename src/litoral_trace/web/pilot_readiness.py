"""Server-rendered first-customer pilot readiness workspace."""
from __future__ import annotations

from fastapi import APIRouter, Request, status
from fastapi.responses import HTMLResponse

from litoral_trace.auth.rbac import Permission
from litoral_trace.db.tenant import get_tenant_scoped_db_session
from litoral_trace.services.pilot_readiness import (
    PilotReadinessPersistenceError,
    PilotReadinessService,
)
from litoral_trace.web.runtime import get_html_route_user, render_web_template


router = APIRouter(tags=["Frontend B2B"])


@router.get(
    "/pilot-readiness",
    response_class=HTMLResponse,
    include_in_schema=False,
    name="render_pilot_readiness",
)
async def render_pilot_readiness(request: Request) -> HTMLResponse:
    user, denied = get_html_route_user(
        request,
        required_permission=Permission.LOTE_READ,
    )
    if denied is not None:
        return denied

    session = get_tenant_scoped_db_session(user.organization_id)
    if session is None:
        return render_web_template(
            request,
            "pilot_readiness.html",
            user=user,
            context={
                "pilot_readiness": None,
                "pilot_error": "El estado del piloto no está disponible temporalmente.",
            },
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    try:
        view = PilotReadinessService(
            session=session,
            organization_id=user.organization_id,
        ).evaluate()
        return render_web_template(
            request,
            "pilot_readiness.html",
            user=user,
            context={"pilot_readiness": view, "pilot_error": None},
        )
    except PilotReadinessPersistenceError:
        return render_web_template(
            request,
            "pilot_readiness.html",
            user=user,
            context={
                "pilot_readiness": None,
                "pilot_error": "No fue posible calcular el estado real del piloto.",
            },
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
    finally:
        session.close()
