"""ASGI entrypoint for the FastAPI API + server-rendered web application."""

from __future__ import annotations

import sys
from pathlib import Path

base_dir = Path(__file__).resolve().parent
src_dir = base_dir / "src"

sys.path.insert(
    0,
    str(src_dir),
)
sys.path.insert(
    0,
    str(src_dir / "litoral_trace"),
)
sys.path.insert(
    0,
    str(base_dir),
)

from fastapi import (
    FastAPI,
    HTTPException,
    Request,
    Response,
    status,
)
from fastapi.responses import (
    JSONResponse,
    RedirectResponse,
)
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST
from sqlalchemy import text

from litoral_trace.config import (
    get_settings,
)
from litoral_trace.api.admin import (
    router as admin_router,
)
from litoral_trace.api.auth import (
    LoginRequest,
    login_b2b,
    logout_b2b_session,
    router as auth_router,
)
from litoral_trace.api.batch_evidence import (
    router as batch_evidence_router,
)
from litoral_trace.api.integrations import (
    router as integrations_router,
)
from litoral_trace.api.lotes import (
    router as lotes_router,
)
from litoral_trace.api.satellite import (
    router as satellite_router,
)
from litoral_trace.api.settings import (
    router as settings_router,
)
from litoral_trace.api.traceability import (
    router as traceability_router,
)
from litoral_trace.api.vault import (
    router as vault_router,
)
from litoral_trace.auth.sessions import (
    ACCESS_TOKEN_COOKIE_KEY,
    REFRESH_TOKEN_COOKIE_KEY,
)
from litoral_trace.db.engine import (
    get_db_session,
)
from litoral_trace.observability.api_metrics import (
    ApiMetricsMiddleware,
    api_metrics,
)
from litoral_trace.storage.readiness import (
    is_vault_storage_ready,
)
from litoral_trace.web.middleware import (
    CookieApiCsrfMiddleware,
)

# Import the module for compatibility wrappers.
import litoral_trace.web.router as web_router_module

# Import the APIRouter explicitly for FastAPI registration.
from litoral_trace.web.router import (
    router as web_router,
)

from litoral_trace.web.runtime import (
    clear_browser_security_cookies,
    copy_response_cookies,
    get_authenticated_html_user,
    rotate_csrf_browser_cookie,
)
from litoral_trace.web.templates import (
    STATIC_DIR,
)


settings = get_settings()


app = FastAPI(
    title=(
        "Litoral Trace | "
        "Compliance Intelligence API"
    ),
    description=(
        "API REST B2B para trazabilidad "
        "foresto-industrial y cumplimiento "
        "del Reglamento (UE) 2023/1115 "
        "(EUDR)."
    ),
    version="2.4.0",
    docs_url=None if settings.is_production else "/docs",
    redoc_url=None if settings.is_production else "/redoc",
    openapi_url=None if settings.is_production else "/openapi.json",
)


app.add_middleware(
    CookieApiCsrfMiddleware
)
app.add_middleware(
    ApiMetricsMiddleware
)


app.mount(
    "/static",
    StaticFiles(
        directory=str(STATIC_DIR)
    ),
    name="static",
)


# ---------------------------------------------------------------------------
# API routers
# ---------------------------------------------------------------------------

app.include_router(
    auth_router
)

app.include_router(
    lotes_router
)

app.include_router(
    batch_evidence_router
)

app.include_router(
    traceability_router
)

app.include_router(
    integrations_router
)

app.include_router(
    vault_router
)

app.include_router(
    settings_router
)

app.include_router(
    admin_router
)

app.include_router(
    satellite_router
)


# ---------------------------------------------------------------------------
# Server-rendered web router
# ---------------------------------------------------------------------------
#
# Keep this explicit.
#
# ``web_router_module`` is used by the legacy compatibility wrappers below.
# ``web_router`` is the actual FastAPI APIRouter registered into ``app``.
# ---------------------------------------------------------------------------

app.include_router(
    web_router
)

# UX10-D owns write routes and imports ``web.runtime``. Import it only after
# the primary web router and the application bootstrap are fully initialized.
# FastAPI copies APIRouter routes eagerly, so this strict late binding prevents
# a partially initialized operational router from being snapshotted as empty.
from litoral_trace.web.traceability_operations import (
    router as traceability_operations_router,
)

app.include_router(
    traceability_operations_router
)

# P1-A owns a dedicated browser workspace for integration staging/reconciliation.
from litoral_trace.web.integrations import (
    router as integrations_web_router,
)

app.include_router(
    integrations_web_router
)


# ---------------------------------------------------------------------------
# Legacy direct-call compatibility
# ---------------------------------------------------------------------------
#
# Production/browser routes live exclusively in:
#
#     litoral_trace.web.router
#
# Some historical regression tests import HTML handlers directly from
# ``main`` and invoke them with synthetic Starlette Request objects that do
# not contain an ASGI ``app`` or ``router`` in their scope.
#
# These wrappers preserve that historical direct-call contract without being
# registered as FastAPI endpoints themselves.
#
# Real HTTP traffic continues through ``web_router`` and therefore keeps the
# current CSRF/authentication behavior.
# ---------------------------------------------------------------------------


def _bind_legacy_request_to_app(
    request: Request,
) -> bool:
    """Attach FastAPI only to synthetic direct-call requests."""

    is_legacy_direct_call = (
        request.scope.get("app") is None
        and request.scope.get("router") is None
    )

    if is_legacy_direct_call:
        request.scope["app"] = app

    return is_legacy_direct_call


def _legacy_home_redirect(
    response: Response,
    *,
    legacy_direct_call: bool,
) -> Response:
    """Preserve the historical unauthenticated redirect contract."""

    if (
        legacy_direct_call
        and response.status_code
        == status.HTTP_303_SEE_OTHER
        and response.headers.get(
            "location"
        )
        == "/login"
    ):
        response.headers[
            "location"
        ] = "/"

    return response


async def dashboard(
    request: Request,
):
    legacy_direct_call = _bind_legacy_request_to_app(
        request
    )

    response = await web_router_module.dashboard_page(
        request
    )

    return _legacy_home_redirect(
        response,
        legacy_direct_call=legacy_direct_call,
    )


async def admin(
    request: Request,
):
    legacy_direct_call = _bind_legacy_request_to_app(
        request
    )

    response = await web_router_module.admin_page(
        request
    )

    return _legacy_home_redirect(
        response,
        legacy_direct_call=legacy_direct_call,
    )


async def settings_page(
    request: Request,
):
    legacy_direct_call = _bind_legacy_request_to_app(
        request
    )

    response = await web_router_module.settings_page(
        request
    )

    return _legacy_home_redirect(
        response,
        legacy_direct_call=legacy_direct_call,
    )


async def vault_page(
    request: Request,
):
    legacy_direct_call = _bind_legacy_request_to_app(
        request
    )

    response = await web_router_module.vault_page(
        request
    )

    return _legacy_home_redirect(
        response,
        legacy_direct_call=legacy_direct_call,
    )


async def batch_import_page(
    request: Request,
):
    legacy_direct_call = _bind_legacy_request_to_app(
        request
    )

    response = await web_router_module.batch_import_page(
        request
    )

    return _legacy_home_redirect(
        response,
        legacy_direct_call=legacy_direct_call,
    )


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "Litoral Trace Engine v2.4.0",
        "version": "2.4.0",
    }


@app.get("/ready")
def readiness_check():
    checks: dict[str, bool] = {
        "database": False,
        "vault": False,
    }

    session = get_db_session()
    if session is not None:
        try:
            session.execute(text("SELECT 1"))
            checks["database"] = True
        except Exception:
            checks["database"] = False
        finally:
            session.close()

    checks["vault"] = is_vault_storage_ready()

    ready = all(checks.values())
    return JSONResponse(
        status_code=(
            status.HTTP_200_OK
            if ready
            else status.HTTP_503_SERVICE_UNAVAILABLE
        ),
        content={
            "status": "ready" if ready else "not_ready",
            "checks": checks,
        },
    )


@app.get("/internal/metrics")
def metrics_endpoint():
    if settings.is_production:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Not Found",
        )

    return Response(
        content=api_metrics.render(),
        media_type=CONTENT_TYPE_LATEST,
    )


@app.get("/api/v1/info")
def api_info():
    return {
        "service": "Litoral Trace",
        "version": "2.4.0",
        "environment": settings.environment,
    }
