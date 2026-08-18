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
from litoral_trace.api.lotes import (
    router as lotes_router,
)
from litoral_trace.api.satellite import (
    router as satellite_router,
)
from litoral_trace.api.settings import (
    router as settings_router,
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


async def render_home_view(
    request: Request,
):
    """Compatibility export for the public homepage."""

    _bind_legacy_request_to_app(
        request
    )

    return await (
        web_router_module
        .render_home_view(
            request
        )
    )


async def render_regional_intelligence_index_view(
    request: Request,
):
    """Compatibility export for Regional Intelligence index."""

    _bind_legacy_request_to_app(
        request
    )

    return await (
        web_router_module
        .render_regional_intelligence_index_view(
            request
        )
    )


async def render_regional_intelligence_detail_view(
    request: Request,
    region_slug: str,
):
    """Compatibility export for one regional profile."""

    _bind_legacy_request_to_app(
        request
    )

    return await (
        web_router_module
        .render_regional_intelligence_detail_view(
            request,
            region_slug,
        )
    )


async def render_login_view(
    request: Request,
):
    """Compatibility export for the browser login page."""

    _bind_legacy_request_to_app(
        request
    )

    return await (
        web_router_module
        .render_login_view(
            request
        )
    )


async def submit_login_view(
    request: Request,
):
    """Compatibility adapter for historical direct login calls.

    Real POST /login traffic continues through
    ``web_router_module.submit_login_view`` and therefore enforces the
    browser-bound CSRF contract.

    Only synthetic direct calls without an ASGI app/router enter the legacy
    compatibility branch.
    """

    legacy_direct_call = (
        _bind_legacy_request_to_app(
            request
        )
    )

    if not legacy_direct_call:
        return await (
            web_router_module
            .submit_login_view(
                request
            )
        )

    form = await request.form()

    username = str(
        form.get(
            "username",
            "",
        )
    ).strip()

    password = str(
        form.get(
            "password",
            "",
        )
    )

    temp_response = Response()

    try:
        await login_b2b(
            LoginRequest(
                username=username,
                password=password,
            ),
            temp_response,
            request,
        )

    except HTTPException as exc:
        error_message = (
            exc.detail
            if (
                exc.status_code
                == status.HTTP_400_BAD_REQUEST
            )
            else (
                "Usuario o contrasena "
                "incorrectos."
            )
        )

        response_status = (
            exc.status_code
            if (
                exc.status_code
                == status.HTTP_400_BAD_REQUEST
            )
            else status.HTTP_401_UNAUTHORIZED
        )

        return (
            web_router_module
            ._render_login_error(
                request,
                message=error_message,
                status_code=response_status,
            )
        )

    response = RedirectResponse(
        url="/dashboard",
        status_code=(
            status.HTTP_303_SEE_OTHER
        ),
    )

    copy_response_cookies(
        source=temp_response,
        target=response,
    )

    rotate_csrf_browser_cookie(
        response
    )

    return response


async def render_dashboard_view(
    request: Request,
):
    """Compatibility export for dashboard rendering."""

    legacy_direct_call = (
        _bind_legacy_request_to_app(
            request
        )
    )

    response = await (
        web_router_module
        .render_dashboard_view(
            request
        )
    )

    return _legacy_home_redirect(
        response,
        legacy_direct_call=(
            legacy_direct_call
        ),
    )


async def render_vault_view(
    request: Request,
):
    """Compatibility export for Vault rendering."""

    legacy_direct_call = (
        _bind_legacy_request_to_app(
            request
        )
    )

    response = await (
        web_router_module
        .render_vault_view(
            request
        )
    )

    return _legacy_home_redirect(
        response,
        legacy_direct_call=(
            legacy_direct_call
        ),
    )


async def render_settings_view(
    request: Request,
):
    """Compatibility export for Settings rendering."""

    legacy_direct_call = (
        _bind_legacy_request_to_app(
            request
        )
    )

    response = await (
        web_router_module
        .render_settings_view(
            request
        )
    )

    return _legacy_home_redirect(
        response,
        legacy_direct_call=(
            legacy_direct_call
        ),
    )


async def render_admin_view(
    request: Request,
):
    """Compatibility export for Superadmin rendering."""

    legacy_direct_call = (
        _bind_legacy_request_to_app(
            request
        )
    )

    response = await (
        web_router_module
        .render_admin_view(
            request
        )
    )

    return _legacy_home_redirect(
        response,
        legacy_direct_call=(
            legacy_direct_call
        ),
    )


async def logout_view(
    request: Request,
):
    """Compatibility export for the logout confirmation page."""

    legacy_direct_call = (
        _bind_legacy_request_to_app(
            request
        )
    )

    response = await (
        web_router_module
        .logout_view(
            request
        )
    )

    return _legacy_home_redirect(
        response,
        legacy_direct_call=(
            legacy_direct_call
        ),
    )


async def logout_submit_view(
    request: Request,
):
    """Compatibility adapter for historical direct logout calls.

    The actual POST /logout route remains CSRF-protected inside the
    registered web router.
    """

    legacy_direct_call = (
        _bind_legacy_request_to_app(
            request
        )
    )

    if not legacy_direct_call:
        return await (
            web_router_module
            .logout_submit_view(
                request
            )
        )

    user, denied_response = (
        get_authenticated_html_user(
            request
        )
    )

    if denied_response is not None:
        return _legacy_home_redirect(
            denied_response,
            legacy_direct_call=True,
        )

    del user

    temp_response = Response()

    await logout_b2b_session(
        temp_response,
        request=request,
        refresh_token_cookie=(
            request.cookies.get(
                REFRESH_TOKEN_COOKIE_KEY
            )
        ),
        session_jwt=(
            request.cookies.get(
                ACCESS_TOKEN_COOKIE_KEY
            )
        ),
    )

    response = RedirectResponse(
        url="/",
        status_code=(
            status.HTTP_303_SEE_OTHER
        ),
    )

    copy_response_cookies(
        source=temp_response,
        target=response,
    )

    clear_browser_security_cookies(
        response
    )

    return response


# ---------------------------------------------------------------------------
# Infrastructure endpoints
# ---------------------------------------------------------------------------


def _runtime_dependency_readiness() -> dict[str, bool]:
    database_ready = False
    vault_ready = False

    try:
        session = get_db_session()
    except Exception:
        session = None

    if session is not None:
        try:
            session.execute(text("SELECT 1"))
            database_ready = True
        except Exception:
            try:
                session.rollback()
            except Exception:
                pass
        finally:
            try:
                session.close()
            except Exception:
                pass

    try:
        vault_ready = bool(is_vault_storage_ready())
    except Exception:
        vault_ready = False

    return {
        "database": database_ready,
        "vault": vault_ready,
    }


@app.get(
    "/health",
    tags=["Infraestructura"],
)
async def health_check() -> JSONResponse:
    """Return a basic service healthcheck."""

    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "service": (
                "Litoral Trace Engine"
            ),
            "version": app.version,
        },
    )


@app.get(
    "/ready",
    tags=["Infraestructura"],
)
async def readiness_check() -> JSONResponse:
    """Fail closed when a required runtime dependency is unavailable."""

    dependencies = _runtime_dependency_readiness()
    if not all(dependencies.values()):
        return JSONResponse(
            status_code=(
                status.HTTP_503_SERVICE_UNAVAILABLE
            ),
            content={
                "status": "unavailable"
            },
        )

    return JSONResponse(
        status_code=(
            status.HTTP_200_OK
        ),
        content={
            "status": "ready"
        },
    )


@app.get(
    "/internal/metrics",
    include_in_schema=False,
)
async def internal_metrics() -> Response:
    """Expose sanitized Prometheus metrics only to the private service network."""

    dependencies = _runtime_dependency_readiness()
    api_metrics.set_dependency_readiness(
        database=dependencies["database"],
        vault=dependencies["vault"],
    )
    return Response(
        content=api_metrics.render(),
        status_code=status.HTTP_200_OK,
        headers={"Content-Type": CONTENT_TYPE_LATEST},
    )


@app.get(
    "/api/v1/info",
    tags=["Infraestructura"],
)
async def root_index() -> JSONResponse:
    """Return basic API metadata."""

    return JSONResponse(
        status_code=200,
        content={
            "message": (
                "Servidor FastAPI "
                "Litoral Trace Activo"
            ),
            "documentation": "/docs",
            "health": "/health",
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
