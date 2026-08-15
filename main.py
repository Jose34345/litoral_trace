"""ASGI entrypoint for the FastAPI HTML + API application."""
from __future__ import annotations

import sys
from pathlib import Path

base_dir = Path(__file__).resolve().parent
src_dir = base_dir / "src"

sys.path.insert(0, str(src_dir))
sys.path.insert(0, str(src_dir / "litoral_trace"))
sys.path.insert(0, str(base_dir))

from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text

from litoral_trace.api.admin import router as admin_router
from litoral_trace.api.auth import (
    LoginRequest,
    clear_auth_cookies,
    get_current_tenant_user,
    login_b2b,
    logout_b2b_session,
    router as auth_router,
)
from litoral_trace.api.lotes import router as lotes_router
from litoral_trace.api.satellite import router as satellite_router
from litoral_trace.api.settings import router as settings_router
from litoral_trace.api.vault import router as vault_router
from litoral_trace.auth.rbac import Permission, ensure_permission, has_permission
from litoral_trace.auth.sessions import ACCESS_TOKEN_COOKIE_KEY, REFRESH_TOKEN_COOKIE_KEY
from litoral_trace.config import get_settings
from litoral_trace.db.engine import get_db_session
from litoral_trace.storage.readiness import is_vault_storage_ready
from litoral_trace.services.admin import listar_empresas_superadmin

app = FastAPI(
    title="Litoral Trace | Compliance Intelligence API",
    description=(
        "API REST B2B para trazabilidad foresto-industrial y cumplimiento "
        "del Reglamento (UE) 2023/1115 (EUDR)."
    ),
    version="2.4.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.include_router(auth_router)
app.include_router(lotes_router)
app.include_router(vault_router)
app.include_router(settings_router)
app.include_router(admin_router)
app.include_router(satellite_router)

possible_template_dirs = [
    src_dir / "litoral_trace" / "templates",
    src_dir / "templates",
    base_dir / "templates",
]

templates_dir = next(
    (directory for directory in possible_template_dirs if directory.exists()),
    src_dir / "litoral_trace" / "templates",
)

templates = Jinja2Templates(directory=str(templates_dir))


def render_template(
    request: Request,
    name: str,
    context: dict | None = None,
):
    """Render a Jinja2 template with Starlette compatibility fallbacks."""
    ctx = context or {}

    try:
        return templates.TemplateResponse(
            request=request,
            name=name,
            context=ctx,
        )
    except Exception:
        try:
            full_ctx = {"request": request, **ctx}
            return templates.TemplateResponse(name, full_ctx)
        except Exception:
            return HTMLResponse(
                content=(
                    f"<h2>Litoral Trace - {name}</h2>"
                    "<p>Servidor activo. Cargando plantilla...</p>"
                )
            )


def _copy_response_cookies(
    *,
    source: Response,
    target: Response,
) -> None:
    for set_cookie_header in source.headers.getlist("set-cookie"):
        target.headers.append("set-cookie", set_cookie_header)


def _render_login_error(
    request: Request,
    *,
    message: str,
    status_code: int,
) -> HTMLResponse:
    response = render_template(
        request,
        "login.html",
        {
            "user": None,
            "error": message,
        },
    )
    response.status_code = status_code
    return response


def _redirect_to_login(*, clear_cookies: bool) -> RedirectResponse:
    response = RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    if clear_cookies:
        clear_auth_cookies(response)
    return response


def _render_access_denied() -> HTMLResponse:
    return HTMLResponse(
        status_code=status.HTTP_403_FORBIDDEN,
        content=(
            "<!DOCTYPE html>"
            "<html lang='es'><head><meta charset='UTF-8'>"
            "<meta name='viewport' content='width=device-width, initial-scale=1.0'>"
            "<title>Acceso denegado</title></head>"
            "<body style='font-family: sans-serif; padding: 2rem;'>"
            "<h2>Acceso denegado</h2>"
            "<p>La cuenta autenticada no posee permisos para esta vista.</p>"
            "<p><a href='/'>Volver al inicio</a></p>"
            "</body></html>"
        ),
    )


def _get_html_route_user(
    request: Request,
    *,
    required_permission: Permission,
):
    session_jwt = request.cookies.get(ACCESS_TOKEN_COOKIE_KEY)
    if not session_jwt:
        return None, _redirect_to_login(clear_cookies=False)

    try:
        user = get_current_tenant_user(session_jwt=session_jwt)
        if user.session_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Sesion HTML invalida o incompleta.",
            )

        ensure_permission(user, required_permission)
        return user, None
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return None, _render_access_denied()
        return None, _redirect_to_login(clear_cookies=True)


@app.get(
    "/",
    response_class=HTMLResponse,
    tags=["Frontend B2B"],
)
async def render_login_view(request: Request):
    """Render the public HTML login page."""
    return render_template(
        request,
        "login.html",
        {"user": None},
    )


@app.post(
    "/login",
    response_class=HTMLResponse,
    tags=["Frontend B2B"],
)
async def submit_login_view(request: Request):
    """Authenticate HTML credentials through the real auth flow."""
    form = await request.form()
    username = str(form.get("username", "")).strip()
    password = str(form.get("password", ""))

    temp_response = Response()

    try:
        await login_b2b(
            LoginRequest(username=username, password=password),
            temp_response,
            request,
        )
    except HTTPException as exc:
        error_message = (
            exc.detail
            if exc.status_code == status.HTTP_400_BAD_REQUEST
            else "Usuario o contrasena incorrectos."
        )
        return _render_login_error(
            request,
            message=error_message,
            status_code=(
                exc.status_code
                if exc.status_code == status.HTTP_400_BAD_REQUEST
                else status.HTTP_401_UNAUTHORIZED
            ),
        )

    redirect_response = RedirectResponse(
        url="/dashboard",
        status_code=status.HTTP_303_SEE_OTHER,
    )
    _copy_response_cookies(
        source=temp_response,
        target=redirect_response,
    )
    return redirect_response


@app.get(
    "/dashboard",
    response_class=HTMLResponse,
    tags=["Frontend B2B"],
)
async def render_dashboard_view(request: Request):
    """Render the B2B dashboard for authenticated users."""
    user, denied_response = _get_html_route_user(
        request,
        required_permission=Permission.LOTE_READ,
    )
    if denied_response is not None:
        return denied_response

    return render_template(
        request,
        "dashboard.html",
        {"user": user},
    )


@app.get(
    "/vault",
    response_class=HTMLResponse,
    tags=["Frontend B2B"],
)
async def render_vault_view(request: Request):
    """Render the tenant Vault with server-derived UI capabilities."""
    user, denied_response = _get_html_route_user(
        request,
        required_permission=Permission.VAULT_READ,
    )
    if denied_response is not None:
        return denied_response

    storage_settings = get_settings().storage

    return render_template(
        request,
        "vault.html",
        {
            "user": user,
            "vault_can_upload": has_permission(
                user,
                Permission.VAULT_UPLOAD,
            ),
            "vault_can_delete": has_permission(
                user,
                Permission.VAULT_DELETE,
            ),
            "vault_max_upload_bytes": storage_settings.max_upload_bytes,
            "vault_max_upload_mb": round(
                storage_settings.max_upload_bytes / (1024 * 1024),
                1,
            ),
        },
    )


@app.get(
    "/settings",
    response_class=HTMLResponse,
    tags=["Frontend B2B"],
)
async def render_settings_view(request: Request):
    """Render the settings page for authenticated users."""
    user, denied_response = _get_html_route_user(
        request,
        required_permission=Permission.SETTINGS_WRITE,
    )
    if denied_response is not None:
        return denied_response

    return render_template(
        request,
        "settings.html",
        {"user": user},
    )


@app.get(
    "/admin",
    response_class=HTMLResponse,
    tags=["Frontend B2B"],
)
async def render_admin_view(request: Request):
    """Render the platform admin page for superadmins."""
    user, denied_response = _get_html_route_user(
        request,
        required_permission=Permission.PLATFORM_ADMIN,
    )
    if denied_response is not None:
        return denied_response

    try:
        organizations = listar_empresas_superadmin(
            refresh_token=request.cookies.get(REFRESH_TOKEN_COOKIE_KEY),
        )
    except HTTPException as exc:
        if exc.status_code == status.HTTP_401_UNAUTHORIZED:
            return _redirect_to_login(clear_cookies=True)
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return _render_access_denied()
        raise

    return render_template(
        request,
        "admin_organizations.html",
        {
            "user": user,
            "organizations": organizations,
            "organization_count": len(organizations),
        },
    )


@app.get(
    "/logout",
    response_class=HTMLResponse,
    tags=["Frontend B2B"],
)
async def logout_view(request: Request):
    """Show a logout confirmation page without mutating state."""
    return HTMLResponse(
        content=(
            "<!DOCTYPE html>"
            "<html lang='es'><head><meta charset='UTF-8'>"
            "<meta name='viewport' content='width=device-width, initial-scale=1.0'>"
            "<title>Confirmar cierre de sesion</title></head>"
            "<body style='font-family: sans-serif; padding: 2rem;'>"
            "<h2>Cerrar sesion</h2>"
            "<p>El cierre de sesion requiere una solicitud POST.</p>"
            "<form method='post' action='/logout'>"
            "<button type='submit'>Confirmar cierre de sesion</button>"
            "</form>"
            "<p><a href='/dashboard'>Volver</a></p>"
            "</body></html>"
        )
    )


@app.post(
    "/logout",
    tags=["Frontend B2B"],
)
async def logout_submit_view(request: Request):
    """Revoke the current session and redirect to login."""
    temp_response = Response()
    await logout_b2b_session(
        temp_response,
        request=request,
        refresh_token_cookie=request.cookies.get(REFRESH_TOKEN_COOKIE_KEY),
        session_jwt=request.cookies.get(ACCESS_TOKEN_COOKIE_KEY),
    )

    response = RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    _copy_response_cookies(
        source=temp_response,
        target=response,
    )
    return response


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
            "service": "Litoral Trace Engine",
            "version": app.version,
        },
    )


@app.get(
    "/ready",
    tags=["Infraestructura"],
)
async def readiness_check() -> JSONResponse:
    """Fail closed when a required runtime dependency is unavailable."""

    try:
        session = get_db_session()
    except Exception:
        session = None

    if session is None:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unavailable"},
        )

    try:
        session.execute(text("SELECT 1"))
    except Exception:
        try:
            session.rollback()
        except Exception:
            pass
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unavailable"},
        )
    finally:
        try:
            session.close()
        except Exception:
            pass

    if not is_vault_storage_ready():
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unavailable"},
        )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"status": "ready"},
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
            "message": "Servidor FastAPI Litoral Trace Activo",
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