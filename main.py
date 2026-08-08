"""Punto de Entrada Servidor ASGI FastAPI - Litoral Trace Enterprise B2B."""
from __future__ import annotations

import sys
from pathlib import Path

# Insertar rutas posibles en sys.path para resolución de módulos.
base_dir = Path(__file__).resolve().parent
src_dir = base_dir / "src"

sys.path.insert(0, str(src_dir))
sys.path.insert(0, str(src_dir / "litoral_trace"))
sys.path.insert(0, str(base_dir))

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

# ---------------------------------------------------------------------------
# Aplicación FastAPI
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Routers de API
# ---------------------------------------------------------------------------
#
# IMPORTANTE:
# Los routers se importan de forma explícita y sin un try/except global.
#
# Si cualquiera de estos módulos tiene un error de importación, el proceso
# debe fallar durante el arranque. Es preferible un fallo explícito a levantar
# una API parcialmente funcional y ocultar rutas por un error de importación.
# ---------------------------------------------------------------------------

from litoral_trace.api.auth import router as auth_router
from litoral_trace.api.lotes import router as lotes_router
from litoral_trace.api.vault import router as vault_router
from litoral_trace.api.settings import router as settings_router
from litoral_trace.api.admin import router as admin_router
from litoral_trace.api.satellite import router as satellite_router

app.include_router(auth_router)
app.include_router(lotes_router)
app.include_router(vault_router)
app.include_router(settings_router)
app.include_router(admin_router)
app.include_router(satellite_router)

# ---------------------------------------------------------------------------
# Configuración de plantillas Jinja2
# ---------------------------------------------------------------------------

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
    """Renderiza una plantilla Jinja2 con compatibilidad entre versiones."""
    ctx = context or {}

    try:
        return templates.TemplateResponse(
            request=request,
            name=name,
            context=ctx,
        )
    except Exception:
        # Compatibilidad con versiones anteriores de Starlette/FastAPI.
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


# ---------------------------------------------------------------------------
# Vistas Frontend HTML B2B
# ---------------------------------------------------------------------------

@app.get(
    "/",
    response_class=HTMLResponse,
    tags=["Frontend B2B"],
)
async def render_login_view(request: Request):
    """Renderiza la pantalla de inicio de sesión."""
    return render_template(
        request,
        "login.html",
        {"user": None},
    )


@app.get(
    "/dashboard",
    response_class=HTMLResponse,
    tags=["Frontend B2B"],
)
async def render_dashboard_view(request: Request):
    """Renderiza el dashboard B2B."""
    demo_user = {
        "username": "admin",
        "organization_name": "Exportadora Forestal del Chaco S.A.",
        "role": "admin",
    }

    return render_template(
        request,
        "dashboard.html",
        {"user": demo_user},
    )


@app.get(
    "/vault",
    response_class=HTMLResponse,
    tags=["Frontend B2B"],
)
async def render_vault_view(request: Request):
    """Renderiza el vault documental."""
    demo_user = {
        "username": "admin",
        "organization_name": "Exportadora Forestal del Chaco S.A.",
        "role": "admin",
    }

    return render_template(
        request,
        "vault.html",
        {"user": demo_user},
    )


@app.get(
    "/settings",
    response_class=HTMLResponse,
    tags=["Frontend B2B"],
)
async def render_settings_view(request: Request):
    """Renderiza la configuración de la cuenta."""
    demo_user = {
        "username": "admin",
        "organization_name": "Exportadora Forestal del Chaco S.A.",
        "role": "admin",
    }

    return render_template(
        request,
        "settings.html",
        {"user": demo_user},
    )


@app.get(
    "/admin",
    response_class=HTMLResponse,
    tags=["Frontend B2B"],
)
async def render_admin_view(request: Request):
    """Renderiza la administración de organizaciones."""
    demo_user = {
        "username": "admin",
        "organization_name": "Litoral Trace SuperAdmin",
        "role": "admin",
    }

    return render_template(
        request,
        "admin_organizations.html",
        {"user": demo_user},
    )


@app.get(
    "/logout",
    response_class=HTMLResponse,
    tags=["Frontend B2B"],
)
async def logout_view(request: Request):
    """Elimina la cookie de sesión y vuelve al login."""
    response = render_template(
        request,
        "login.html",
        {"user": None},
    )

    response.delete_cookie("session_jwt")

    return response


# ---------------------------------------------------------------------------
# Healthchecks e información del servicio
# ---------------------------------------------------------------------------

@app.get(
    "/health",
    tags=["Infraestructura"],
)
async def health_check() -> JSONResponse:
    """Healthcheck básico del servidor FastAPI."""
    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "service": "Litoral Trace Engine",
            "version": app.version,
        },
    )


@app.get(
    "/api/v1/info",
    tags=["Infraestructura"],
)
async def root_index() -> JSONResponse:
    """Información básica de la API."""
    return JSONResponse(
        status_code=200,
        content={
            "message": "Servidor FastAPI Litoral Trace Activo",
            "documentation": "/docs",
            "health": "/health",
        },
    )


# ---------------------------------------------------------------------------
# Ejecución directa
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
