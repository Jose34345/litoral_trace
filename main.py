"""Punto de Entrada Servidor ASGI FastAPI - Litoral Trace Enterprise B2B."""
from __future__ import annotations
import sys
from pathlib import Path
from datetime import datetime, timezone

# Insertar rutas posibles en sys.path para resolución de módulos
base_dir = Path(__file__).resolve().parent
src_dir = base_dir / "src"
sys.path.insert(0, str(src_dir))
sys.path.insert(0, str(src_dir / "litoral_trace"))
sys.path.insert(0, str(base_dir))

from fastapi import FastAPI, Request, Form, Response, HTTPException, status
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

# Importaciones flexibles de routers API
try:
    from litoral_trace.api.auth import router as auth_router
    from litoral_trace.api.lotes import router as lotes_router
    from litoral_trace.api.vault import router as vault_router
    from litoral_trace.api.settings import router as settings_router
    from litoral_trace.api.admin import router as admin_router
except ModuleNotFoundError:
    from api.auth import router as auth_router
    from api.lotes import router as lotes_router
    from api.vault import router as vault_router
    from api.settings import router as settings_router
    from api.admin import router as admin_router

from litoral_trace.auth.tokens import create_jwt_token

# Inicializar FastAPI
app = FastAPI(
    title="Litoral Trace | Compliance Intelligence API",
    description="API REST B2B para trazabilidad foresto-industrial y cumplimiento del Reglamento (UE) 2023/1115 (EUDR).",
    version="2.4.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Incluir Routers REST API
app.include_router(auth_router)
app.include_router(lotes_router)
app.include_router(vault_router)
app.include_router(settings_router)
app.include_router(admin_router)

# Configurar plantillas Jinja2
possible_template_dirs = [
    src_dir / "litoral_trace" / "templates",
    src_dir / "templates",
    base_dir / "templates"
]
templates_dir = next((d for d in possible_template_dirs if d.exists()), src_dir / "litoral_trace" / "templates")
templates = Jinja2Templates(directory=str(templates_dir))

def render_template(request: Request, name: str, context: dict | None = None):
    """Helper de rendering con fallback seguro."""
    ctx = context or {}
    try:
        return templates.TemplateResponse(request=request, name=name, context=ctx)
    except Exception:
        try:
            full_ctx = {"request": request, **ctx}
            return templates.TemplateResponse(name, full_ctx)
        except Exception as e:
            return HTMLResponse(f"<h2>Litoral Trace - {name}</h2><p>Servidor activo. Cargando plantilla...</p>")

# --- VISTAS FRONTEND HTML B2B & FORMULARIO DE LOGIN ---

@app.get("/", response_class=HTMLResponse, tags=["Frontend B2B"])
async def render_login_view(request: Request):
    return render_template(request, "login.html", {"user": None})

@app.post("/login", tags=["Frontend B2B"])
async def handle_login_form(
    request: Request,
    username: str = Form(...),
    password: str = Form(...)
):
    """Manejo de inicio de sesión desde el formulario HTML B2B con redirección HTTP 303."""
    u = username.strip()
    p = password.strip()

    if u == "admin" and p == "admin123":
        user_data = {
            "sub": "admin",
            "org_id": 1,
            "org_name": "Exportadora Forestal del Chaco S.A.",
            "role": "admin",
            "email": "comercial@litoraltrace.com"
        }
    elif u and p:
        user_data = {
            "sub": u,
            "org_id": 42,
            "org_name": "Aserradero Gran Chaco S.R.L.",
            "role": "manager",
            "email": f"{u}@litoraltrace.com"
        }
    else:
        return render_template(request, "login.html", {"error": "Credenciales inválidas. Ingrese usuario y clave."})

    jwt_token = create_jwt_token(user_data, expires_in_seconds=86400)
    
    # Redirección HTTP 303 al Dashboard con cookie de sesión
    response = RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)
    response.set_cookie(
        key="session_jwt",
        value=jwt_token,
        httponly=True,
        samesite="lax",
        max_age=86400
    )
    return response

@app.get("/dashboard", response_class=HTMLResponse, tags=["Frontend B2B"])
async def render_dashboard_view(request: Request):
    demo_user = {"username": "admin", "organization_name": "Exportadora Forestal del Chaco S.A.", "role": "admin"}
    return render_template(request, "dashboard.html", {"user": demo_user})

@app.get("/vault", response_class=HTMLResponse, tags=["Frontend B2B"])
async def render_vault_view(request: Request):
    demo_user = {"username": "admin", "organization_name": "Exportadora Forestal del Chaco S.A.", "role": "admin"}
    return render_template(request, "vault.html", {"user": demo_user})

@app.get("/settings", response_class=HTMLResponse, tags=["Frontend B2B"])
async def render_settings_view(request: Request):
    demo_user = {"username": "admin", "organization_name": "Exportadora Forestal del Chaco S.A.", "role": "admin"}
    return render_template(request, "settings.html", {"user": demo_user})

@app.get("/admin", response_class=HTMLResponse, tags=["Frontend B2B"])
async def render_admin_view(request: Request):
    """Panel de SuperAdmin para gestión de empresas clientes y licencias B2B."""
    demo_user = {"username": "admin", "organization_name": "Litoral Trace SuperAdmin", "role": "admin"}
    return render_template(request, "admin_organizations.html", {"user": demo_user})

@app.get("/logout", response_class=HTMLResponse, tags=["Frontend B2B"])
async def logout_view(request: Request):
    response = render_template(request, "login.html", {"user": None})
    response.delete_cookie("session_jwt")
    return response

@app.get("/health", tags=["Infraestructura"])
async def health_check() -> JSONResponse:
    return JSONResponse(status_code=200, content={"status": "healthy", "service": "Litoral Trace Engine v2.4.0"})

@app.get("/api/v1/info", tags=["Infraestructura"])
async def root_index() -> JSONResponse:
    return JSONResponse(status_code=200, content={"message": "Servidor FastAPI Litoral Trace Activo", "documentation": "/docs", "health": "/health"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
