"""ASGI entrypoint for the FastAPI API + server-rendered web application."""
from __future__ import annotations

import sys
from pathlib import Path

base_dir = Path(__file__).resolve().parent
src_dir = base_dir / "src"

sys.path.insert(0, str(src_dir))
sys.path.insert(0, str(src_dir / "litoral_trace"))
sys.path.insert(0, str(base_dir))

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

from litoral_trace.api.admin import router as admin_router
from litoral_trace.api.auth import router as auth_router
from litoral_trace.api.batch_evidence import router as batch_evidence_router
from litoral_trace.api.lotes import router as lotes_router
from litoral_trace.api.satellite import router as satellite_router
from litoral_trace.api.settings import router as settings_router
from litoral_trace.api.vault import router as vault_router
from litoral_trace.db.engine import get_db_session
from litoral_trace.storage.readiness import is_vault_storage_ready
from litoral_trace.web.middleware import CookieApiCsrfMiddleware
from litoral_trace.web.router import router as web_router
from litoral_trace.web.templates import STATIC_DIR


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

app.add_middleware(CookieApiCsrfMiddleware)

app.mount(
    "/static",
    StaticFiles(directory=str(STATIC_DIR)),
    name="static",
)

app.include_router(auth_router)
app.include_router(lotes_router)
app.include_router(batch_evidence_router)
app.include_router(vault_router)
app.include_router(settings_router)
app.include_router(admin_router)
app.include_router(satellite_router)
app.include_router(web_router)


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