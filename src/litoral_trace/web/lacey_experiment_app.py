"""Standalone ASGI app for the U.S. Lacey GTM market-validation microsite.

This entrypoint deliberately avoids importing the authenticated B2B application.
It can be deployed independently while reusing Litoral Trace templates/assets.
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from litoral_trace.web.lacey_gtm import router as lacey_router
from litoral_trace.web.templates import STATIC_DIR


app = FastAPI(
    title="Litoral Trace — U.S. Lacey Private Beta",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

app.mount(
    "/static",
    StaticFiles(directory=str(STATIC_DIR)),
    name="static",
)
app.include_router(lacey_router)


@app.get("/", include_in_schema=False)
async def root_redirect() -> RedirectResponse:
    return RedirectResponse(url="/lacey", status_code=307)


@app.get("/health", include_in_schema=False)
async def health() -> JSONResponse:
    return JSONResponse(status_code=200, content={"status": "healthy"})
