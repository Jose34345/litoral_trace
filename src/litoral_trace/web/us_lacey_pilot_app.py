"""Standalone ASGI entrypoint for the private U.S. Lacey pilot application.

It intentionally does not reuse the public marketing microsite process. Runtime
readiness fails closed until explicit U.S.-only database and storage settings are
present.
"""
from __future__ import annotations

from fastapi import FastAPI, HTTPException, status

from litoral_trace.us_lacey.config import (
    UsLaceyConfigurationError,
    load_us_lacey_runtime_config,
)


app = FastAPI(
    title="Litoral Trace U.S. Lacey Pilot",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


@app.get("/health")
def health() -> dict[str, str]:
    """Process liveness only; does not claim infrastructure readiness."""
    return {"status": "healthy", "service": "us-lacey-pilot"}


@app.get("/ready")
def ready() -> dict[str, str]:
    """Fail closed until isolated U.S. runtime configuration exists."""
    try:
        config = load_us_lacey_runtime_config()
    except UsLaceyConfigurationError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="U.S. Lacey pilot runtime is not safely configured.",
        ) from exc

    return {
        "status": "ready",
        "service": "us-lacey-pilot",
        "environment": config.environment,
        "hostname": config.app_hostname,
    }
