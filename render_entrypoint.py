"""Render-specific public ASGI entrypoint.

The canonical production topology exposes ``/internal/metrics`` only on the
private service network and blocks ``/internal/*`` at Nginx. Render's direct
Web Service ingress does not have that Nginx boundary, so this wrapper applies
the same public-ingress invariant before delegating to the FastAPI app.

P1-A integration routers are registered here explicitly because Render uses
this entrypoint as its production boundary. This keeps the cutover narrow while
ensuring the integration API and server-rendered workspace are actually
reachable in the deployed application.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from starlette.responses import Response

from main import app as fastapi_app
from litoral_trace.api.integrations import router as integrations_api_router
from litoral_trace.web.integrations import router as integrations_web_router


# FastAPI snapshots APIRouter routes when ``include_router`` is called. Register
# P1-A before wrapping the app so both REST and browser surfaces are present on
# the exact FastAPI instance delegated to by Render.
fastapi_app.include_router(integrations_api_router)
fastapi_app.include_router(integrations_web_router)


ASGIReceive = Callable[[], Awaitable[dict[str, Any]]]
ASGISend = Callable[[dict[str, Any]], Awaitable[None]]
ASGIApp = Callable[[dict[str, Any], ASGIReceive, ASGISend], Awaitable[None]]


class RenderPublicIngressGuard:
    """Return 404 for private infrastructure paths on Render public ingress."""

    def __init__(self, downstream: ASGIApp):
        self._downstream = downstream

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: ASGIReceive,
        send: ASGISend,
    ) -> None:
        if (
            scope.get("type") == "http"
            and str(scope.get("path", "")).startswith("/internal/")
        ):
            response = Response(status_code=404)
            await response(scope, receive, send)
            return

        await self._downstream(scope, receive, send)


app = RenderPublicIngressGuard(fastapi_app)
