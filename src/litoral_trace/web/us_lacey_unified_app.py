"""Unified free-tier entrypoint for the customer-facing U.S. Lacey product.

This module composes the already-certified private portal/inline-worker runtime
with the public U.S. Lacey marketing, synthetic-demo, hosted-billing and
superadmin-only owner-control routes.

The composition exists so one customer-facing hostname can serve the full flow:
landing -> sample -> signup -> verification -> login -> billing -> operations,
plus /admin for an authenticated persisted platform superadmin.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
import re

from fastapi import Request, Response

from litoral_trace.us_lacey.operations import (
    UsLaceyOperationError,
    UsLaceyOperationNotFound,
    UsLaceyOperationService,
)
from litoral_trace.us_lacey.portal_auth import (
    US_LACEY_SESSION_COOKIE,
    UsLaceyPortalAuthError,
    resolve_us_lacey_session,
)
from litoral_trace.us_lacey.translation_backfill import run_translation_backfill
from litoral_trace.web.lacey_gtm import render_lacey_landing, router as lacey_router
from litoral_trace.web.us_lacey_free_app import app
from litoral_trace.web.us_lacey_intelligent_workflow import router as intelligent_workflow_router
from litoral_trace.web.us_lacey_lemon_billing import router as lemon_billing_router
from litoral_trace.web.us_lacey_platform_admin import router as platform_admin_router


# Preserve the already-certified free-tier lifespan (inline worker, storage probe)
# and add the historical translation repair as a detached one-shot task. Scheduling
# with create_task means HTTP startup never waits for PostgreSQL or the public
# translation backend. All synchronous DB/network work inside the task uses
# asyncio.to_thread() and the task is capped at 50 spans per process start.
_free_tier_lifespan_context = app.router.lifespan_context


@asynccontextmanager
async def _phase_d_lifespan(application):
    async with _free_tier_lifespan_context(application) as state:
        backfill_task = asyncio.create_task(
            run_translation_backfill(),
            name="us-lacey-translation-backfill",
        )
        application.state.us_lacey_translation_backfill_task = backfill_task
        try:
            yield state
        finally:
            if not backfill_task.done():
                backfill_task.cancel()
            with suppress(asyncio.CancelledError):
                await backfill_task


app.router.lifespan_context = _phase_d_lifespan


# Public marketing/sample, payment-provider, upload-first workflow and superadmin
# routes are additive and do not shadow the certified customer portal endpoints.
# The admin router uses get_us_lacey_db_session() directly so DATABASE_URL remains
# untouched and the existing U.S.-vs-generic database collision sentinel keeps working.
app.include_router(lacey_router)
app.include_router(lemon_billing_router)
app.include_router(intelligent_workflow_router)
app.include_router(platform_admin_router)


_COMPLETE_PATH = re.compile(r"^/operations/(?P<operation_id>[0-9a-fA-F-]{36})/complete$")


@app.middleware("http")
async def _require_explicit_confirmation_before_completion(request: Request, call_next):
    """Fail closed if the legacy completion endpoint sees unconfirmed FOUND values.

    The canonical review service historically treated FOUND as extracted/resolved. The
    upload-first UX changes the authority boundary: FOUND is now a suggestion awaiting
    explicit customer confirmation. This customer-facing guard prevents a direct POST
    from bypassing that boundary while the underlying review-state contract is migrated.
    """
    if request.method == "POST":
        match = _COMPLETE_PATH.fullmatch(request.url.path)
        session_token = request.cookies.get(US_LACEY_SESSION_COOKIE)
        if match and session_token:
            try:
                identity = resolve_us_lacey_session(session_token)
                detail = UsLaceyOperationService().get_detail(
                    organization_id=identity.organization_id,
                    operation_public_id=match.group("operation_id"),
                )
            except (UsLaceyPortalAuthError, UsLaceyOperationNotFound, UsLaceyOperationError):
                # Let the certified endpoint produce its normal auth/not-found behavior.
                detail = None
            if detail is not None and any(field.status == "FOUND" for field in detail.fields):
                return Response(
                    "Confirm all supported suggestions before completing preparation.",
                    status_code=409,
                    media_type="text/plain",
                    headers={"Cache-Control": "no-store, max-age=0"},
                )
    return await call_next(request)


@app.head("/health", include_in_schema=False)
def health_head() -> Response:
    """Lightweight HEAD liveness probe for external uptime monitors.

    The portal's canonical GET /health contract remains owned by the certified
    portal app. UptimeRobot uses HEAD for HTTP monitors by default, so accepting
    HEAD here prevents a healthy service from being reported as HTTP 405/DOWN.
    """
    response = Response(status_code=200)
    response.headers["Cache-Control"] = "no-store, max-age=0"
    return response


@app.middleware("http")
async def _serve_public_landing_at_root(request: Request, call_next):
    """Show the landing at / for visitors while preserving signed-in root flow.

    A browser carrying a portal session cookie is delegated to the original root
    handler, which resolves the opaque session and redirects to billing or the
    operations workspace. Anonymous visitors receive the public U.S. Lacey
    landing directly without a redirect or a second Render service.
    """
    if request.method == "GET" and request.url.path == "/":
        if request.cookies.get(US_LACEY_SESSION_COOKIE):
            return await call_next(request)
        return render_lacey_landing(request)
    return await call_next(request)
