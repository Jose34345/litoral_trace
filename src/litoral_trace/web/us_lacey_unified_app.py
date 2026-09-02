"""Unified free-tier entrypoint for the customer-facing U.S. Lacey product.

This module composes the already-certified private portal/inline-worker runtime
with the public U.S. Lacey marketing and synthetic-demo routes. It deliberately
keeps the operational application unchanged: authenticated traffic, PostgreSQL
RLS, S3 probes, worker lifecycle, billing and review routes continue to be owned
by ``us_lacey_free_app``.

The composition exists so one customer-facing hostname can serve the full flow:
landing -> sample -> signup -> verification -> login -> billing -> operations.
"""
from __future__ import annotations

from fastapi import Request

from litoral_trace.us_lacey.portal_auth import US_LACEY_SESSION_COOKIE
from litoral_trace.web.lacey_gtm import render_lacey_landing, router as lacey_router
from litoral_trace.web.us_lacey_free_app import app


# Public marketing/sample routes are additive and do not shadow the certified
# portal endpoints (/signup, /login, /billing, /operations, /ready, ...).
app.include_router(lacey_router)


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
