"""Public U.S. Lacey Act GTM validation surface.

This module is intentionally isolated from Assurance and regulatory decisioning.
It measures only aggregate, non-PII funnel events for the weekend market test.
"""
from __future__ import annotations

from fastapi import APIRouter, Form, Request, status
from fastapi.responses import HTMLResponse, Response
from prometheus_client import Counter

from litoral_trace.web.templates import render_template


router = APIRouter(tags=["U.S. GTM experiment"])

_ALLOWED_EVENTS = frozenset(
    {
        "lacey_visit",
        "lacey_cta_click",
        "lacey_form_start",
        "lacey_form_submit",
    }
)

_LACEY_GTM_EVENTS = Counter(
    "litoral_trace_lacey_gtm_events_total",
    "Aggregate non-PII conversion events for the U.S. Lacey private beta landing.",
    ("event",),
)


@router.get("/lacey", response_class=HTMLResponse, include_in_schema=False)
async def render_lacey_private_beta(request: Request) -> HTMLResponse:
    """Render the isolated English-language Lacey market-validation landing."""
    response = render_template(
        request,
        "public/lacey.html",
        {
            "commercial_email": "comercial@litoraltrace.com",
        },
    )
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response


@router.post("/lacey/event", include_in_schema=False)
async def record_lacey_gtm_event(
    event: str = Form(...),
) -> Response:
    """Count one whitelisted funnel event without accepting or storing PII."""
    normalized = str(event or "").strip().lower()
    if normalized not in _ALLOWED_EVENTS:
        return Response(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            headers={"Cache-Control": "no-store"},
        )

    _LACEY_GTM_EVENTS.labels(event=normalized).inc()
    return Response(
        status_code=status.HTTP_204_NO_CONTENT,
        headers={"Cache-Control": "no-store"},
    )
