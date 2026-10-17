"""Jinja views for the five-shipment product-led evaluation."""
from __future__ import annotations

from litoral_trace.web.templates import templates


def render_evaluation_upgrade(
    *,
    request,
    identity,
    entitlement,
) -> str:
    return templates.get_template("us_lacey/evaluation_upgrade.html").render(
        request=request,
        identity=identity,
        entitlement=entitlement,
    )


def render_evaluation_claim(
    *,
    request,
    identity,
    entitlement,
    csrf_token: str,
    error: str | None = None,
) -> str:
    return templates.get_template("us_lacey/evaluation_claim.html").render(
        request=request,
        identity=identity,
        entitlement=entitlement,
        csrf_token=csrf_token,
        error=error,
    )
