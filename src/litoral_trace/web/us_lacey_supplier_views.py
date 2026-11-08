"""Jinja-backed views for U.S. Lacey supplier intelligence."""
from __future__ import annotations

from litoral_trace.web.templates import templates


def _render(request, name: str, **context: object) -> str:
    return templates.get_template(f"us_lacey/{name}.html").render(
        request=request,
        **context,
    )


def render_suppliers_list(
    *,
    request,
    identity,
    entitlement,
    directory,
) -> str:
    return _render(
        request,
        "suppliers_list",
        identity=identity,
        entitlement=entitlement,
        directory=directory,
    )


def render_supplier_detail(
    *,
    request,
    identity,
    entitlement,
    supplier,
) -> str:
    return _render(
        request,
        "supplier_detail",
        identity=identity,
        entitlement=entitlement,
        supplier=supplier,
    )
