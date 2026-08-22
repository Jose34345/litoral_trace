"""Regression coverage for the Jinja ``dict.items`` traceability collision."""
from __future__ import annotations

from types import SimpleNamespace

from litoral_trace.web.templates import templates
from litoral_trace.web.traceability import build_traceability_view


def test_jinja_prefers_literal_items_key_over_dict_method() -> None:
    rendered = templates.env.from_string(
        "{% for item in value.items %}{{ item }}{% endfor %}"
    ).render(value={"items": ["A", "B"]})

    assert rendered == "AB"


def test_complete_traceability_template_renders_result_items() -> None:
    payload = {
        "shipment": {
            "shipment_code": "SMOKE-RENDER-001",
            "status": "DISPATCHED",
            "lineage_state": "FINAL",
        },
        "allocation_method": "PROPORTIONAL_INPUT_ALLOCATION",
        "complete": True,
        "issues": [],
        "unit_totals": [],
        "items": [],
        "events": [],
        "source_lotes": [],
    }
    view = build_traceability_view(
        query="SMOKE-RENDER-001",
        payload=payload,
    )
    context = {
        "traceability_view": view,
        "user": SimpleNamespace(
            username="smoke",
            organization_name="Smoke Org",
            role="admin",
        ),
        "navigation": (),
        "csrf_form_field": "csrf_token",
        "csrf_token": "test-token",
        "csrf_header_name": "X-CSRF-Token",
        "url_for": lambda name, **kwargs: (
            f"/static{kwargs.get('path', '')}"
        ),
    }

    rendered = templates.env.get_template(
        "traceability.html"
    ).render(context)

    assert "SMOKE-RENDER-001" in rendered
    assert "Productos despachados" in rendered
