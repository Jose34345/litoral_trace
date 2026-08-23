"""Regression gate for P1-A route registration on the Render entrypoint."""
from __future__ import annotations


def test_render_entrypoint_registers_p1a_api_and_web_routes() -> None:
    from render_entrypoint import fastapi_app

    route_methods = {
        (route.path, method)
        for route in fastapi_app.routes
        if hasattr(route, "path")
        for method in getattr(route, "methods", set())
    }

    expected = {
        ("/integrations", "GET"),
        ("/integrations/connections", "POST"),
        ("/integrations/sync-json", "POST"),
        ("/api/v1/integrations/connections", "GET"),
        ("/api/v1/integrations/connections", "POST"),
        ("/api/v1/integrations/entities", "GET"),
        ("/api/v1/integrations/sync-runs", "GET"),
    }

    missing = expected - route_methods
    assert not missing, f"P1-A Render routes are not registered: {sorted(missing)}"
