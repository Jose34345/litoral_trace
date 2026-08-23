"""Regression gate for P1-A route registration on the Render entrypoint."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_render_entrypoint_registers_p1a_api_and_web_routes() -> None:
    """Validate the production import path in a clean interpreter.

    The full unit suite intentionally imports and reconstructs application
    modules in several tests. Inspecting a cached ``render_entrypoint`` object
    in that shared interpreter can therefore observe a stale FastAPI instance.
    A fresh subprocess matches how Render starts the service and keeps this
    release gate independent from pytest module-order side effects.
    """

    root = Path(__file__).resolve().parents[1]
    probe = r'''
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
if missing:
    raise SystemExit(
        "P1-A Render routes are not registered: " + repr(sorted(missing))
    )
'''

    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )

    assert result.returncode == 0, (
        result.stderr.strip()
        or result.stdout.strip()
        or "Render P1-A route probe failed without diagnostic output."
    )
