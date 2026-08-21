"""API registration contract for the P1C reverse genealogy endpoint."""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys


EXPECTED_PATH = "/api/v1/traceability/shipments/{shipment_code}/origin"


def test_p1c_origin_endpoint_is_registered_once() -> None:
    """Validate registration in a new Python process like a fresh ASGI worker."""
    root_dir = Path(__file__).resolve().parents[1]
    probe = f"""
import fastapi
import main
from litoral_trace.api.traceability import router as traceability_router
expected = {EXPECTED_PATH!r}
paths = [
    route.path
    for route in main.app.routes
    if getattr(route, 'path', None) == expected
]
if paths != [expected]:
    diagnostics = {{
        'fastapi_version': fastapi.__version__,
        'router_type': f'{{type(traceability_router).__module__}}.{{type(traceability_router).__name__}}',
        'router_routes': [
            {{
                'type': f'{{type(route).__module__}}.{{type(route).__name__}}',
                'path': getattr(route, 'path', None),
                'repr': repr(route),
            }}
            for route in traceability_router.routes
        ],
        'main_router_is_same': main.traceability_router is traceability_router,
        'app_routes': [
            {{
                'type': f'{{type(route).__module__}}.{{type(route).__name__}}',
                'path': getattr(route, 'path', None),
                'repr': repr(route),
            }}
            for route in main.app.routes
        ],
    }}
    raise AssertionError(diagnostics)
"""
    completed = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=root_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, (
        "P1C endpoint was not registered on isolated ASGI cold start.\n"
        f"STDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
    )
