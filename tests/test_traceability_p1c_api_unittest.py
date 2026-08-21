"""API registration contract for the P1C reverse genealogy endpoint."""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys


EXPECTED_PATH = "/api/v1/traceability/shipments/{shipment_code}/origin"


def test_p1c_origin_endpoint_is_registered_once() -> None:
    """Validate the effective route tree in a fresh ASGI worker process."""
    root_dir = Path(__file__).resolve().parents[1]
    probe = f"""
import main
expected = {EXPECTED_PATH!r}
schema = main.app.openapi()
assert expected in schema['paths'], sorted(schema['paths'])
methods = schema['paths'][expected]
assert 'get' in methods, methods
"""
    completed = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=root_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, (
        "P1C endpoint was not exposed on isolated ASGI cold start.\n"
        f"STDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
    )
