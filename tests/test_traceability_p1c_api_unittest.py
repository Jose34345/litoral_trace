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
import main
expected = {EXPECTED_PATH!r}
paths = [
    route.path
    for route in main.app.routes
    if getattr(route, 'path', None) == expected
]
assert paths == [expected], [
    getattr(route, 'path', None)
    for route in main.app.routes
]
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
