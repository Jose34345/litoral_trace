"""API registration contract for the P1C reverse genealogy endpoint."""
from __future__ import annotations

from pathlib import Path
import runpy


EXPECTED_PATH = "/api/v1/traceability/shipments/{shipment_code}/origin"


def test_p1c_origin_endpoint_is_registered_once() -> None:
    """Load a fresh ASGI entrypoint instead of reusing suite-mutated globals."""
    main_path = Path(__file__).resolve().parents[1] / "main.py"
    namespace = runpy.run_path(
        str(main_path),
        run_name="p1c_main_registration_probe",
    )
    app = namespace["app"]

    paths = [
        route.path
        for route in app.routes
        if getattr(route, "path", None) == EXPECTED_PATH
    ]
    assert paths == [EXPECTED_PATH]
