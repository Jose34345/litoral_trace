"""API registration contract for the P1C reverse genealogy endpoint."""
from __future__ import annotations

import main


def test_p1c_origin_endpoint_is_registered_once() -> None:
    paths = [
        route.path
        for route in main.app.routes
        if getattr(route, "path", None)
        == "/api/v1/traceability/shipments/{shipment_code}/origin"
    ]
    assert paths == [
        "/api/v1/traceability/shipments/{shipment_code}/origin"
    ]
