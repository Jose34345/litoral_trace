from pathlib import Path
from types import SimpleNamespace

from litoral_trace.api.pilot_readiness import router as pilot_api_router
from litoral_trace.web.navigation import build_navigation
from litoral_trace.web.pilot_readiness import router as pilot_web_router


ROOT = Path(__file__).resolve().parents[1]


def test_p1f_navigation_and_routes_are_read_only_and_discoverable() -> None:
    admin = SimpleNamespace(role="admin")
    navigation = build_navigation(admin, current_path="/pilot-readiness")
    pilot_items = [item for item in navigation if item.key == "pilot_readiness"]
    assert len(pilot_items) == 1
    assert pilot_items[0].label == "Preparar piloto"
    assert pilot_items[0].href == "/pilot-readiness"
    assert pilot_items[0].active is True

    api_routes = {(method, route.path) for route in pilot_api_router.routes for method in route.methods}
    assert api_routes == {("GET", "/api/v1/pilot-readiness")}

    web_routes = {(method, route.path) for route in pilot_web_router.routes for method in route.methods}
    assert web_routes == {("GET", "/pilot-readiness")}


def test_p1f_surface_does_not_block_pilot_on_acceptance_or_enable_live() -> None:
    api_source = (ROOT / "src/litoral_trace/api/pilot_readiness.py").read_text(encoding="utf-8")
    template = (ROOT / "src/litoral_trace/templates/pilot_readiness.html").read_text(encoding="utf-8")
    service = (ROOT / "src/litoral_trace/services/pilot_readiness.py").read_text(encoding="utf-8")

    assert '"acceptance_smoke_required_for_pilot": False' in api_source
    assert '"live_eudr_enabled": False' in api_source
    assert "El smoke remoto de ACCEPTANCE no bloquea este piloto" in template
    assert "PILOT_READY" in service
    assert "EudrAcceptance" not in service
    assert "eudr_acceptance_attempts" not in service
