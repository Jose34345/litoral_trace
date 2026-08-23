from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
STATIC_SRC = ROOT / "src" / "litoral_trace" / "static" / "src"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_k_base_loads_mobile_motion_assets_once() -> None:
    base = _read(TEMPLATES / "base.html")

    assert base.count("/src/mobile-motion.css") == 1
    assert base.count("/src/js/mobile-motion.js") == 1


def test_k_app_shell_exposes_progressive_reveal_root_without_replacing_navigation_contract() -> None:
    shell = _read(TEMPLATES / "app" / "base_app.html")

    assert "data-auto-reveal" in shell
    assert 'id="app-navigation"' in shell
    assert 'data-app-drawer' in shell
    assert 'data-app-drawer-open' in shell
    assert 'data-app-drawer-close' in shell
    assert 'id="main-content"' in shell


def test_k_scroll_reveal_is_intersection_observer_based_and_once_only() -> None:
    controller = _read(STATIC_SRC / "js" / "mobile-motion.js")

    assert "IntersectionObserver" in controller
    assert "observer.unobserve(entry.target)" in controller
    assert 'rootMargin: "0px 0px -7% 0px"' in controller
    assert "htmx:afterSwap" in controller
    assert "htmx:load" in controller
    assert "wheel" not in controller
    assert "touchmove" not in controller
    assert "scrollIntoView" not in controller


def test_k_motion_respects_reduced_motion_and_transform_only_reveal_geometry() -> None:
    styles = _read(STATIC_SRC / "mobile-motion.css")

    assert "@media (prefers-reduced-motion: reduce)" in styles
    assert "scroll-behavior: auto" in styles
    assert "opacity: 0" in styles
    assert "translate3d" in styles
    assert "lt-reveal-visible" in styles
    assert "transition: none" in styles


def test_k_dashboard_keeps_leaflet_contract_and_adds_mobile_lot_cards() -> None:
    dashboard = _read(TEMPLATES / "dashboard.html")

    for contract in (
        'id="map"',
        'id="map-scope"',
        "L.map('map'",
        "L.tileLayer(",
        "L.marker(",
        ".bindPopup(",
        ".fitBounds(",
        "/api/v1/lotes",
        "/api/v1/satellite/jobs",
    ):
        assert contract in dashboard

    assert 'id="lotes-mobile"' in dashboard
    assert "lt-mobile-lote-card" in dashboard
    assert "renderLotesMobile" in dashboard
    assert "lt-desktop-lote-table" in dashboard
    assert "lt-dashboard-map" in dashboard
    assert "invalidateSize" in dashboard


def test_k_mobile_controls_keep_minimum_touch_targets() -> None:
    styles = _read(STATIC_SRC / "mobile-motion.css")
    cockpit = _read(TEMPLATES / "app" / "_dashboard_traceability_cockpit.html")

    assert "min-height: 2.75rem" in styles
    assert "[data-app-drawer-close]" in styles
    assert ".lt-user-avatar" in styles
    assert "lt-dashboard-actions" in cockpit
    assert "min-h-11" in cockpit


def test_k_leaflet_container_is_explicitly_protected_from_auto_translate_reveal() -> None:
    controller = _read(STATIC_SRC / "js" / "mobile-motion.js")

    assert 'child.querySelector("#map, .leaflet-container")' in controller
    assert 'containsMap ? "fade" : "up"' in controller
