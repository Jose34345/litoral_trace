from pathlib import Path

from jinja2 import Environment


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
STATIC_SRC = ROOT / "src" / "litoral_trace" / "static" / "src"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_p17_geospatial_layer_is_loaded_without_rewriting_dashboard_template() -> None:
    base = _read(TEMPLATES / "base.html")
    dashboard = _read(TEMPLATES / "dashboard.html")

    Environment().parse(base)
    Environment().parse(dashboard)

    assert "path='/src/geospatial.css'" in base
    assert 'id="map"' in dashboard
    assert 'id="map-scope"' in dashboard


def test_p17_geospatial_preserves_leaflet_runtime_contract() -> None:
    dashboard = _read(TEMPLATES / "dashboard.html")

    for contract in (
        "L.map('map'",
        "L.tileLayer(",
        "L.marker(",
        ".bindPopup(",
        ".fitBounds(",
        "document.getElementById('map-scope')",
        "/api/v1/lotes",
        "/api/v1/satellite/jobs",
    ):
        assert contract in dashboard

    assert "Mapa geoespacial de rodales" in dashboard
    assert "Muestra únicamente los lotes reales disponibles para tu organización." in dashboard


def test_p17_geospatial_preserves_historical_map_tailwind_candidates() -> None:
    dashboard = _read(TEMPLATES / "dashboard.html")

    # I is progressive enhancement: the existing map geometry and Tailwind
    # candidates remain untouched so dist/app.css and Leaflet sizing stay stable.
    assert 'id="map" class="h-96 w-full rounded-xl border border-slate-300 bg-slate-100"' in dashboard
    assert 'id="map-scope" class="text-xs font-semibold text-slate-500"' in dashboard


def test_p17_geospatial_css_styles_existing_hooks_and_leaflet_controls() -> None:
    css = _read(STATIC_SRC / "geospatial.css")

    for selector in (
        "#map-scope",
        "#map-scope::before",
        "#map",
        "#map:focus-visible",
        "#map .leaflet-control-zoom",
        "#map .leaflet-popup-content-wrapper",
        "#map .leaflet-popup-content",
        "#map .leaflet-control-attribution",
        ".lt-geo-panel",
        ".lt-geo-panel__header",
        ".lt-geo-panel__viewport",
        ".lt-geo-panel__meta",
        ".lt-geo-legend",
    ):
        assert selector in css

    assert "min-height: 24rem" in css
    assert "@media (max-width: 639px)" in css
    assert "@media (prefers-reduced-motion: reduce)" in css


def test_p17_geospatial_does_not_fake_risk_or_compliance_semantics() -> None:
    css = _read(STATIC_SRC / "geospatial.css").lower()

    for forbidden in (
        "eudr compliant",
        "certified",
        "approved",
        "low risk",
        "high risk",
    ):
        assert forbidden not in css
