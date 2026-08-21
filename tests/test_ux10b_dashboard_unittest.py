from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
DASHBOARD = TEMPLATES / "dashboard.html"
COCKPIT = TEMPLATES / "app" / "_dashboard_traceability_cockpit.html"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_dashboard_uses_traceability_cockpit_as_primary_entry_point():
    dashboard = _read(DASHBOARD)

    assert '{% include "app/_dashboard_traceability_cockpit.html" %}' in dashboard
    assert "Origen forestal, geolocalización y evidencia" not in dashboard


def test_cockpit_searches_existing_traceability_workspace_without_new_mutation():
    cockpit = _read(COCKPIT)

    assert 'action="/traceability"' in cockpit
    assert 'method="get"' in cockpit
    assert 'name="shipment_code"' in cockpit
    assert 'maxlength="120"' in cockpit
    assert "Reconstruir origen" in cockpit
    assert "method=\"post\"" not in cockpit.lower()


def test_cockpit_explains_end_to_end_operational_flow():
    cockpit = _read(COCKPIT)

    for expected in (
        "Trazabilidad de despachos",
        "Origen registrado",
        "Cadena industrial",
        "Despacho",
        "Expediente",
        "PDF, GeoJSON y manifiesto de evidencia",
        "Base de origen",
        "Evidencia de apoyo",
    ):
        assert expected in cockpit


def test_cockpit_exposes_only_live_product_destinations():
    cockpit = _read(COCKPIT)

    for href in (
        "/traceability",
        "/imports",
        "/vault",
        "/api/v1/batch/template",
    ):
        assert f'href="{href}"' in cockpit


def test_authenticated_dashboard_preserves_real_origin_and_satellite_runtime():
    dashboard = _read(DASHBOARD)

    assert "/api/v1/lotes" in dashboard
    assert "/api/v1/satellite/jobs" in dashboard
    assert "Mapa geoespacial de rodales" in dashboard
    assert "Análisis NDVI persistente" in dashboard
    assert "No emite un certificado oficial EUDR" in dashboard


def test_cockpit_has_no_fake_commercial_or_compliance_metrics():
    content = _read(COCKPIT).lower()

    forbidden = (
        "exp-ue-2026-001",
        "99.9%",
        "100% compliant",
        "cumplimiento garantizado",
        "eudr compliant",
        "aprobado por traces",
        "certificado eudr",
        "riesgo bajo",
    )

    for claim in forbidden:
        assert claim not in content


def test_cockpit_keeps_geospatial_and_satellite_evidence_secondary():
    cockpit = _read(COCKPIT)

    assert "La evidencia geoespacial y satelital acompaña esa cadena; no la reemplaza." in cockpit
    assert "Parcelas, superficies, coordenadas y análisis satelitales" in cockpit


def test_cockpit_is_keyboard_and_screen_reader_friendly():
    cockpit = _read(COCKPIT)

    assert 'aria-labelledby="dashboard-traceability-heading"' in cockpit
    assert 'id="dashboard-traceability-heading"' in cockpit
    assert 'aria-label="Buscar trazabilidad de un despacho"' in cockpit
    assert 'for="dashboard-shipment-code"' in cockpit
    assert 'id="dashboard-shipment-code"' in cockpit
