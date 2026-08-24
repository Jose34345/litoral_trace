from __future__ import annotations

from pathlib import Path

from jinja2 import Environment


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
STATIC_SRC = ROOT / "src" / "litoral_trace" / "static" / "src"

CUSTOMER_SURFACES = (
    "integrations.html",
    "eudr_dds_candidate.html",
    "eudr_acceptance_transport.html",
    "shipment_export_case.html",
    "shipment_phytosanitary_case.html",
    "traceability_operations.html",
    "traceability.html",
    "traceability_release_control.html",
    "traceability_evidence.html",
)


def _template(name: str) -> str:
    return (TEMPLATES / name).read_text(encoding="utf-8")


def test_k1_customer_surfaces_parse_and_hide_roadmap_copy() -> None:
    environment = Environment()
    rendered_sources = []
    for name in CUSTOMER_SURFACES:
        source = _template(name)
        environment.parse(source)
        rendered_sources.append(source)

    combined = "\n".join(rendered_sources)
    forbidden = (
        "P1-A · Integration Core",
        "P1-B · Corrientes + ARCA",
        "P1-C · SENASA + ePhyto",
        "P1-D · API V3",
        "P1-D2 · API V3",
        "Staging only:",
        "Staging ERP",
        "Bridge genérico",
        "Readiness documental",
        "Readiness fitosanitario",
        "Evidence Vault",
        "Vault único",
        "Dossier de origen",
        "Manifest técnico",
        "Código técnico:",
        "manifest canónico",
        "Huella Documental",
    )
    for token in forbidden:
        assert token not in combined


def test_k1_integrations_localizes_display_without_changing_contract_values() -> None:
    source = _template("integrations.html")
    assert "Integración ERP" in source
    assert "Recepción controlada:" in source
    assert "Conciliada" in source
    assert "Pendiente" in source
    assert "ERP-JSON" in source
    assert "entity.status == 'RECONCILED'" in source
    assert "entity.status == 'CONFLICT'" in source
    assert "connection.connector_type == 'GENERIC_JSON'" in source
    assert "connection.status == 'ACTIVE'" in source


def test_k1_eudr_copy_keeps_acceptance_and_live_fail_closed_boundary() -> None:
    candidate = _template("eudr_dds_candidate.html")
    transport = _template("eudr_acceptance_transport.html")
    assert "ACCEPTANCE es un entorno de prueba y no acredita cumplimiento legal" in candidate
    assert "LIVE no está habilitado desde esta pantalla" in candidate
    assert "ACCEPTANCE no demuestra cumplimiento EUDR" in transport
    assert "LIVE no está habilitado desde este módulo" in transport
    assert "nunca se reintentan automáticamente" in transport


def test_k1_operations_and_traceability_use_customer_language() -> None:
    operations = _template("traceability_operations.html")
    traceability = _template("traceability.html")
    release = _template("traceability_release_control.html")

    assert "ledger" not in operations.lower()
    assert "contabilización" in operations
    assert "Expediente de origen" in traceability
    assert "manifiesto técnico" in traceability
    assert "Referencia técnica:" in traceability
    assert "manifiesto canónico" in release
    assert "huella documental" in release


def test_k1_file_picker_is_progressive_and_preserves_native_inputs() -> None:
    script = (STATIC_SRC / "js" / "file-input.js").read_text(encoding="utf-8")
    css = (STATIC_SRC / "mobile-motion.css").read_text(encoding="utf-8")
    base = _template("base.html")

    assert 'input[type="file"]' in script
    assert "data-file-input-enhanced" in script
    assert "Seleccionar archivo" in script
    assert "Ningún archivo seleccionado" in script
    assert "htmx:load" in script
    assert ".lt-file-input__native" in css
    assert ".lt-file-input__name" in css
    assert "#evidence-title ~ .mt-5" in css
    assert "path='/src/js/file-input.js'" in base

    for name in ("batch_import.html", "vault.html", "traceability_evidence.html"):
        assert 'type="file"' in _template(name)
