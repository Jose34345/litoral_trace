from __future__ import annotations

from pathlib import Path

from litoral_trace.web.navigation import build_navigation


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "src/litoral_trace/templates/assurance_workspace.html"


def test_workspace_is_one_document_first_entry_without_redundant_business_form():
    html = TEMPLATE.read_text(encoding="utf-8")
    assert "Agregar documentos/datos de operación" in html
    assert 'type="file" multiple' in html
    assert "Arrastrá los archivos acá" in html
    assert "No hace falta completar un formulario antes de subirlos." in html
    assert "PDF, XLSX, XLS o CSV" in html
    # v1 starts from files already used by the company; it does not demand a
    # duplicate supplier/order/lot form before ingestion.
    assert 'name="supplier"' not in html
    assert 'name="lot"' not in html
    assert 'name="order"' not in html


def test_workspace_reports_automation_and_only_renders_pending_field_review():
    html = TEMPLATE.read_text(encoding="utf-8")
    assert "Datos detectados" in html
    assert "Aceptados automáticamente" in html
    assert "Necesitan revisión" in html
    assert ".filter((field) => field.needs_review)" in html
    assert "Sólo requiere tu atención" in html
    assert "Aprobar seleccionados" in html
    assert "Seleccionar todo" in html


def test_workspace_uses_existing_cookie_csrf_contract_for_unsafe_api_calls():
    html = TEMPLATE.read_text(encoding="utf-8")
    assert "data-csrf-token" in html
    assert "data-csrf-header" in html
    assert "[csrfHeader]: csrfToken" in html
    assert "credentials: 'same-origin'" in html


def test_workspace_navigation_is_hidden_by_default_and_visible_only_when_enabled(monkeypatch):
    monkeypatch.delenv("LT_ASSURANCE_V1_ENABLED", raising=False)
    monkeypatch.delenv("LT_ASSURANCE_DOCUMENT_INTELLIGENCE_ENABLED", raising=False)
    hidden = {item.key for item in build_navigation("admin", current_path="/dashboard")}
    assert "assurance_workspace" not in hidden

    monkeypatch.setenv("LT_ASSURANCE_V1_ENABLED", "1")
    monkeypatch.setenv("LT_ASSURANCE_DOCUMENT_INTELLIGENCE_ENABLED", "1")
    visible = {
        item.key: item
        for item in build_navigation("admin", current_path="/api/v1/assurance/workspace")
    }
    assert visible["assurance_workspace"].active is True
    assert visible["assurance_workspace"].href == "/api/v1/assurance/workspace"
