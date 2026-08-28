from __future__ import annotations

from pathlib import Path

from litoral_trace.api.assurance_preflight import (
    AssurancePreflightRequest,
    build_preflight_input,
)
from litoral_trace.assurance.preflight import PreflightStatus, evaluate_preflight
from litoral_trace.web.navigation import build_navigation


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "src/litoral_trace/templates/assurance_workspace.html"


def test_workspace_is_one_document_first_entry_without_redundant_business_form():
    html = TEMPLATE.read_text(encoding="utf-8")
    assert "Agregar documentos/datos de operación" in html
    assert 'type="file" multiple' in html
    assert "Arrastrá los archivos acá" in html
    assert "event.dataTransfer.files" in html
    assert "No hace falta completar un formulario antes de subirlos." in html
    assert "PDF, XLSX, XLS o CSV" in html
    # v1 starts from files already used by the company; it does not demand a
    # duplicate supplier/order/lot form before ingestion.
    assert 'name="supplier"' not in html
    assert 'name="lot"' not in html
    assert 'name="order"' not in html


def test_workspace_shows_automatic_values_and_only_pending_fields_are_editable():
    html = TEMPLATE.read_text(encoding="utf-8")
    assert "Datos detectados" in html
    assert "Aceptados automáticamente" in html
    assert "Necesitan revisión" in html
    assert "Datos obtenidos automáticamente" in html
    assert ".filter((field) => field.auto_accepted)" in html
    assert ".filter((field) => field.needs_review)" in html
    assert "Sólo requiere tu atención" in html
    assert "Confianza" in html


def test_workspace_supports_document_and_batch_bulk_approval():
    html = TEMPLATE.read_text(encoding="utf-8")
    assert "Aprobar seleccionados" in html
    assert "Seleccionar todos los pendientes" in html
    assert "Aprobar selección masiva" in html
    assert "const grouped = new Map()" in html
    assert "review/approve" in html


def test_workspace_reports_created_updated_and_immediate_discrepancy_outcome():
    html = TEMPLATE.read_text(encoding="utf-8")
    assert "Registros creados" in html
    assert "Registros actualizados" in html
    assert "Discrepancias abiertas" in html
    assert "Discrepancias detectadas inmediatamente" in html
    assert "reconciliation_created_count" in html
    assert "reconciliation_refreshed_count" in html
    assert "review.issues" in html


def test_workspace_waits_for_full_pipeline_then_runs_preflight_automatically():
    html = TEMPLATE.read_text(encoding="utf-8")
    assert 'data-preflight-url="{{ assurance_preflight_url }}"' in html
    assert "metadata.pipeline_completed === true" in html
    assert "runAutomaticPreflight()" in html
    assert "Preflight automático" in html
    assert "Completar sólo lo que falta" in html
    assert "Reevaluar Preflight" in html
    assert "terminalDocuments !== totalDocuments" in html


def test_partial_automatic_preflight_is_fail_closed_instead_of_requiring_a_form_first():
    request = AssurancePreflightRequest(operation_reference="assurance-batch:test")
    result = evaluate_preflight(build_preflight_input(request))
    assert result.status == PreflightStatus.BLOCKED
    assert "INVALID_MINIMUM_INPUT" in result.reason_codes
    assert "ORIGIN_UNASSESSED" in result.reason_codes
    assert "GENEALOGY_UNASSESSED" in result.reason_codes


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
