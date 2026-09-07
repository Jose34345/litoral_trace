from pathlib import Path
from types import SimpleNamespace

from litoral_trace.web.us_lacey_operational_views import _review_field_sets


ROOT = Path(__file__).resolve().parents[1]
OPERATIONS_TEMPLATE = ROOT / "src/litoral_trace/templates/us_lacey/operations.html"
OPERATION_DETAIL_TEMPLATE = ROOT / "src/litoral_trace/templates/us_lacey/operation_detail.html"
WORKSPACE_TEMPLATE = ROOT / "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html"
UNIFIED_APP = ROOT / "src/litoral_trace/web/us_lacey_unified_app.py"
INTELLIGENT_ROUTES = ROOT / "src/litoral_trace/web/us_lacey_intelligent_workflow.py"


def _field(status: str, value=None):
    return SimpleNamespace(status=status, effective_value=value)


def test_operations_starts_with_multi_file_upload_and_no_manual_metadata_fields():
    source = OPERATIONS_TEMPLATE.read_text(encoding="utf-8")
    assert 'action="/operations/intake"' in source
    assert 'name="documents"' in source
    assert "multiple required" in source
    assert "No importer, consignee, broker, date or line references are required up front." in source
    for old_manual_name in (
        'name="client_reference"',
        'name="importer_name"',
        'name="supplier_name"',
        'name="consignee_name"',
        'name="broker_name"',
        'name="operation_date"',
        'name="line_references"',
    ):
        assert old_manual_name not in source


def test_operations_hides_locale_dependent_native_file_picker_chrome():
    source = OPERATIONS_TEMPLATE.read_text(encoding="utf-8")
    assert 'id="intake-documents"' in source
    assert 'class="absolute inset-0 z-10 h-full w-full cursor-pointer opacity-0"' in source
    assert 'aria-describedby="intake-file-summary"' in source
    assert ">No files selected.</div>" in source
    assert 'style=' not in source


def test_found_values_are_suggestions_not_confirmed_data():
    detail = SimpleNamespace(
        fields=(
            _field("FOUND", "MSKU9228574"),
            _field("MATCHED", "123-4567890-1"),
            _field("MISSING", None),
        )
    )
    exceptions, settled = _review_field_sets(detail)
    assert [field.status for field in exceptions] == ["FOUND", "MISSING"]
    assert [field.status for field in settled] == ["MATCHED"]


def test_workspace_offers_safe_bulk_confirmation_but_keeps_conflicts_explicit():
    source = WORKSPACE_TEMPLATE.read_text(encoding="utf-8")
    assert "/review/accept-supported" in source
    assert "Accept all safe suggestions" in source
    assert "Conflicting values found." in source
    assert "The prefilled value may be wrong." in source
    assert 'field.status == "FOUND"' in source
    assert "/review-supported/" in source
    assert "Country of origin alone is not treated as proof." in source
    assert "Shipment gross weight is not substituted automatically." in source


def test_operation_detail_keeps_document_type_override_advanced_and_evidence_collapsed():
    source = OPERATION_DETAIL_TEMPLATE.read_text(encoding="utf-8")
    assert "Advanced: set document type manually" in source
    assert '<option value="UNKNOWN" selected>Auto-detect</option>' in source
    assert '<details id="engine2-dossier"' in source
    assert "Advanced evidence details" in source


def test_zero_entry_intake_is_mounted_and_creates_default_line_without_regulatory_inference():
    routes = INTELLIGENT_ROUTES.read_text(encoding="utf-8")
    unified = UNIFIED_APP.read_text(encoding="utf-8")
    assert '@router.post("/operations/intake"' in routes
    assert 'line_references=("1",)' in routes
    assert 'document_role="UNKNOWN"' in routes
    assert "intelligent_workflow_router" in unified
    assert "app.include_router(intelligent_workflow_router)" in unified


def test_direct_completion_is_blocked_while_supported_suggestions_are_unconfirmed():
    unified = UNIFIED_APP.read_text(encoding="utf-8")
    assert "_require_explicit_confirmation_before_completion" in unified
    assert 'any(field.status == "FOUND" for field in detail.fields)' in unified
    assert "Confirm all supported suggestions before completing preparation." in unified
    assert "status_code=409" in unified
