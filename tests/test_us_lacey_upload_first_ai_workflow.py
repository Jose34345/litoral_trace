import ast
from pathlib import Path
from types import SimpleNamespace

from litoral_trace.web.us_lacey_operational_views import _review_field_groups


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
    assert "Best practice: upload all available shipment documents before the first processing run." in source
    assert "You can add more files later and reprocess." in source
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
    assert '<label for="intake-documents"' in source
    assert 'id="intake-documents" hidden name="documents" type="file"' in source
    assert 'aria-describedby="intake-file-summary"' in source
    assert "data-file-staging-form" in source
    assert "data-file-dropzone" in source
    assert "data-file-dropzone-input" in source
    assert "data-file-staging-list" in source
    assert ">No files selected.</div>" in source
    assert 'style=' not in source


def test_supported_values_are_separate_from_action_required_and_confirmed_data():
    detail = SimpleNamespace(
        fields=(
            _field("SUPPORTED", "MSKU9228574"),
            _field("MATCHED", "123-4567890-1"),
            _field("MISSING", None),
        )
    )
    attention, supported, settled = _review_field_groups(detail)
    assert [field.status for field in attention] == ["MISSING"]
    assert [field.status for field in supported] == ["SUPPORTED"]
    assert [field.status for field in settled] == ["MATCHED"]



def test_workspace_offers_safe_bulk_confirmation_but_keeps_conflicts_explicit():
    source = WORKSPACE_TEMPLATE.read_text(encoding="utf-8")
    assert "/review/actions/accept-supported" in source
    assert "/review/actions/fields/" in source
    assert "Confirm All Auto-Resolved Data" in source
    assert "Action Required" in source
    assert "Auto-Resolved Data" in source
    assert "genuinely different supported values" in source.lower()
    assert 'field.status == "CONFLICT"' in source
    assert "Regulatory Analysis" in source



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
    tree = ast.parse(routes)
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    create_call = next(
        call for call in calls
        if isinstance(call.func, ast.Name) and call.func.id == "create_us_lacey_customer_operation"
    )
    create_keywords = {keyword.arg: keyword.value for keyword in create_call.keywords}
    assert isinstance(create_keywords["line_references"], ast.Tuple)
    assert [element.value for element in create_keywords["line_references"].elts] == ["1"]

    batch_call = next(
        call for call in calls
        if isinstance(call.func, ast.Name) and call.func.id == "upload_and_enqueue_us_lacey_document_batch"
    )
    batch_keywords = {keyword.arg: keyword.value for keyword in batch_call.keywords}
    assert "documents" in batch_keywords
    assert any(
        isinstance(node, ast.Constant) and node.value == "UNKNOWN"
        for node in ast.walk(batch_keywords["documents"])
    )
    assert "intelligent_workflow_router" in unified
    assert "app.include_router(intelligent_workflow_router)" in unified


def test_direct_completion_is_blocked_while_auto_resolved_data_is_unconfirmed():
    unified = UNIFIED_APP.read_text(encoding="utf-8")
    assert "_require_explicit_confirmation_before_completion" in unified
    assert 'any(field.status == "SUPPORTED" for field in detail.fields)' in unified
    assert "Confirm all supported suggestions before completing preparation." in unified
    assert "status_code=409" in unified
