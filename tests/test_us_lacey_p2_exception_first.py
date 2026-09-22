from pathlib import Path
from types import SimpleNamespace

import pytest

from litoral_trace.us_lacey.batch_hardening import ShipmentBatchRejected
from litoral_trace.us_lacey.review_telemetry import parse_review_telemetry
from litoral_trace.us_lacey import workflow
from litoral_trace.web.us_lacey_operational_views import _review_field_groups


ROOT = Path(__file__).resolve().parents[1]
OPERATIONS_TEMPLATE = ROOT / "src/litoral_trace/templates/us_lacey/operations.html"
BASE_TEMPLATE = ROOT / "src/litoral_trace/templates/base.html"
FORM_CONTROLS_CSS = ROOT / "src/litoral_trace/static/src/form-controls.css"
DETAIL_TEMPLATE = ROOT / "src/litoral_trace/templates/us_lacey/operation_detail.html"
WORKSPACE_TEMPLATE = ROOT / "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html"
REGULATORY_TEMPLATE = ROOT / "src/litoral_trace/templates/us_lacey/fragments/regulatory_assessment_card.html"
FILE_INPUT_JS = ROOT / "src/litoral_trace/static/src/js/file-input.js"
WORKSPACE_JS = ROOT / "src/litoral_trace/static/src/js/us-lacey-workspace.js"


def _field(
    field_id: int,
    status: str,
    *,
    value: str | None,
    validation_status: str = "VALID",
    candidates=(),
):
    return SimpleNamespace(
        id=field_id,
        field_name="species",
        line_reference="1",
        scope="PLANT_LINE",
        status=status,
        proposed_value=value,
        effective_value=value,
        validation_status=validation_status,
        candidates=tuple(candidates),
    )


def test_review_projection_separates_action_supported_and_settled_states():
    detail = SimpleNamespace(fields=(
        _field(1, "SUPPORTED", value="brasiliensis"),
        _field(2, "CONFLICT", value="brasiliensis", validation_status="REVIEW_REQUIRED"),
        _field(3, "MISSING", value=None, validation_status="MISSING"),
        _field(4, "MATCHED", value="brasiliensis"),
        _field(5, "CONFLICT", value="brasiliensis", validation_status="REVIEW_REQUIRED"),
    ))

    attention, supported, settled = _review_field_groups(detail)

    assert [field.id for field in supported] == [1]
    assert [field.id for field in attention] == [2, 3, 5]
    assert [field.id for field in settled] == [4]
    assert supported[0].status == "SUPPORTED"


def test_operations_intake_defends_against_cached_legacy_file_enhancer():
    template = OPERATIONS_TEMPLATE.read_text(encoding="utf-8")
    base = BASE_TEMPLATE.read_text(encoding="utf-8")
    css = FORM_CONTROLS_CSS.read_text(encoding="utf-8")

    assert "data-file-staging-form" in template
    assert "data-file-dropzone-input" in template
    assert "data-file-staging-list" in template
    assert "[data-file-staging-form] .lt-file-input" in css
    assert "?v=20260918-file-staging-2" in base


def test_operation_upload_uses_one_hidden_multi_file_input_and_staging_surface():
    template = DETAIL_TEMPLATE.read_text(encoding="utf-8")
    javascript = FILE_INPUT_JS.read_text(encoding="utf-8")

    assert "data-file-staging-form" in template
    assert 'id="operation-documents" hidden name="documents" type="file"' in template
    assert "multiple required" in template
    assert "data-file-dropzone" in template
    assert "data-file-staging-list" in template
    assert "Drop shipment files here or click to browse" in template

    assert 'input.hasAttribute("data-file-dropzone-input")' in javascript
    assert "new DataTransfer()" in javascript
    assert 'remove.setAttribute("aria-label", `Remove ${file.name}`)' in javascript


def test_batch_rejection_is_detected_before_any_persistent_ingestion(monkeypatch):
    checked = []

    def fake_budget(*, filename, content):
        checked.append((filename, content))
        if filename == "bulk.csv":
            raise ShipmentBatchRejected(
                "MULTI_SHIPMENT_DATASET",
                "This file contains multiple shipments.",
            )

    class NoWriteIngestion:
        def ingest_document(self, **_kwargs):
            raise AssertionError("ingestion must not begin before full batch validation")

    monkeypatch.setattr(workflow, "enforce_shipment_document_budget", fake_budget)

    with pytest.raises(workflow.UsLaceyWorkflowError, match="multiple shipments"):
        workflow.upload_and_enqueue_us_lacey_document_batch(
            organization_id=7,
            user_id=11,
            operation_public_id="80d62297-c3b5-4890-8217-a6131f89c315",
            documents=(
                ("invoice.pdf", "application/pdf", b"invoice", "UNKNOWN"),
                ("bulk.csv", "text/csv", b"bulk", "UNKNOWN"),
            ),
            ingestion=NoWriteIngestion(),
        )

    assert [item[0] for item in checked] == ["invoice.pdf", "bulk.csv"]


def test_exception_first_workspace_exposes_tabs_and_bulk_confirmation():
    template = WORKSPACE_TEMPLATE.read_text(encoding="utf-8")

    assert "Action Required" in template
    assert "Auto-Resolved Data" in template
    assert "Regulatory Analysis" in template
    assert "data-review-tab-button" in template
    assert "data-review-tab-panel" in template
    assert "data-review-line=" in template
    assert "Confirm All Auto-Resolved Data" in template
    assert "attention_fields|length == 0 and auto_supported_fields|length == 0" in template


def test_regulatory_fail_and_indeterminate_are_expanded_while_neutral_states_collapse():
    template = REGULATORY_TEMPLATE.read_text(encoding="utf-8")

    assert "data-regulatory-exceptions" in template
    assert "failed + indeterminate" in template
    assert "data-regulatory-passed" in template
    assert "data-regulatory-not-applicable" in template
    assert "NOT APPLICABLE" in template
    assert "<details" in template
    assert "They are not an overall legal compliance determination." in template


def test_review_telemetry_parser_is_bounded_deduplicated_and_fail_open():
    telemetry = parse_review_telemetry(
        started_at="2026-09-18T15:30:00Z",
        elapsed_seconds="91",
        modified_field_ids="4,4,9,-2,garbage,12",
    )
    assert telemetry.started_at == "2026-09-18T15:30:00+00:00"
    assert telemetry.elapsed_seconds == 91
    assert telemetry.modified_field_ids == (4, 9, 12)

    malformed = parse_review_telemetry(
        started_at="not-a-date",
        elapsed_seconds="-1",
        modified_field_ids="x,y",
    )
    assert malformed.started_at is None
    assert malformed.elapsed_seconds is None
    assert malformed.modified_field_ids == ()


def test_review_telemetry_tracks_only_explicit_review_required_edit_inputs():
    template = WORKSPACE_TEMPLATE.read_text(encoding="utf-8")
    javascript = WORKSPACE_JS.read_text(encoding="utf-8")

    assert 'data-review-required="true"' in template
    assert "data-review-started-at" in template
    assert "data-review-elapsed-seconds" in template
    assert "data-review-modified-field-ids" in template
    assert '[data-review-field][data-review-required="true"] input[name="value"]' in javascript
    assert "sessionStorage" in javascript
    assert 'document.addEventListener("submit"' not in javascript
