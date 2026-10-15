"""Visible review UX invariants: accessible control, evidence authority, no flooded list."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "src/litoral_trace/templates/us_lacey/fragments/applicability_triage.html").read_text(encoding="utf-8")
OP = (ROOT / "src/litoral_trace/templates/us_lacey/operation_detail.html").read_text(encoding="utf-8")
WORKSPACE = (ROOT / "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html").read_text(encoding="utf-8")
CSS = (ROOT / "src/litoral_trace/static/css/us_lacey_source_inspector.css").read_text(encoding="utf-8")
JS = (ROOT / "src/litoral_trace/static/js/us_lacey_source_inspector.js").read_text(encoding="utf-8")


def test_one_entry_type_control_never_renders_unbounded_alternative_list():
    assert APP.count('name="entry_type"') == 2  # scenario select + posted attestation snapshot
    assert 'data-entry-type' in APP
    assert 'data-entry-search' not in APP
    assert 'data-entry-option' not in APP
    assert 'data-entry-combobox' not in APP
    assert 'class="lt-entry-native' in APP
    assert 'aria-describedby="entry-type-hint-' in APP
    assert 'option.code' in APP  # ALL supported CBP codes still available
    assert "if (!nativeSelect || !search || !current || options.length === 0) return;" in JS


def test_review_is_primary_navigation_and_advanced_attestation_is_progressive():
    assert OP.index('id="shipment-summary"') < OP.index('id="processing-status"') < OP.index('fragments/applicability_triage.html')
    assert 'data-applicability-authorized-review' in APP
    assert 'class="lt-attestation-details"' in APP
    assert 'Record an evidence-backed determination' in APP
    assert 'name="explicitly_confirmed"' in APP
    assert 'name="source_document_id"' in APP
    assert 'disabled' in APP  # derived originals not selectable
    assert 'name="csrf_token"' in APP


def test_versioned_css_and_legible_document_cards():
    assert "us_lacey_source_inspector.css') }}?v=20261015-ux4" in OP
    assert "us_lacey_source_inspector.js') }}?v=20261015-ux4" in OP
    assert 'class="lt-evidence-card__copy"' in APP
    assert "overflow-wrap:anywhere" in CSS
    assert ".lt-attestation-summary" in CSS
    assert ".lt-entry-native:focus-visible" in CSS
    assert ".lt-evidence-preview[hidden]" in CSS


def test_exception_queue_keeps_fail_closed_worksheet_action():
    assert 'data-worksheet-not-authority' in WORKSPACE
    assert 'data-request-evidence' in WORKSPACE
    assert 'Request original evidence' in WORKSPACE
    assert 'Confirm value' in WORKSPACE
    assert 'data-quick-accept' in WORKSPACE
