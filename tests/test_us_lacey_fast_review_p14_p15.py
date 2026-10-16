"""P1.4/P1.5 review acceptance: inline evidence, explicit choice and safe advance."""
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment
from litoral_trace.us_lacey.quick_review_eligibility import quick_accept_eligible
from litoral_trace.us_lacey.document_evidence_class import classify_document

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT / "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html"
DETAIL = ROOT / "src/litoral_trace/templates/us_lacey/operation_detail.html"
REVIEW = ROOT / "src/litoral_trace/us_lacey/review.py"


def _candidate(*, candidate_id=11, value="11250", confidence=0.50,
               source_id=20, validation_status="VALID", validation_error=None):
    return SimpleNamespace(
        id=candidate_id, original_value=value, normalized_value=value,
        confidence=confidence, source_document_id=source_id,
        source_page=1, validation_status=validation_status,
        validation_error=validation_error,
    )


def _field(*, name="entered_value", candidates=(), proposed="11250", source_id=20):
    return SimpleNamespace(
        id=991, field_name=name, label=name.replace("_", " ").title(),
        scope="LINE", line_reference="1", status="REVIEW",
        candidates=tuple(candidates), proposed_value=proposed, provenance="current_shipment",
        source_page=1 if source_id is not None else None, source_locator=None,
        source_assurance_document_id=source_id, effective_value=proposed,
    )


def _render_field(field):
    text = WORKSPACE.read_text(encoding="utf-8")
    begin = text.index("{% macro action_field(")
    end = text.index("{% endmacro %}", begin) + len("{% endmacro %}")
    only_macro = text[begin:end] + "{{ action_field(field, 1) }}"
    env = Environment(autoescape=True)
    return env.from_string(only_macro).render(
        field=field,
        detail=SimpleNamespace(public_id="ab570aba-9a07-42e9-baf2-155f6ba36e7f"),
        review_csrf={991: "sample-csrf"},
        field_input_values={}, field_errors={},
        missing_guidance=lambda *_: "Upload original evidence",
        action_status=lambda *_: "Needs confirmation",
        quick_accept_eligible=quick_accept_eligible,
        source_class_for_filename=classify_document,
        button=lambda text, **kwargs: '<button type="submit">' + text + '</button>',
    )


def test_single_current_candidate_has_inline_accept_with_exact_id_and_low_confidence():
    html = _render_field(_field(candidates=[_candidate(confidence=0.50)]))
    assert 'data-inline-candidate="11"' in html
    assert 'value="11"' in html
    assert 'name="candidate_id"' in html
    assert "Extraction confidence" not in html  # scores are reserved for technical audit
    assert 'data-quick-accept' in html
    assert "Confirm value" in html
    assert 'data-multiple-candidates' not in html
    assert 'name="csrf_token"' in html
    assert '/review/actions/fields/991' in html
    assert 'name="action" value="accept"' in html
    assert "Inspect original PDF" in html


def test_distinct_candidates_stay_explicit_and_never_auto_select():
    html = _render_field(_field(candidates=[
        _candidate(candidate_id=11, value="11250"),
        _candidate(candidate_id=22, value="11900"),
    ]))
    assert 'data-multiple-candidates' in html
    assert 'data-inline-candidate' not in html
    assert 'data-quick-accept' not in html
    assert 'value="11"' in html and 'value="22"' in html
    assert "Compare 2 candidates" in html


def test_missing_value_has_no_accept_but_can_request_evidence():
    html = _render_field(_field(candidates=(), proposed=None, source_id=None))
    assert 'data-quick-accept' not in html
    assert 'data-request-evidence' in html
    assert 'data-override-open' in html
    assert "No current linked source candidate." in html


def test_invalid_candidate_never_exposes_accept():
    for status, err, source_id in (
        ("INVALID", "Quantity invalid", 20),
        ("MISSING", None, 20),
        ("VALID", None, None),
    ):
        html = _render_field(_field(candidates=[
            _candidate(validation_status=status, validation_error=err, source_id=source_id)
        ]))
        assert 'data-quick-accept' not in html
        assert "Value cannot be accepted directly." in html


def test_review_required_metadata_is_human_decision_not_invalid_ppq():
    field = _field(name="entered_value", candidates=[
        _candidate(value="11250", confidence=0.50, validation_status="REVIEW_REQUIRED",
                   validation_error="Human review is required.")
    ])
    assert quick_accept_eligible(field, field.candidates[0])
    html = _render_field(field)
    assert 'data-quick-accept' in html
    assert "Confirm value" in html
    assert "Value cannot be accepted directly." not in html


def test_ppq_invalid_value_fails_closed_even_if_candidate_metadata_says_valid():
    invalid = _field(name="entered_value", proposed="not-a-number", candidates=[
        _candidate(value="not-a-number", validation_status="REVIEW_REQUIRED")
    ])
    assert not quick_accept_eligible(invalid, invalid.candidates[0])
    assert 'data-quick-accept' not in _render_field(invalid)
    conflict = _field(candidates=[_candidate()])
    conflict.status = "CONFLICT"
    assert not quick_accept_eligible(conflict, conflict.candidates[0])


def test_six_field_pack2_contract_keeps_entry_pending_and_no_implicit_approval():
    for name, value in (
        ("entered_value", "11250"),
        ("article_component", "solid bamboo coasters"),
        ("plant_quantity", "288"),
        ("metric_unit", "kg"),
        ("merchandise_description", "retail boxed 4 piece solid bamboo coaster sets"),
    ):
        html = _render_field(_field(name=name, proposed=value,
                                   candidates=[_candidate(value=value)]))
        assert value in html
        assert "Confirm value" in html
    entry = _render_field(_field(name="filing_entry_reference", proposed=None,
                                candidates=(), source_id=None))
    assert "Keep pending" in entry
    assert "Not yet issued by CBP" in entry
    assert 'data-quick-accept' not in entry


def test_advance_occurs_only_after_server_success_and_authoritative_rerender():
    text = WORKSPACE.read_text(encoding="utf-8")
    assert 'event.detail?.successful === true' in text
    assert 'pendingQuickAcceptance.serverSucceeded' in text
    assert 'if (priorField) return' in text
    assert "nextField.scrollIntoView" in text
    assert "focus({preventScroll:true})" in text
    assert "window.__ltReviewTabsBound" in text
    assert 'data-review-field-id' in text
    assert 'hx-target="#operation-workspace"' in text


def test_p15_reduces_duplicate_status_and_sticky_obstruction():
    text = WORKSPACE.read_text(encoding="utf-8")
    detail = DETAIL.read_text(encoding="utf-8")
    assert 'class="lt-sticky-action-bar"' not in text
    assert 'data-review-next-controls' in text
    assert 'data-review-kpis' in text
    assert 'data-review-kpi="action"' in text
    assert 'data-review-kpi="confirmed"' in text
    assert 'data-review-kpi="resolved"' in text
    assert 'data-review-kpi="documents"' not in text
    assert 'id="review-summary"' in text
    assert "need attention before generating the declaration package." not in text
    assert "Workflow stages · view progress" in detail
    assert 'data-workflow-stage-details' in detail
    assert 'data-secondary-reprocess' in detail
    assert 'data-unified-audit' in detail


def test_backend_review_contract_is_reused_not_bypassed():
    source = REVIEW.read_text(encoding="utf-8")
    for invariant in (
        'def review_us_lacey_field(',
        'UsLaceyFieldCandidate.organization_id == org_id',
        'UsLaceyFieldCandidate.operation_id == operation.id',
        'UsLaceyFieldCandidate.operation_field_id == field.id',
        'validate_ppq_value(field.field_name, proposed)',
        'journal_reviewed_field(',
        'V2 review source set is stale.',
    ):
        assert invariant in source


def test_derived_only_pack2_field_requests_original_instead_of_accept():
    field = _field(name="plant_quantity", proposed="288",
                   candidates=[_candidate(value="288")])
    field.source_filename = "05_Entry_Preparation_Worksheet_Shipment2.pdf"
    html = _render_field(field)
    assert "Derived worksheet" in html
    assert 'data-worksheet-not-authority' in html
    assert "Request original evidence" in html
    assert "View worksheet" in html
    assert 'data-quick-accept' not in html
    assert "Confirm value" not in html


def test_original_document_preserves_explicit_quick_review():
    field = _field(name="plant_quantity", proposed="288",
                   candidates=[_candidate(value="288")])
    field.source_filename = "supplier_material_origin.pdf"
    html = _render_field(field)
    assert 'data-quick-accept' in html
    assert "Request original evidence" not in html
