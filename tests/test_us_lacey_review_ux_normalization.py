from dataclasses import dataclass
from pathlib import Path

from litoral_trace.us_lacey.candidate_normalization import group_candidate_evidence
from litoral_trace.us_lacey.ppq505 import (
    PpqValidationStatus,
    canonical_ppq_value_key,
    validate_ppq_value,
)
from litoral_trace.web.us_lacey_operational_views import _present_review_field


@dataclass(frozen=True)
class _Candidate:
    id: int
    original_value: str
    normalized_value: str | None
    validation_status: str = "VALID"
    validation_error: str | None = None
    confidence: float = 0.90
    source_document_id: int = 10
    source_page: int | None = 4
    source_locator: str | None = None
    decision: str = "PENDING"


@dataclass(frozen=True)
class _Field:
    field_name: str
    candidates: tuple[_Candidate, ...]


def test_formatted_entered_value_is_sanitized_before_decimal_validation():
    result = validate_ppq_value("entered_value", "$18,600.00")

    assert result.status is PpqValidationStatus.VALID
    assert result.normalized_value == "18600"
    assert result.error is None


def test_numeric_sanitizer_handles_grouping_and_unicode_spaces_without_guessing_text():
    quantity = validate_ppq_value("plant_quantity", " 1,\u202f440.00 ")
    malformed = validate_ppq_value("entered_value", "USD $18,600.00")

    assert quantity.status is PpqValidationStatus.VALID
    assert quantity.normalized_value == "1440"
    assert malformed.status is PpqValidationStatus.INVALID
    assert malformed.error == "Entered value must be numeric."


def test_candidate_identity_ignores_case_whitespace_page_and_confidence_metadata():
    assert canonical_ppq_value_key("species", " brasiliensis ") == canonical_ppq_value_key(
        "species", "BRASILIENSIS"
    )
    assert canonical_ppq_value_key("entered_value", "$18,600.00") == canonical_ppq_value_key(
        "entered_value", "18600"
    )


def test_same_species_evidence_collapses_to_one_group_and_uses_highest_confidence():
    candidates = (
        _Candidate(1, "brasiliensis", "brasiliensis", confidence=0.90, source_page=4),
        _Candidate(2, "BRASILIENSIS", "BRASILIENSIS", confidence=0.98, source_page=4),
    )

    groups = group_candidate_evidence("species", candidates)

    assert len(groups) == 1
    assert groups[0].representative.id == 2
    assert groups[0].confidence == 0.98
    assert groups[0].source_pages == (4,)
    assert len(groups[0].evidence) == 2


def test_same_component_across_pages_merges_references_for_review_display():
    field = _Field(
        field_name="article_component",
        candidates=(
            _Candidate(1, "Solid rubberwood coasters", "Solid rubberwood coasters", source_page=4),
            _Candidate(2, "solid rubberwood coasters", "solid rubberwood coasters", confidence=0.98, source_page=5),
        ),
    )

    presented = _present_review_field(field)

    assert len(presented.candidates) == 1
    assert presented.candidates[0].id == 2
    assert presented.candidates[0].confidence == 0.98
    assert presented.candidates[0].source_page == "4, 5"


def test_genuinely_different_entered_values_remain_separate_candidate_groups():
    candidates = (
        _Candidate(1, "$18,600.00", "18600", confidence=0.98, source_page=1),
        _Candidate(2, "$14,880.00", "14880", confidence=0.98, source_page=5),
    )

    groups = group_candidate_evidence("entered_value", candidates)

    assert len(groups) == 2
    assert {group.canonical_value for group in groups} == {"18600", "14880"}


def test_review_workspace_js_uses_htmx_and_scrolls_to_next_review_item():
    source = Path("src/litoral_trace/static/src/js/us-lacey-workspace.js").read_text(encoding="utf-8")
    template = Path(
        "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html"
    ).read_text(encoding="utf-8")

    assert 'document.addEventListener("submit"' not in source
    assert "fetch(form.action" not in source
    assert 'document.addEventListener("htmx:beforeRequest"' in source
    assert 'document.addEventListener("htmx:afterSwap"' in source
    assert 'scrollIntoView({ behavior: "smooth", block: "center" })' in source
    assert "restoreViewport(viewport.x, viewport.y)" in source

    assert 'hx-target="#operation-workspace"' in template
    assert 'hx-swap="outerHTML"' in template
    assert '/review/actions/fields/' in template
    assert 'data-reconciliation-invariant="entered-value"' in template
    assert 'aria-invalid="true"' in template
    assert 'button_type="submit", disabled=True' in template


def test_disabled_complete_preparation_has_entered_value_reconciliation_tooltip():
    ui = Path("src/litoral_trace/templates/components/ui.html").read_text(encoding="utf-8")

    assert 'disabled and label == "Complete preparation"' in ui
    assert (
        "Cannot complete preparation: Please resolve the Entered Value reconciliation inconsistency first."
        in ui
    )
