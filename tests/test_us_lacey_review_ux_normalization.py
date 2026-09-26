from dataclasses import dataclass
import inspect
from pathlib import Path

from litoral_trace.us_lacey.candidate_normalization import (
    group_candidate_evidence,
    merchandise_description_candidate_role,
)
from litoral_trace.us_lacey.ppq505 import (
    PpqValidationStatus,
    canonical_ppq_value_key,
    validate_ppq_value,
)
from litoral_trace.web.us_lacey_operational_views import (
    _auto_resolved_evidence_summary,
    _present_review_field,
    render_operation_workspace,
)


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

    presented = _present_review_field(field, all_fields=(field,))

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


def test_merchandise_description_pool_rejects_golden_packet_structural_noise():
    rejected = (
        ("Solid rubberwood plant material", "PLANT_COMPONENT_DESCRIPTION"),
        ("MDF plant material", "PLANT_COMPONENT_DESCRIPTION"),
        ("Metal fasteners / protective pads / adhesive", "PLANT_COMPONENT_DESCRIPTION"),
        ("Corrugated cartons, inserts, pallets and other packing", "PACKAGING_DESCRIPTION"),
    )
    candidates = tuple(
        _Candidate(index, value, value, confidence=0.87, source_page=2)
        for index, (value, _role) in enumerate(rejected, start=1)
    ) + (
        _Candidate(
            99,
            "Retail set: four solid rubberwood coasters with one MDF holder; natural finish; packed for retail sale",
            "Retail set: four solid rubberwood coasters with one MDF holder; natural finish; packed for retail sale",
            confidence=0.98,
            source_page=1,
        ),
    )

    for value, role in rejected:
        assert merchandise_description_candidate_role(value) == role
    assert merchandise_description_candidate_role(candidates[-1].original_value) is None

    groups = group_candidate_evidence("merchandise_description", candidates)
    assert len(groups) == 1
    assert groups[0].representative.id == 99


def test_exception_first_workspace_has_four_tabs_and_htmx_actions():
    template = Path(
        "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html"
    ).read_text(encoding="utf-8")

    assert 'data-review-tab-button="action"' in template
    assert 'data-review-tab-button="resolved"' in template
    assert 'data-review-tab-button="confirmed"' in template
    assert 'data-review-tab-button="regulatory"' in template
    assert 'data-review-tab-panel="action"' in template
    assert 'data-review-tab-panel="resolved"' in template
    assert 'data-review-tab-panel="confirmed"' in template
    assert 'data-review-tab-panel="regulatory"' in template
    assert "Action Required" in template
    assert "Auto-Resolved Data" in template
    assert "Confirmed <span" in template
    assert "Regulatory Analysis" in template
    assert "Confirm All Auto-Resolved Data" in template
    assert 'hx-target="#operation-workspace"' in template
    assert '/review/actions/fields/' in template
    assert '/review/actions/accept-supported' in template


def test_review_workspace_vanilla_js_updates_tabs_and_action_counts_after_htmx():
    template = Path(
        "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html"
    ).read_text(encoding="utf-8")

    assert 'data-review-tabs' in template
    assert 'htmx:afterSettle' in template
    assert 'htmx:afterSwap' in template
    assert 'data-action-required-count' in template
    assert 'data-action-tab-count' in template
    assert 'refreshActionCount(root)' in template
    assert 'activateTab(root, "action")' in template
    assert 'data-review-resolved="true"' in template



def test_disabled_complete_preparation_has_entered_value_reconciliation_tooltip():
    ui = Path("src/litoral_trace/templates/components/ui.html").read_text(encoding="utf-8")

    assert 'disabled and label == "Complete preparation"' in ui
    assert (
        "Cannot complete preparation: Please resolve the Entered Value reconciliation inconsistency first."
        in ui
    )


def test_exception_first_ui_polish_uses_quiet_enterprise_surfaces():
    workspace = Path(
        "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html"
    ).read_text(encoding="utf-8")
    regulatory = Path(
        "src/litoral_trace/templates/us_lacey/fragments/regulatory_assessment_card.html"
    ).read_text(encoding="utf-8")

    assert "lt-review-card--blocking" in workspace
    assert "lt-review-card--confirmation" in workspace
    assert "bg-emerald-50/40" not in workspace
    assert "border-emerald-200 bg-emerald-50/40" not in workspace
    assert "ring-emerald-600/20" in workspace
    assert "ring-amber-600/20" in workspace
    assert "focus:ring-2 focus:ring-emerald-600" in workspace
    assert 'button("Save", variant="secondary"' in workspace
    assert 'border border-slate-200 bg-white p-4 shadow-sm' in workspace
    assert "Source: Document evidence" in workspace
    assert "border-t border-slate-100 bg-slate-50 px-3 py-3" in workspace
    assert "semantically equivalent source observations were reconciled" not in workspace

    assert "U.S. Lacey ruleset" not in regulatory
    assert "border border-slate-200 border-l-4" in regulatory
    assert "bg-amber-50/80" not in regulatory
    assert "bg-rose-50/80" not in regulatory
    assert "ring-amber-600/20" in regulatory
    assert "bg-emerald-50/40" not in regulatory



@dataclass(frozen=True)
class _PresentedField:
    field_name: str
    proposed_value: str
    effective_value: str
    source_page: int | str | None
    source_assurance_document_id: int | None
    source_locator: str | None
    candidates: tuple[_Candidate, ...]
    scope: str = "SHIPMENT"


def test_auto_resolved_evidence_summary_deduplicates_pages_without_mutating_value():
    field = _PresentedField(
        field_name="species",
        proposed_value="grandis",
        effective_value="grandis",
        source_page=2,
        source_assurance_document_id=10,
        source_locator=None,
        candidates=(
            _Candidate(1, "grandis", "grandis", source_page=2),
            _Candidate(2, "grandis", "grandis", source_page=2),
            _Candidate(3, "grandis", "grandis", source_page=3),
        ),
    )

    [presented] = _auto_resolved_evidence_summary((field,))

    assert presented.proposed_value == "grandis"
    assert presented.source_page == "2, 3"
    assert "Evidence:" not in presented.proposed_value
