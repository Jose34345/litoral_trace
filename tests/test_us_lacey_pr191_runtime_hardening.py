from __future__ import annotations

from dataclasses import dataclass
import inspect
from types import SimpleNamespace

from litoral_trace.us_lacey.ppq505 import PpqValidationStatus, validate_ppq_value
from litoral_trace.us_lacey.reconciliation_invariants import (
    _mark_line_fields_reconciliation_state,
)
from litoral_trace.us_lacey.review import finalize_us_lacey_review, review_us_lacey_field
from litoral_trace.web.us_lacey_operational_views import _present_review_field


@dataclass(frozen=True)
class _DescriptionCandidate:
    id: int
    original_value: str
    normalized_value: str | None = None
    confidence: float = 0.87
    source_page: int | None = 2
    source_document_id: int = 10


@dataclass(frozen=True)
class _DescriptionField:
    field_name: str
    candidates: tuple[_DescriptionCandidate, ...]


def test_human_review_reconciles_before_readiness_and_commit():
    source = inspect.getsource(review_us_lacey_field)

    reconcile_at = source.index("reconcile_entered_value_invariant(")
    refresh_at = source.index("refresh_us_lacey_operation_status(")
    commit_at = source.index("session.commit()")

    assert reconcile_at < refresh_at < commit_at
    assert "Human review is a state-machine mutation" in source


def test_finalization_recomputes_reconciliation_as_hard_gate():
    source = inspect.getsource(finalize_us_lacey_review)

    reconcile_at = source.index("reconcile_entered_value_invariant(")
    count_at = source.index("review_count, missing_count, conflict_count = _counts(")
    completion_at = source.index('operation.status = "COMPLETED"')

    assert reconcile_at < count_at < completion_at
    assert "arithmetic.evaluated and not arithmetic.reconciled" in source
    assert (
        "Cannot complete preparation: Please resolve the Entered Value reconciliation inconsistency first."
        in source
    )
    assert "session.commit()" in source[reconcile_at:count_at]


def test_broken_reconciliation_returns_human_entered_values_to_editable_review_state():
    first = SimpleNamespace(
        human_value="18600",
        normalized_value="14880",
        original_value="$14,880.00",
        reviewed_at=object(),
        field_status="MATCHED",
    )
    second = SimpleNamespace(
        human_value="3720",
        normalized_value="3720",
        original_value="$3,720.00",
        reviewed_at=object(),
        field_status="MATCHED",
    )

    _mark_line_fields_reconciliation_state([first, second], reconciled=False)

    assert first.field_status == "REVIEW"
    assert second.field_status == "REVIEW"

    first.human_value = "14880"
    _mark_line_fields_reconciliation_state([first, second], reconciled=True)

    assert first.field_status == "MATCHED"
    assert second.field_status == "MATCHED"


def test_reconciled_unreviewed_extracted_value_is_not_silently_confirmed():
    field = SimpleNamespace(
        human_value=None,
        normalized_value="3720",
        original_value="$3,720.00",
        reviewed_at=None,
        field_status="REVIEW",
    )

    _mark_line_fields_reconciliation_state([field], reconciled=True)

    assert field.field_status == "REVIEW"


def test_structural_description_values_are_invalid_at_ppq_domain_boundary():
    rejected = (
        "Solid rubberwood plant material",
        "MDF plant material",
        "Metal fasteners / protective pads / adhesive",
        "Corrugated cartons, inserts, pallets and other packing",
    )
    for value in rejected:
        result = validate_ppq_value("merchandise_description", value)
        assert result.status is PpqValidationStatus.INVALID
        assert result.error is not None

    commercial = validate_ppq_value(
        "merchandise_description",
        "Retail set: four solid rubberwood coasters with one MDF holder; natural finish; packed for retail sale",
    )
    assert commercial.status is PpqValidationStatus.VALID


def test_all_filtered_description_candidates_do_not_fall_back_to_original_pool():
    field = _DescriptionField(
        field_name="merchandise_description",
        candidates=(
            _DescriptionCandidate(1, "Solid rubberwood plant material"),
            _DescriptionCandidate(2, "MDF plant material"),
            _DescriptionCandidate(3, "Metal fasteners / protective pads / adhesive"),
            _DescriptionCandidate(4, "Corrugated cartons, inserts, pallets and other packing"),
        ),
    )

    presented = _present_review_field(field)

    assert presented.candidates == ()
