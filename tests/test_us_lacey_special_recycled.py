from __future__ import annotations

from litoral_trace.us_lacey.regulatory.engine import RegulatoryContext, RegulatorySubject
from litoral_trace.us_lacey.regulatory.rules import (
    RuleStatus,
    SpecialRecycledInput,
    SpecialRecycledRule,
    TriState,
    evaluate_special_recycled,
)


def _context(subject: RegulatorySubject) -> RegulatoryContext:
    return RegulatoryContext(subjects=(subject,))


def test_special_recycled_no_positive_recycled_evidence_is_not_applicable():
    subject = RegulatorySubject(
        subject_ref="LINE-1",
        line_reference="LINE-1",
        article_component="solid oak panel",
    )

    result = SpecialRecycledRule().evaluate(
        subject=subject,
        context=_context(subject),
    )

    assert result.status is RuleStatus.NOT_APPLICABLE
    assert result.reason_codes == ("NO_RECYCLED_MATERIAL_EVIDENCE",)


def test_special_recycled_known_species_is_not_applicable_even_with_recycled_material():
    subject = RegulatorySubject(
        subject_ref="LINE-1",
        line_reference="LINE-1",
        genus="Quercus",
        species="rubra",
        enrichment={"material_description": "100% recycled paperboard"},
    )

    result = SpecialRecycledRule().evaluate(
        subject=subject,
        context=_context(subject),
    )

    assert result.status is RuleStatus.NOT_APPLICABLE
    assert result.reason_codes == ("SPECIES_DETERMINABLE_AFTER_DUE_CARE",)


def test_special_recycled_recycled_evidence_with_unknown_due_care_is_indeterminate():
    subject = RegulatorySubject(
        subject_ref="LINE-1",
        line_reference="LINE-1",
        enrichment={"material_description": "100% recycled paperboard"},
    )

    result = SpecialRecycledRule().evaluate(
        subject=subject,
        context=_context(subject),
    )

    assert result.status is RuleStatus.INDETERMINATE
    assert result.reason_codes == ("DUE_CARE_INSUFFICIENT_OR_UNKNOWN",)
    assert result.review_required is True


def test_special_recycled_pass_requires_positive_recycled_and_due_care_facts():
    subject = RegulatorySubject(
        subject_ref="LINE-1",
        line_reference="LINE-1",
        rule_inputs={
            "SPECIAL_RECYCLED": SpecialRecycledInput(
                subject_ref="LINE-1",
                highly_processed_recycled_material=TriState.YES,
                species_determinable_after_due_care=TriState.NO,
            )
        },
    )

    result = SpecialRecycledRule().evaluate(
        subject=subject,
        context=_context(subject),
    )

    assert result.status is RuleStatus.PASS
    assert result.reason_codes == ("SPECIAL_RECYCLED_CRITERIA_SATISFIED",)
    assert result.review_required is False


def test_special_recycled_pure_evaluator_treats_unknown_recycled_status_as_not_applicable():
    result = evaluate_special_recycled(
        SpecialRecycledInput(
            subject_ref="LINE-1",
            highly_processed_recycled_material=TriState.UNKNOWN,
            species_determinable_after_due_care=TriState.UNKNOWN,
        )
    )

    assert result.status is RuleStatus.NOT_APPLICABLE
    assert result.reason_codes == ("NO_RECYCLED_MATERIAL_EVIDENCE",)


def test_known_species_overrides_contradictory_due_care_rule_input():
    subject = RegulatorySubject(
        subject_ref="LINE-1",
        line_reference="LINE-1",
        genus="Quercus",
        species="rubra",
        rule_inputs={
            "SPECIAL_RECYCLED": SpecialRecycledInput(
                subject_ref="LINE-1",
                highly_processed_recycled_material=TriState.YES,
                species_determinable_after_due_care=TriState.NO,
            )
        },
    )

    result = SpecialRecycledRule().evaluate(
        subject=subject,
        context=_context(subject),
    )

    assert result.status is RuleStatus.NOT_APPLICABLE
    assert result.reason_codes == ("SPECIES_DETERMINABLE_AFTER_DUE_CARE",)
