from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pytest

from litoral_trace.us_lacey.regulatory.engine import (
    RegulatoryContext,
    RegulatorySubject,
    evaluate_regulatory_rules,
)
from litoral_trace.us_lacey.regulatory.rules.domain import (
    RULESET_VERSION,
    RuleAssessment,
    RuleStatus,
)


@dataclass(frozen=True)
class _StaticRule:
    rule_id: str
    status: RuleStatus

    def evaluate(self, *, subject, context):
        return RuleAssessment(
            rule_id=self.rule_id,
            ruleset_version=RULESET_VERSION,
            status=self.status,
            reason_codes=(f"{self.rule_id}_TEST",),
            explanation=f"{self.rule_id} evaluated {subject.subject_ref}.",
            calculation_trace={"evaluation_date": str(context.evaluation_date)},
        )


@dataclass(frozen=True)
class _NotApplicableRule:
    rule_id: str = "OPTIONAL_RULE"

    def evaluate(self, *, subject, context):
        return None


@dataclass(frozen=True)
class _ExplodingRule:
    rule_id: str = "BROKEN_RULE"

    def evaluate(self, *, subject, context):
        raise RuntimeError("synthetic rule failure")


@dataclass(frozen=True)
class _MismatchedRule:
    rule_id: str = "EXPECTED_RULE"

    def evaluate(self, *, subject, context):
        return RuleAssessment(
            rule_id="WRONG_RULE",
            ruleset_version=RULESET_VERSION,
            status=RuleStatus.INDETERMINATE,
            reason_codes=("TEST",),
            explanation="Synthetic mismatch.",
        )


def _subject(reference: str, *, hts10: str | None = None) -> RegulatorySubject:
    return RegulatorySubject(
        subject_ref=reference,
        line_reference=reference,
        hts10=hts10,
    )


def test_engine_evaluates_botanical_subject_without_bom_enrichment():
    context = RegulatoryContext(
        subjects=(_subject("LINE-1", hts10="4407990190"),),
        evaluation_date=date(2026, 9, 18),
    )

    results = evaluate_regulatory_rules(
        context,
        rules=(
            _StaticRule("HTS_APPLICABILITY", RuleStatus.PASS),
            _StaticRule("DE_MINIMIS", RuleStatus.INDETERMINATE),
        ),
    )

    assert [(item.subject_ref, item.assessment.rule_id, item.assessment.status) for item in results] == [
        ("LINE-1", "HTS_APPLICABILITY", RuleStatus.PASS),
        ("LINE-1", "DE_MINIMIS", RuleStatus.INDETERMINATE),
    ]
    assert context.subjects[0].enrichment == {}


def test_engine_order_is_subject_major_then_rule_order():
    context = RegulatoryContext(
        subjects=(_subject("LINE-1"), _subject("LINE-2")),
    )

    results = evaluate_regulatory_rules(
        context,
        rules=(
            _StaticRule("RULE_A", RuleStatus.PASS),
            _StaticRule("RULE_B", RuleStatus.FAIL),
        ),
    )

    assert [(item.subject_ref, item.assessment.rule_id) for item in results] == [
        ("LINE-1", "RULE_A"),
        ("LINE-1", "RULE_B"),
        ("LINE-2", "RULE_A"),
        ("LINE-2", "RULE_B"),
    ]


def test_explicit_not_applicable_rule_can_return_none_without_removing_subject():
    context = RegulatoryContext(subjects=(_subject("LINE-1"),))

    results = evaluate_regulatory_rules(
        context,
        rules=(
            _NotApplicableRule(),
            _StaticRule("HTS_APPLICABILITY", RuleStatus.INDETERMINATE),
        ),
    )

    assert len(results) == 1
    assert results[0].subject_ref == "LINE-1"
    assert results[0].assessment.rule_id == "HTS_APPLICABILITY"


def test_rule_exception_propagates_instead_of_silently_emitting_partial_results():
    context = RegulatoryContext(subjects=(_subject("LINE-1"),))

    with pytest.raises(RuntimeError, match="synthetic rule failure"):
        evaluate_regulatory_rules(
            context,
            rules=(
                _StaticRule("HTS_APPLICABILITY", RuleStatus.PASS),
                _ExplodingRule(),
            ),
        )


def test_engine_rejects_rule_id_mismatch():
    context = RegulatoryContext(subjects=(_subject("LINE-1"),))

    with pytest.raises(ValueError, match="mismatched rule_id"):
        evaluate_regulatory_rules(context, rules=(_MismatchedRule(),))


def test_context_rejects_duplicate_primary_subject_refs():
    with pytest.raises(ValueError, match="unique subject_ref"):
        RegulatoryContext(
            subjects=(
                _subject("LINE-1"),
                _subject("LINE-1"),
            )
        )


def test_engine_rejects_duplicate_rule_ids():
    context = RegulatoryContext(subjects=(_subject("LINE-1"),))

    with pytest.raises(ValueError, match="rule_id values must be unique"):
        evaluate_regulatory_rules(
            context,
            rules=(
                _StaticRule("HTS_APPLICABILITY", RuleStatus.PASS),
                _StaticRule("HTS_APPLICABILITY", RuleStatus.INDETERMINATE),
            ),
        )
