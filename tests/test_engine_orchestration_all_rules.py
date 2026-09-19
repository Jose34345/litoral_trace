from __future__ import annotations

from datetime import date

from litoral_trace.us_lacey.regulatory.engine import (
    RegulatoryContext,
    RegulatorySubject,
    evaluate_regulatory_rules,
)
from litoral_trace.us_lacey.regulatory.rules import (
    DeMinimisRule,
    RuleStatus,
    SpecialCompositeRule,
    SpecialRecycledRule,
)
from litoral_trace.us_lacey.regulatory.rules.hts_applicability import (
    HtsApplicabilityRule,
)


def test_engine_orchestrates_all_initial_rules_fail_closed_without_bom():
    subject = RegulatorySubject(
        subject_ref="LINE-1",
        line_reference="LINE-1",
        hts10="4407990190",
        article_component="sawn wood",
    )
    context = RegulatoryContext(
        subjects=(subject,),
        evaluation_date=date(2026, 9, 18),
    )

    results = evaluate_regulatory_rules(
        context,
        rules=(
            HtsApplicabilityRule(),
            DeMinimisRule(),
            SpecialCompositeRule(),
            SpecialRecycledRule(),
        ),
    )

    by_rule = {
        item.assessment.rule_id: item.assessment
        for item in results
    }

    assert set(by_rule) == {
        "HTS_APPLICABILITY",
        "DE_MINIMIS",
        "SPECIAL_COMPOSITE",
        "SPECIAL_RECYCLED",
    }

    assert by_rule["HTS_APPLICABILITY"].status is RuleStatus.PASS
    assert by_rule["DE_MINIMIS"].status is RuleStatus.INDETERMINATE
    assert by_rule["SPECIAL_COMPOSITE"].status is RuleStatus.INDETERMINATE
    assert by_rule["SPECIAL_RECYCLED"].status is RuleStatus.FAIL

    assert by_rule["DE_MINIMIS"].review_required is True
    assert by_rule["SPECIAL_COMPOSITE"].review_required is True
    assert by_rule["SPECIAL_RECYCLED"].reason_codes == (
        "NO_RECYCLED_MATERIAL_EVIDENCE",
    )
