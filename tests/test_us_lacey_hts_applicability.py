from __future__ import annotations

from datetime import date

import pytest

from litoral_trace.us_lacey.regulatory.catalogs.hts_schedule import (
    APHIS_HTS_SCHEDULE,
    APHIS_HTS_SCHEDULE_VERSION,
    HtsScheduleCatalog,
    HtsScheduleEntry,
)
from litoral_trace.us_lacey.regulatory.engine import (
    RegulatoryContext,
    RegulatorySubject,
    evaluate_regulatory_rules,
)
from litoral_trace.us_lacey.regulatory.rules.domain import RuleStatus
from litoral_trace.us_lacey.regulatory.rules.hts_applicability import (
    HtsApplicabilityRule,
)


PASS_EXPLANATION = (
    "HTS is included in the APHIS Lacey declaration implementation schedule. "
    "Final applicability also depends on entry type, plant content and applicable exceptions."
)


def _subject(hts10: str | None) -> RegulatorySubject:
    return RegulatorySubject(
        subject_ref="LINE-1",
        line_reference="LINE-1",
        hts10=hts10,
    )


def test_catalog_is_versioned_and_matches_4407_by_prefix():
    assert APHIS_HTS_SCHEDULE.version == APHIS_HTS_SCHEDULE_VERSION
    assert APHIS_HTS_SCHEDULE.version == "aphis-lacey-hts-2026-01-13"

    match = APHIS_HTS_SCHEDULE.match(
        "4407990190",
        effective_date=date(2026, 9, 18),
    )

    assert match is not None
    assert match.hts_prefix == "4407"
    assert match.effective_from == date(2009, 4, 1)


def test_catalog_matches_4415_only_after_its_effective_date():
    assert APHIS_HTS_SCHEDULE.match(
        "4415204000",
        effective_date=date(2021, 9, 30),
    ) is None

    match = APHIS_HTS_SCHEDULE.match(
        "4415204000",
        effective_date=date(2021, 10, 1),
    )

    assert match is not None
    assert match.hts_prefix == "4415"


def test_catalog_uses_longest_active_prefix_match():
    catalog = HtsScheduleCatalog(
        version="test-catalog",
        as_of=date(2026, 1, 1),
        entries=(
            HtsScheduleEntry(
                hts_prefix="9401",
                effective_from=date(2024, 12, 1),
            ),
            HtsScheduleEntry(
                hts_prefix="9401692010",
                effective_from=date(2025, 1, 1),
            ),
        ),
    )

    match = catalog.match("9401692010", effective_date=date(2026, 1, 1))

    assert match is not None
    assert match.hts_prefix == "9401692010"


def test_catalog_respects_effective_dates():
    catalog = HtsScheduleCatalog(
        version="test-catalog",
        as_of=date(2026, 1, 1),
        entries=(
            HtsScheduleEntry(
                hts_prefix="4407",
                effective_from=date(2026, 6, 1),
            ),
        ),
    )

    assert catalog.match("4407990190", effective_date=date(2026, 5, 31)) is None
    assert catalog.match("4407990190", effective_date=date(2026, 6, 1)) is not None


def test_catalog_rejects_invalid_prefix_and_date_range():
    with pytest.raises(ValueError, match="4 to 10 digits"):
        HtsScheduleEntry(
            hts_prefix="44.07",
            effective_from=date(2009, 4, 1),
        )

    with pytest.raises(ValueError, match="cannot precede"):
        HtsScheduleEntry(
            hts_prefix="4407",
            effective_from=date(2026, 1, 2),
            effective_to=date(2026, 1, 1),
        )


def test_hts_rule_missing_code_is_indeterminate_fail_closed():
    rule = HtsApplicabilityRule()
    result = rule.evaluate(
        subject=_subject(None),
        context=RegulatoryContext(
            subjects=(_subject(None),),
            evaluation_date=date(2026, 9, 18),
        ),
    )

    assert result.status is RuleStatus.INDETERMINATE
    assert result.reason_codes == ("HTS10_MISSING",)
    assert result.review_required is True


def test_hts_rule_malformed_code_is_indeterminate_fail_closed():
    subject = _subject("44079901")
    rule = HtsApplicabilityRule()

    result = rule.evaluate(
        subject=subject,
        context=RegulatoryContext(
            subjects=(subject,),
            evaluation_date=date(2026, 9, 18),
        ),
    )

    assert result.status is RuleStatus.INDETERMINATE
    assert result.reason_codes == ("HTS10_INVALID",)
    assert result.review_required is True


def test_hts_rule_partial_catalog_miss_is_indeterminate_fail_closed():
    subject = _subject("0101210010")
    rule = HtsApplicabilityRule()

    result = rule.evaluate(
        subject=subject,
        context=RegulatoryContext(
            subjects=(subject,),
            evaluation_date=date(2026, 9, 18),
        ),
    )

    assert result.status is RuleStatus.INDETERMINATE
    assert result.reason_codes == ("HTS_NOT_IN_PARTIAL_CATALOG",)
    assert result.review_required is True
    assert result.calculation_trace["matched_prefix"] is None


def test_hts_rule_complete_catalog_miss_can_return_fail():
    subject = _subject("0101210010")
    complete_catalog = HtsScheduleCatalog(
        version="complete-test-catalog",
        as_of=date(2026, 9, 18),
        entries=(
            HtsScheduleEntry(
                hts_prefix="4407",
                effective_from=date(2009, 4, 1),
            ),
        ),
        is_complete=True,
    )
    rule = HtsApplicabilityRule(complete_catalog)

    result = rule.evaluate(
        subject=subject,
        context=RegulatoryContext(
            subjects=(subject,),
            evaluation_date=date(2026, 9, 18),
        ),
    )

    assert result.status is RuleStatus.FAIL
    assert result.reason_codes == ("HTS_NOT_ON_APHIS_SCHEDULE",)
    assert result.review_required is False


def test_hts_rule_passes_4407_with_exact_defensive_copy():
    subject = _subject("4407990190")
    rule = HtsApplicabilityRule()

    result = rule.evaluate(
        subject=subject,
        context=RegulatoryContext(
            subjects=(subject,),
            evaluation_date=date(2026, 9, 18),
        ),
    )

    assert result.status is RuleStatus.PASS
    assert result.reason_codes == ("HTS_ON_APHIS_SCHEDULE",)
    assert result.explanation == PASS_EXPLANATION
    assert result.calculation_trace["matched_prefix"] == "4407"
    assert result.calculation_trace["catalog_version"] == "aphis-lacey-hts-2026-01-13"


def test_hts_rule_passes_exact_seeded_furniture_code():
    subject = _subject("9401692010")
    rule = HtsApplicabilityRule()

    result = rule.evaluate(
        subject=subject,
        context=RegulatoryContext(
            subjects=(subject,),
            evaluation_date=date(2026, 9, 18),
        ),
    )

    assert result.status is RuleStatus.PASS
    assert result.explanation == PASS_EXPLANATION
    assert result.calculation_trace["matched_prefix"] == "9401692010"


def test_hts_rule_uses_catalog_as_of_when_context_has_no_evaluation_date():
    subject = _subject("4407990190")
    rule = HtsApplicabilityRule()

    result = rule.evaluate(
        subject=subject,
        context=RegulatoryContext(subjects=(subject,)),
    )

    assert result.status is RuleStatus.PASS
    assert result.calculation_trace["evaluation_date"] == "2026-01-13"


def test_hts_rule_conforms_to_subject_first_engine_protocol():
    subject = _subject("4407990190")
    context = RegulatoryContext(
        subjects=(subject,),
        evaluation_date=date(2026, 9, 18),
    )

    results = evaluate_regulatory_rules(
        context,
        rules=(HtsApplicabilityRule(),),
    )

    assert len(results) == 1
    assert results[0].subject_ref == "LINE-1"
    assert results[0].assessment.rule_id == "HTS_APPLICABILITY"
    assert results[0].assessment.status is RuleStatus.PASS
