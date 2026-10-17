"""Rule-scoped APHIS Lacey HTS implementation-schedule assessment."""
from __future__ import annotations

import re

from litoral_trace.us_lacey.regulatory.catalogs.hts_schedule import (
    APHIS_HTS_SCHEDULE,
    HtsScheduleCatalog,
)
from litoral_trace.us_lacey.regulatory.engine import RegulatoryContext, RegulatorySubject
from litoral_trace.us_lacey.regulatory.rules.domain import (
    RULESET_VERSION,
    RuleAssessment,
    RuleStatus,
)


_HTS10 = re.compile(r"^\d{10}$")
_PASS_EXPLANATION = (
    "HTS is included in the APHIS Lacey declaration implementation schedule. "
    "Final applicability also depends on entry type, plant content and applicable exceptions."
)


class HtsApplicabilityRule:
    """Evaluate only whether a botanical line's HTS is on the seeded APHIS schedule."""

    rule_id = "HTS_APPLICABILITY"

    def __init__(self, catalog: HtsScheduleCatalog = APHIS_HTS_SCHEDULE) -> None:
        self._catalog = catalog

    def evaluate(
        self,
        *,
        subject: RegulatorySubject,
        context: RegulatoryContext,
    ) -> RuleAssessment:
        raw_hts = None if subject.hts10 is None else str(subject.hts10).strip()

        if not raw_hts:
            return RuleAssessment(
                rule_id=self.rule_id,
                ruleset_version=RULESET_VERSION,
                status=RuleStatus.INDETERMINATE,
                reason_codes=("HTS10_MISSING",),
                explanation=(
                    "A valid 10-digit HTS code is required to evaluate APHIS "
                    "implementation-schedule coverage."
                ),
                calculation_trace={
                    "subject_ref": subject.subject_ref,
                    "hts10": None,
                    "catalog_version": self._catalog.version,
                },
                evidence_refs=subject.evidence_refs,
                review_required=True,
            )

        if not _HTS10.fullmatch(raw_hts):
            return RuleAssessment(
                rule_id=self.rule_id,
                ruleset_version=RULESET_VERSION,
                status=RuleStatus.INDETERMINATE,
                reason_codes=("HTS10_INVALID",),
                explanation=(
                    "The HTS code is incomplete or invalid; a 10-digit HTS code "
                    "is required for schedule evaluation."
                ),
                calculation_trace={
                    "subject_ref": subject.subject_ref,
                    "hts10": raw_hts,
                    "catalog_version": self._catalog.version,
                },
                evidence_refs=subject.evidence_refs,
                review_required=True,
            )

        effective_date = context.evaluation_date or self._catalog.as_of
        match = self._catalog.match(raw_hts, effective_date=effective_date)
        trace = {
            "subject_ref": subject.subject_ref,
            "hts10": raw_hts,
            "evaluation_date": effective_date.isoformat(),
            "catalog_version": self._catalog.version,
            "matched_prefix": None if match is None else match.hts_prefix,
        }

        if match is None:
            if not self._catalog.is_complete:
                return RuleAssessment(
                    rule_id=self.rule_id,
                    ruleset_version=RULESET_VERSION,
                    status=RuleStatus.INDETERMINATE,
                    reason_codes=("HTS_NOT_IN_PARTIAL_CATALOG",),
                    explanation=(
                        f"HTS {raw_hts} is not present in the configured partial APHIS "
                        "implementation-schedule catalog; schedule coverage cannot be "
                        "determined from this catalog alone."
                    ),
                    calculation_trace=trace,
                    evidence_refs=subject.evidence_refs,
                    review_required=True,
                )
            return RuleAssessment(
                rule_id=self.rule_id,
                ruleset_version=RULESET_VERSION,
                status=RuleStatus.FAIL,
                reason_codes=("HTS_NOT_ON_APHIS_SCHEDULE",),
                explanation=(
                    f"HTS {raw_hts} is not included in the configured APHIS Lacey "
                    "implementation-schedule catalog for the evaluated date."
                ),
                calculation_trace=trace,
                evidence_refs=subject.evidence_refs,
                review_required=False,
            )

        return RuleAssessment(
            rule_id=self.rule_id,
            ruleset_version=RULESET_VERSION,
            status=RuleStatus.PASS,
            reason_codes=("HTS_ON_APHIS_SCHEDULE",),
            explanation=_PASS_EXPLANATION,
            calculation_trace=trace,
            evidence_refs=subject.evidence_refs,
            review_required=False,
        )


__all__ = ["HtsApplicabilityRule"]
