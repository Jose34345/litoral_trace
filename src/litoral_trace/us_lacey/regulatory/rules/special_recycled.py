"""Fail-closed APHIS SPECIAL / RECYCLED assessment."""
from __future__ import annotations

import re
from collections.abc import Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from litoral_trace.us_lacey.regulatory.engine import RegulatoryContext, RegulatorySubject

from .domain import (
    RULESET_VERSION,
    RuleAssessment,
    RuleStatus,
    SpecialRecycledInput,
    TriState,
)


_RECYCLED_HIGHLY_PROCESSED = re.compile(
    r"\brecycled\s+(?:paper|paperboard)\b",
    flags=re.IGNORECASE,
)


def _tri_state(value: object) -> TriState:
    if isinstance(value, TriState):
        return value
    if isinstance(value, bool):
        return TriState.YES if value else TriState.NO
    try:
        return TriState(str(value).strip().upper())
    except (ValueError, AttributeError):
        return TriState.UNKNOWN


def _textual_recycled_evidence(subject: "RegulatorySubject") -> bool:
    candidates = (
        subject.enrichment.get("material"),
        subject.enrichment.get("material_description"),
        subject.enrichment.get("description"),
        subject.article_component,
    )
    return any(
        _RECYCLED_HIGHLY_PROCESSED.search(str(value or "")) is not None
        for value in candidates
    )


def _subject_has_known_scientific_name(subject: "RegulatorySubject") -> bool:
    genus = str(subject.genus or "").strip()
    species = str(subject.species or "").strip()
    if not genus or not species:
        return False
    if genus.casefold() == "special":
        return False
    return True


def _assessment(
    inputs: SpecialRecycledInput,
    *,
    status: RuleStatus,
    reasons: tuple[str, ...],
    explanation: str,
) -> RuleAssessment:
    return RuleAssessment(
        rule_id="SPECIAL_RECYCLED",
        ruleset_version=RULESET_VERSION,
        status=status,
        reason_codes=reasons,
        explanation=explanation,
        calculation_trace={
            "subject_ref": inputs.subject_ref,
            "highly_processed_recycled_material": (
                inputs.highly_processed_recycled_material.value
            ),
            "species_determinable_after_due_care": (
                inputs.species_determinable_after_due_care.value
            ),
        },
        evidence_refs=inputs.evidence_refs,
        review_required=status is RuleStatus.INDETERMINATE,
    )


def evaluate_special_recycled(inputs: SpecialRecycledInput) -> RuleAssessment:
    """Evaluate SPECIAL / RECYCLED only from explicit, evidence-backed facts."""

    if inputs.species_determinable_after_due_care is TriState.YES:
        return _assessment(
            inputs,
            status=RuleStatus.NOT_APPLICABLE,
            reasons=("SPECIES_DETERMINABLE_AFTER_DUE_CARE",),
            explanation=(
                "A scientific name is determinable from the supported evidence. "
                "The SPECIAL / RECYCLED alternative is not applicable."
            ),
        )

    if inputs.highly_processed_recycled_material in {TriState.NO, TriState.UNKNOWN}:
        return _assessment(
            inputs,
            status=RuleStatus.NOT_APPLICABLE,
            reasons=("NO_RECYCLED_MATERIAL_EVIDENCE",),
            explanation="The evidence does not identify this subject as highly processed recycled plant material.",
        )

    if inputs.species_determinable_after_due_care is TriState.UNKNOWN:
        return _assessment(
            inputs,
            status=RuleStatus.INDETERMINATE,
            reasons=("DUE_CARE_INSUFFICIENT_OR_UNKNOWN",),
            explanation=(
                "Recycled material is supported, but the due-care basis for being unable "
                "to determine the scientific species is unknown or insufficient."
            ),
        )

    if inputs.species_determinable_after_due_care is TriState.NO:
        return _assessment(
            inputs,
            status=RuleStatus.PASS,
            reasons=("SPECIAL_RECYCLED_CRITERIA_SATISFIED",),
            explanation=(
                "The supplied evidence supports highly processed recycled material and "
                "documents that the scientific species could not be determined after due care."
            ),
        )

    return _assessment(
        inputs,
        status=RuleStatus.INDETERMINATE,
        reasons=("UNSUPPORTED_RECYCLED_STATE",),
        explanation="The SPECIAL / RECYCLED assessment contains an unsupported state.",
    )


class SpecialRecycledRule:
    """Protocol adapter for APHIS SPECIAL / RECYCLED."""

    rule_id = "SPECIAL_RECYCLED"

    def evaluate(
        self,
        *,
        subject: "RegulatorySubject",
        context: "RegulatoryContext",
    ) -> RuleAssessment:
        del context
        configured = subject.rule_inputs.get(self.rule_id)

        if isinstance(configured, SpecialRecycledInput):
            due_care_state = configured.species_determinable_after_due_care
            if _subject_has_known_scientific_name(subject):
                due_care_state = TriState.YES
            inputs = SpecialRecycledInput(
                subject_ref=subject.subject_ref,
                highly_processed_recycled_material=(
                    configured.highly_processed_recycled_material
                ),
                species_determinable_after_due_care=due_care_state,
                evidence_refs=configured.evidence_refs or subject.evidence_refs,
            )
            return evaluate_special_recycled(inputs)

        values: Mapping[str, object]
        if isinstance(configured, Mapping):
            values = configured
        else:
            values = {}

        recycled_state = _tri_state(
            values.get("highly_processed_recycled_material")
        )
        if recycled_state is TriState.UNKNOWN and _textual_recycled_evidence(subject):
            recycled_state = TriState.YES

        due_care_state = _tri_state(
            values.get("species_determinable_after_due_care")
        )
        if _subject_has_known_scientific_name(subject):
            due_care_state = TriState.YES

        return evaluate_special_recycled(
            SpecialRecycledInput(
                subject_ref=subject.subject_ref,
                highly_processed_recycled_material=recycled_state,
                species_determinable_after_due_care=due_care_state,
                evidence_refs=subject.evidence_refs,
            )
        )


__all__ = [
    "SpecialRecycledRule",
    "evaluate_special_recycled",
]
