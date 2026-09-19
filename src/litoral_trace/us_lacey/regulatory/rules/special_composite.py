"""Fail-closed SPECIAL / COMPOSITE assessment helpers."""
from __future__ import annotations

import re
from typing import TYPE_CHECKING, Mapping

if TYPE_CHECKING:
    from litoral_trace.us_lacey.regulatory.engine import RegulatoryContext, RegulatorySubject

from .domain import (
    RULESET_VERSION,
    CompositeMaterialFacts,
    RuleAssessment,
    RuleStatus,
    SpecialCompositeInput,
    TriState,
)


def _normalize_material(value: object) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip().casefold()
    return text


_COMPOSITE_EXACT_ALIASES = frozenset(
    {
        "mdf",
        "medium density fiberboard",
        "medium-density fiberboard",
        "hdf",
        "high density fiberboard",
        "high-density fiberboard",
        "osb",
        "oriented strand board",
        "oriented-strand board",
        "particle board",
        "particleboard",
        "paper",
        "paperboard",
        "cardboard",
    }
)

_THIN_SOLID_PLY_ALIASES = frozenset(
    {
        "plywood",
        "wood veneer",
        "veneer",
    }
)


def classify_composite_material_name(value: object) -> CompositeMaterialFacts:
    """Return only exact, conservative construction facts; never infer due care."""
    normalized = _normalize_material(value)
    if normalized in _COMPOSITE_EXACT_ALIASES:
        return CompositeMaterialFacts(
            material_normalized=normalized,
            # A generic material class does not prove multiple plant kinds.
            small_fibers_more_than_one_plant_kind=TriState.UNKNOWN,
            mechanically_processed_mixed_chemically_bonded=TriState.YES,
            thin_solid_plies_or_layers=TriState.NO,
        )
    if normalized in _THIN_SOLID_PLY_ALIASES:
        return CompositeMaterialFacts(
            material_normalized=normalized,
            small_fibers_more_than_one_plant_kind=TriState.UNKNOWN,
            mechanically_processed_mixed_chemically_bonded=TriState.UNKNOWN,
            thin_solid_plies_or_layers=TriState.YES,
        )
    return CompositeMaterialFacts(
        material_normalized=normalized,
        small_fibers_more_than_one_plant_kind=TriState.UNKNOWN,
        mechanically_processed_mixed_chemically_bonded=TriState.UNKNOWN,
        thin_solid_plies_or_layers=TriState.UNKNOWN,
    )


def _assessment(
    inputs: SpecialCompositeInput,
    *,
    status: RuleStatus,
    reasons: tuple[str, ...],
    explanation: str,
) -> RuleAssessment:
    return RuleAssessment(
        rule_id="SPECIAL_COMPOSITE",
        ruleset_version=RULESET_VERSION,
        status=status,
        reason_codes=reasons,
        explanation=explanation,
        calculation_trace={
            "subject_ref": inputs.subject_ref,
            "small_fibers_more_than_one_plant_kind": inputs.small_fibers_more_than_one_plant_kind.value,
            "mechanically_processed_mixed_chemically_bonded": inputs.mechanically_processed_mixed_chemically_bonded.value,
            "thin_solid_plies_or_layers": inputs.thin_solid_plies_or_layers.value,
            "species_determinable_after_due_care": inputs.species_determinable_after_due_care.value,
        },
        evidence_refs=inputs.evidence_refs,
        review_required=status is RuleStatus.INDETERMINATE,
    )


def evaluate_special_composite(inputs: SpecialCompositeInput) -> RuleAssessment:
    """Evaluate SPECIAL/COMPOSITE only from explicit, evidence-backed facts."""
    if inputs.thin_solid_plies_or_layers is TriState.YES:
        return _assessment(
            inputs,
            status=RuleStatus.FAIL,
            reasons=("THIN_SOLID_PLIES_DISQUALIFY",),
            explanation="Thin plies or layers of solid wood do not support SPECIAL / COMPOSITE.",
        )
    if inputs.species_determinable_after_due_care is TriState.YES:
        return _assessment(
            inputs,
            status=RuleStatus.FAIL,
            reasons=("SPECIES_DETERMINABLE_AFTER_DUE_CARE",),
            explanation="The scientific name is determinable after due care, so SPECIAL / COMPOSITE is not supported.",
        )
    if (
        inputs.small_fibers_more_than_one_plant_kind is TriState.NO
        or inputs.mechanically_processed_mixed_chemically_bonded is TriState.NO
    ):
        return _assessment(
            inputs,
            status=RuleStatus.FAIL,
            reasons=("COMPOSITE_CONSTRUCTION_NOT_SATISFIED",),
            explanation="The supplied construction facts do not satisfy SPECIAL / COMPOSITE criteria.",
        )

    required = (
        inputs.small_fibers_more_than_one_plant_kind,
        inputs.mechanically_processed_mixed_chemically_bonded,
        inputs.thin_solid_plies_or_layers,
        inputs.species_determinable_after_due_care,
    )
    if any(value is TriState.UNKNOWN for value in required):
        return _assessment(
            inputs,
            status=RuleStatus.INDETERMINATE,
            reasons=("MISSING_COMPOSITE_FACTS",),
            explanation="One or more SPECIAL / COMPOSITE facts are unknown; review is required.",
        )

    if (
        inputs.small_fibers_more_than_one_plant_kind is TriState.YES
        and inputs.mechanically_processed_mixed_chemically_bonded is TriState.YES
        and inputs.thin_solid_plies_or_layers is TriState.NO
        and inputs.species_determinable_after_due_care is TriState.NO
    ):
        return _assessment(
            inputs,
            status=RuleStatus.PASS,
            reasons=("SPECIAL_COMPOSITE_CRITERIA_SATISFIED",),
            explanation="The supplied evidence satisfies the rule-scoped SPECIAL / COMPOSITE criteria.",
        )

    # Defensive fail-closed fallback if future enum states are added.
    return _assessment(
        inputs,
        status=RuleStatus.INDETERMINATE,
        reasons=("UNSUPPORTED_COMPOSITE_STATE",),
        explanation="The composite assessment contains an unsupported state; review is required.",
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


class SpecialCompositeRule:
    """Protocol adapter around the existing pure SPECIAL / COMPOSITE evaluator."""

    rule_id = "SPECIAL_COMPOSITE"

    def evaluate(
        self,
        *,
        subject: "RegulatorySubject",
        context: "RegulatoryContext",
    ) -> RuleAssessment:
        del context
        configured = subject.rule_inputs.get(self.rule_id)

        if isinstance(configured, SpecialCompositeInput):
            inputs = SpecialCompositeInput(
                subject_ref=subject.subject_ref,
                small_fibers_more_than_one_plant_kind=(
                    configured.small_fibers_more_than_one_plant_kind
                ),
                mechanically_processed_mixed_chemically_bonded=(
                    configured.mechanically_processed_mixed_chemically_bonded
                ),
                thin_solid_plies_or_layers=configured.thin_solid_plies_or_layers,
                species_determinable_after_due_care=(
                    configured.species_determinable_after_due_care
                ),
                evidence_refs=configured.evidence_refs or subject.evidence_refs,
            )
            return evaluate_special_composite(inputs)

        values: Mapping[str, object]
        if isinstance(configured, Mapping):
            values = configured
        else:
            values = {}

        material_value = (
            values.get("material")
            or subject.enrichment.get("material")
            or subject.enrichment.get("material_description")
            or subject.article_component
        )
        classified = classify_composite_material_name(material_value)

        inputs = SpecialCompositeInput(
            subject_ref=subject.subject_ref,
            small_fibers_more_than_one_plant_kind=_tri_state(
                values.get(
                    "small_fibers_more_than_one_plant_kind",
                    classified.small_fibers_more_than_one_plant_kind,
                )
            ),
            mechanically_processed_mixed_chemically_bonded=_tri_state(
                values.get(
                    "mechanically_processed_mixed_chemically_bonded",
                    classified.mechanically_processed_mixed_chemically_bonded,
                )
            ),
            thin_solid_plies_or_layers=_tri_state(
                values.get(
                    "thin_solid_plies_or_layers",
                    classified.thin_solid_plies_or_layers,
                )
            ),
            species_determinable_after_due_care=_tri_state(
                values.get("species_determinable_after_due_care")
            ),
            evidence_refs=subject.evidence_refs,
        )
        return evaluate_special_composite(inputs)


__all__ = [
    "SpecialCompositeRule",
    "classify_composite_material_name",
    "evaluate_special_composite",
]
