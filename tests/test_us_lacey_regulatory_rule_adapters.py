from __future__ import annotations

from decimal import Decimal

from litoral_trace.us_lacey.regulatory.engine import RegulatoryContext, RegulatorySubject
from litoral_trace.us_lacey.regulatory.rules import (
    DeMinimisInput,
    DeMinimisRule,
    ProtectedPlantStatus,
    RuleStatus,
    SpecialCompositeInput,
    SpecialCompositeRule,
    TriState,
)


def _context(subject: RegulatorySubject) -> RegulatoryContext:
    return RegulatoryContext(subjects=(subject,))


def test_de_minimis_adapter_delegates_typed_input_to_existing_evaluator():
    subject = RegulatorySubject(
        subject_ref="LINE-1",
        line_reference="LINE-1",
        hts10="4407990190",
        rule_inputs={
            "DE_MINIMIS": DeMinimisInput(
                subject_ref="legacy-subject",
                hts10="4407990190",
                plant_mass_per_unit_kg=Decimal("0.100"),
                total_unit_mass_kg=Decimal("10.000"),
                entry_same_hts_plant_mass_kg=Decimal("1.000"),
                protected_status=ProtectedPlantStatus.CLEAR,
            )
        },
    )

    result = DeMinimisRule().evaluate(subject=subject, context=_context(subject))

    assert result.status is RuleStatus.PASS
    assert result.calculation_trace["subject_ref"] == "LINE-1"


def test_de_minimis_adapter_without_weight_inputs_is_indeterminate():
    subject = RegulatorySubject(
        subject_ref="LINE-1",
        line_reference="LINE-1",
        hts10="4407990190",
    )

    result = DeMinimisRule().evaluate(subject=subject, context=_context(subject))

    assert result.status is RuleStatus.INDETERMINATE
    assert "MISSING_REQUIRED_INPUTS" in result.reason_codes


def test_special_composite_adapter_uses_material_enrichment_but_not_due_care_guessing():
    subject = RegulatorySubject(
        subject_ref="LINE-1",
        line_reference="LINE-1",
        enrichment={"material": "MDF"},
    )

    result = SpecialCompositeRule().evaluate(
        subject=subject,
        context=_context(subject),
    )

    assert result.status is RuleStatus.INDETERMINATE
    assert result.reason_codes == ("MISSING_COMPOSITE_FACTS",)


def test_special_composite_known_species_overrides_contradictory_due_care_input():
    subject = RegulatorySubject(
        subject_ref="LINE-1",
        line_reference="LINE-1",
        genus="Quercus",
        species="rubra",
        rule_inputs={
            "SPECIAL_COMPOSITE": SpecialCompositeInput(
                subject_ref="LINE-1",
                small_fibers_more_than_one_plant_kind=TriState.YES,
                mechanically_processed_mixed_chemically_bonded=TriState.YES,
                thin_solid_plies_or_layers=TriState.NO,
                species_determinable_after_due_care=TriState.NO,
            )
        },
    )

    result = SpecialCompositeRule().evaluate(
        subject=subject,
        context=_context(subject),
    )

    assert result.status is RuleStatus.FAIL
    assert result.reason_codes == ("SPECIES_DETERMINABLE_AFTER_DUE_CARE",)
