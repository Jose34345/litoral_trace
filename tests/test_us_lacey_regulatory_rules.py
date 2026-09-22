from __future__ import annotations

from decimal import Decimal
import importlib


def _rules():
    return importlib.import_module("litoral_trace.us_lacey.regulatory.rules")


def _de_minimis_input(rules, **overrides):
    values = {
        "subject_ref": "SKU-1",
        "hts10": "9401692010",
        "plant_mass_per_unit_kg": Decimal("0.500"),
        "total_unit_mass_kg": Decimal("10.000"),
        "entry_same_hts_plant_mass_kg": Decimal("2.900"),
        "protected_status": rules.ProtectedPlantStatus.CLEAR,
    }
    values.update(overrides)
    return rules.DeMinimisInput(**values)


def test_de_minimis_exact_boundaries_pass():
    rules = _rules()
    result = rules.evaluate_de_minimis(_de_minimis_input(rules))

    assert result.rule_id == "DE_MINIMIS"
    assert result.ruleset_version == rules.RULESET_VERSION
    assert result.status is rules.RuleStatus.PASS
    assert result.review_required is False
    assert result.reason_codes == ("QUALIFIES_DE_MINIMIS",)
    assert result.calculation_trace["plant_percent"] == "5.00"
    assert result.calculation_trace["entry_same_hts_plant_mass_kg"] == "2.900"


def test_de_minimis_unit_percentage_above_threshold_fails():
    rules = _rules()
    result = rules.evaluate_de_minimis(
        _de_minimis_input(
            rules,
            plant_mass_per_unit_kg=Decimal("0.501"),
            total_unit_mass_kg=Decimal("10.000"),
        )
    )

    assert result.status is rules.RuleStatus.FAIL
    assert "UNIT_PLANT_PERCENT_EXCEEDED" in result.reason_codes


def test_de_minimis_entry_mass_above_threshold_fails():
    rules = _rules()
    result = rules.evaluate_de_minimis(
        _de_minimis_input(rules, entry_same_hts_plant_mass_kg=Decimal("2.901"))
    )

    assert result.status is rules.RuleStatus.FAIL
    assert "ENTRY_PLANT_MASS_EXCEEDED" in result.reason_codes


def test_de_minimis_protected_plant_present_fails_even_below_thresholds():
    rules = _rules()
    result = rules.evaluate_de_minimis(
        _de_minimis_input(rules, protected_status=rules.ProtectedPlantStatus.PRESENT)
    )

    assert result.status is rules.RuleStatus.FAIL
    assert result.reason_codes == ("PROTECTED_PLANT_PRESENT",)


def test_de_minimis_unknown_protected_status_is_indeterminate():
    rules = _rules()
    result = rules.evaluate_de_minimis(
        _de_minimis_input(rules, protected_status=rules.ProtectedPlantStatus.UNKNOWN)
    )

    assert result.status is rules.RuleStatus.INDETERMINATE
    assert result.review_required is True
    assert "PROTECTED_STATUS_UNKNOWN" in result.reason_codes


def test_de_minimis_missing_inputs_are_indeterminate_not_safe():
    rules = _rules()
    result = rules.evaluate_de_minimis(
        _de_minimis_input(
            rules,
            hts10=None,
            plant_mass_per_unit_kg=None,
            total_unit_mass_kg=None,
            entry_same_hts_plant_mass_kg=None,
            protected_status=rules.ProtectedPlantStatus.UNKNOWN,
        )
    )

    assert result.status is rules.RuleStatus.INDETERMINATE
    assert result.review_required is True
    assert "MISSING_REQUIRED_INPUTS" in result.reason_codes


def test_de_minimis_malformed_hts10_and_zero_total_mass_are_indeterminate():
    rules = _rules()
    result = rules.evaluate_de_minimis(
        _de_minimis_input(rules, hts10="94016920", total_unit_mass_kg=Decimal("0"))
    )

    assert result.status is rules.RuleStatus.INDETERMINATE
    assert result.review_required is True
    assert "INVALID_HTS10" in result.reason_codes
    assert "INVALID_TOTAL_UNIT_MASS" in result.reason_codes


def test_special_composite_pass_requires_all_construction_facts_and_due_care():
    rules = _rules()
    result = rules.evaluate_special_composite(
        rules.SpecialCompositeInput(
            subject_ref="SKU-1:panel",
            small_fibers_more_than_one_plant_kind=rules.TriState.YES,
            mechanically_processed_mixed_chemically_bonded=rules.TriState.YES,
            thin_solid_plies_or_layers=rules.TriState.NO,
            species_determinable_after_due_care=rules.TriState.NO,
        )
    )

    assert result.rule_id == "SPECIAL_COMPOSITE"
    assert result.status is rules.RuleStatus.PASS
    assert result.review_required is False
    assert result.reason_codes == ("SPECIAL_COMPOSITE_CRITERIA_SATISFIED",)


def test_special_composite_known_species_is_not_applicable():
    rules = _rules()
    result = rules.evaluate_special_composite(
        rules.SpecialCompositeInput(
            subject_ref="SKU-1:panel",
            small_fibers_more_than_one_plant_kind=rules.TriState.YES,
            mechanically_processed_mixed_chemically_bonded=rules.TriState.YES,
            thin_solid_plies_or_layers=rules.TriState.NO,
            species_determinable_after_due_care=rules.TriState.YES,
        )
    )

    assert result.status is rules.RuleStatus.NOT_APPLICABLE
    assert result.reason_codes == ("SPECIES_DETERMINABLE_AFTER_DUE_CARE",)


def test_special_composite_thin_solid_plies_are_not_applicable():
    rules = _rules()
    result = rules.evaluate_special_composite(
        rules.SpecialCompositeInput(
            subject_ref="SKU-1:plywood",
            small_fibers_more_than_one_plant_kind=rules.TriState.UNKNOWN,
            mechanically_processed_mixed_chemically_bonded=rules.TriState.UNKNOWN,
            thin_solid_plies_or_layers=rules.TriState.YES,
            species_determinable_after_due_care=rules.TriState.UNKNOWN,
        )
    )

    assert result.status is rules.RuleStatus.NOT_APPLICABLE
    assert result.reason_codes == ("THIN_SOLID_PLIES_NOT_SPECIAL_COMPOSITE",)


def test_special_composite_unknown_fact_is_indeterminate():
    rules = _rules()
    result = rules.evaluate_special_composite(
        rules.SpecialCompositeInput(
            subject_ref="SKU-1:panel",
            small_fibers_more_than_one_plant_kind=rules.TriState.UNKNOWN,
            mechanically_processed_mixed_chemically_bonded=rules.TriState.YES,
            thin_solid_plies_or_layers=rules.TriState.NO,
            species_determinable_after_due_care=rules.TriState.NO,
        )
    )

    assert result.status is rules.RuleStatus.INDETERMINATE
    assert result.review_required is True
    assert "MISSING_COMPOSITE_FACTS" in result.reason_codes


def test_exact_material_classifier_is_conservative_and_has_no_fuzzy_guessing():
    rules = _rules()

    mdf = rules.classify_composite_material_name("MDF")
    assert mdf.mechanically_processed_mixed_chemically_bonded is rules.TriState.YES
    assert mdf.thin_solid_plies_or_layers is rules.TriState.NO
    assert mdf.small_fibers_more_than_one_plant_kind is rules.TriState.UNKNOWN

    plywood = rules.classify_composite_material_name("plywood")
    assert plywood.thin_solid_plies_or_layers is rules.TriState.YES

    typo = rules.classify_composite_material_name("particl bord")
    assert typo.small_fibers_more_than_one_plant_kind is rules.TriState.UNKNOWN
    assert typo.mechanically_processed_mixed_chemically_bonded is rules.TriState.UNKNOWN
    assert typo.thin_solid_plies_or_layers is rules.TriState.UNKNOWN
