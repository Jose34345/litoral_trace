from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from litoral_trace.lacey_benchmark.regulatory_contracts import (
    CompletenessInput,
    CompositeInput,
    DecisionStatus,
    DeMinimisInput,
    Measurement,
    RegulatoryEvaluationInput,
    SudInput,
)
from litoral_trace.lacey_benchmark.regulatory_engine import RegulatoryEngine
from litoral_trace.lacey_benchmark.regulatory_ruleset import RegulatoryRuleSet
from litoral_trace.lacey_benchmark.special_use import SpecialUseRegistry


def _engine() -> RegulatoryEngine:
    ruleset = RegulatoryRuleSet.load(Path("benchmarks/lacey/reference/aphis/ruleset.json"))
    sud = SpecialUseRegistry.load(Path("benchmarks/lacey/reference/aphis/sud.json"))
    return RegulatoryEngine(ruleset, sud)


def test_versioned_ruleset_and_metric_unit_conversion() -> None:
    engine = _engine()
    assert engine.ruleset.ruleset_id == "APHIS-LACEY-V1"
    assert len(engine.ruleset.fingerprint) == 64

    converted = engine.convert_measurement(Measurement(value=Decimal("1000"), unit="G"), "KG")
    assert converted.value == Decimal("1")
    assert converted.unit == "KG"

    converted_volume = engine.convert_measurement(Measurement(value=Decimal("1000000"), unit="CM3"), "M3")
    assert converted_volume.value == Decimal("1")


def test_de_minimis_pass_fail_and_indeterminate_are_fail_closed() -> None:
    engine = _engine()

    passes = engine.assess_de_minimis(
        DeMinimisInput(
            total_product_weight_per_unit=Measurement(value=Decimal("10"), unit="KG"),
            plant_material_weight_per_unit=Measurement(value=Decimal("0.5"), unit="KG"),
            entry_plant_material_weight_same_hts=Measurement(value=Decimal("2.9"), unit="KG"),
            protected_species_present=False,
        )
    )
    assert passes.status is DecisionStatus.PASS
    assert passes.trace

    too_much = engine.assess_de_minimis(
        DeMinimisInput(
            total_product_weight_per_unit=Measurement(value=Decimal("10"), unit="KG"),
            plant_material_weight_per_unit=Measurement(value=Decimal("0.51"), unit="KG"),
            entry_plant_material_weight_same_hts=Measurement(value=Decimal("2.9"), unit="KG"),
            protected_species_present=False,
        )
    )
    assert too_much.status is DecisionStatus.FAIL

    protected_unknown = engine.assess_de_minimis(
        DeMinimisInput(
            total_product_weight_per_unit=Measurement(value=Decimal("10"), unit="KG"),
            plant_material_weight_per_unit=Measurement(value=Decimal("0.2"), unit="KG"),
            entry_plant_material_weight_same_hts=Measurement(value=Decimal("1"), unit="KG"),
            protected_species_present=None,
        )
    )
    assert protected_unknown.status is DecisionStatus.INDETERMINATE

    wrong_dimension = engine.assess_de_minimis(
        DeMinimisInput(
            total_product_weight_per_unit=Measurement(value=Decimal("1"), unit="M3"),
            plant_material_weight_per_unit=Measurement(value=Decimal("0.01"), unit="M3"),
            entry_plant_material_weight_same_hts=Measurement(value=Decimal("1"), unit="KG"),
            protected_species_present=False,
        )
    )
    assert wrong_dimension.status is DecisionStatus.INDETERMINATE


def test_composite_classifier_distinguishes_explicit_known_unknown_materials() -> None:
    engine = _engine()

    assert engine.classify_composite(CompositeInput(material_type="MDF")).status is DecisionStatus.PASS
    assert engine.classify_composite(CompositeInput(material_type="thin solid wood veneer")).status is DecisionStatus.FAIL
    assert engine.classify_composite(CompositeInput(material_type="wood product")).status is DecisionStatus.INDETERMINATE

    evidence_pass = engine.classify_composite(
        CompositeInput(
            material_type=None,
            small_fibers=True,
            multiple_plant_kinds=True,
            chemically_bonded=True,
        )
    )
    assert evidence_pass.status is DecisionStatus.PASS


def test_sud_evaluator_requires_applicability_and_due_care() -> None:
    engine = _engine()

    composite_ok = engine.evaluate_sud(
        SudInput(
            genus="SPECIAL",
            species="COMPOSITE",
            due_care_cannot_determine_species=True,
            composite=CompositeInput(material_type="MDF"),
        )
    )
    assert composite_ok.status is DecisionStatus.PASS

    composite_known = engine.evaluate_sud(
        SudInput(
            genus="SPECIAL",
            species="COMPOSITE",
            due_care_cannot_determine_species=False,
            composite=CompositeInput(material_type="MDF"),
        )
    )
    assert composite_known.status is DecisionStatus.FAIL

    aath_ok = engine.evaluate_sud(
        SudInput(
            genus="TEMP",
            species="AATH",
            possible_species=("Abies amabilis", "Tsuga heterophylla"),
        )
    )
    assert aath_ok.status is DecisionStatus.PASS

    aath_wrong = engine.evaluate_sud(
        SudInput(
            genus="TEMP",
            species="AATH",
            possible_species=("Pinus taeda",),
        )
    )
    assert aath_wrong.status is DecisionStatus.FAIL

    aath_unknown = engine.evaluate_sud(SudInput(genus="TEMP", species="AATH"))
    assert aath_unknown.status is DecisionStatus.INDETERMINATE


def test_country_quantity_completeness_and_reproducible_trace() -> None:
    engine = _engine()

    complete = CompletenessInput(
        countries_of_harvest=("BR",),
        quantity=Measurement(value=Decimal("12.5"), unit="M3"),
    )
    complete_result = engine.assess_completeness(complete)
    assert complete_result.status is DecisionStatus.PASS

    missing_country = engine.assess_completeness(
        CompletenessInput(
            countries_of_harvest=(),
            quantity=Measurement(value=Decimal("12.5"), unit="M3"),
        )
    )
    assert missing_country.status is DecisionStatus.FAIL

    invalid_unit = engine.assess_completeness(
        CompletenessInput(
            countries_of_harvest=("BR",),
            quantity=Measurement(value=Decimal("100"), unit="BOARD_FOOT"),
        )
    )
    assert invalid_unit.status is DecisionStatus.FAIL

    payload = RegulatoryEvaluationInput(
        completeness=complete,
        de_minimis=DeMinimisInput(
            total_product_weight_per_unit=Measurement(value=Decimal("10"), unit="KG"),
            plant_material_weight_per_unit=Measurement(value=Decimal("0.2"), unit="KG"),
            entry_plant_material_weight_same_hts=Measurement(value=Decimal("1"), unit="KG"),
            protected_species_present=False,
        ),
    )
    first = engine.evaluate(payload)
    second = engine.evaluate(payload)

    assert first == second
    assert first.status is DecisionStatus.PASS
    assert first.ruleset_fingerprint == second.ruleset_fingerprint
    assert first.input_fingerprint == second.input_fingerprint
    assert len(first.trace) >= 4
    assert all(item.rule_id and item.source_ref for item in first.trace)
