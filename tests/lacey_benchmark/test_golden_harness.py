from __future__ import annotations

from decimal import Decimal
import json

from litoral_trace.lacey_benchmark.contracts import (
    CanonicalField,
    EvalClassification,
    FieldStatus,
    PlantLine,
    ShipmentTruth,
)
from litoral_trace.lacey_benchmark.evaluator import SemanticEvaluator
from litoral_trace.lacey_benchmark.run_benchmark import run_benchmark


def text_field(
    value: str | None,
    status: FieldStatus = FieldStatus.AUTO_SUPPORTED,
    confidence: float = 1.0,
) -> CanonicalField[str]:
    return CanonicalField[str](value=value, status=status, confidence=confidence)


def quantity_field(
    value: Decimal | None,
    status: FieldStatus = FieldStatus.AUTO_SUPPORTED,
    confidence: float = 1.0,
) -> CanonicalField[Decimal]:
    return CanonicalField[Decimal](value=value, status=status, confidence=confidence)


def line(
    line_id: str,
    *,
    hts: str | None,
    genus: str | None,
    species: str | None,
    quantity: Decimal | None,
    hts_status: FieldStatus = FieldStatus.AUTO_SUPPORTED,
    genus_status: FieldStatus = FieldStatus.AUTO_SUPPORTED,
    species_status: FieldStatus = FieldStatus.AUTO_SUPPORTED,
    quantity_status: FieldStatus = FieldStatus.AUTO_SUPPORTED,
) -> PlantLine:
    return PlantLine(
        line_id=line_id,
        hts=text_field(hts, hts_status),
        genus=text_field(genus, genus_status),
        species=text_field(species, species_status),
        quantity=quantity_field(quantity, quantity_status),
    )


def classifications(scorecard):
    return {
        (result.line_id, result.field_name): result.classification
        for result in scorecard.results
    }


def test_semantic_matrix_uses_line_identity_and_detects_cross_line_contamination() -> None:
    expected = ShipmentTruth(
        plant_lines=[
            line(
                "LINE-A",
                hts="4407110190",
                genus="Eucalyptus",
                species="grandis",
                quantity=Decimal("10"),
            ),
            line(
                "LINE-B",
                hts="4407990190",
                genus="Pinus",
                species="taeda",
                quantity=Decimal("20"),
            ),
        ]
    )
    actual = ShipmentTruth(
        plant_lines=[
            # Deliberately reversed list order. Matching must be line_id only.
            line(
                "LINE-B",
                hts="9999999999",
                genus="Pinus",
                species="grandis",
                quantity=Decimal("99"),
                quantity_status=FieldStatus.REVIEW_REQUIRED,
            ),
            line(
                "LINE-A",
                hts="4407110190",
                genus="Eucalyptus",
                species="taeda",
                quantity=None,
                genus_status=FieldStatus.REVIEW_REQUIRED,
                quantity_status=FieldStatus.MISSING,
            ),
        ]
    )

    scorecard = SemanticEvaluator.evaluate(expected, actual, case_id="matrix")
    by_field = classifications(scorecard)

    assert by_field[("LINE-A", "hts")] is EvalClassification.TRUE_SAFE
    assert by_field[("LINE-A", "genus")] is EvalClassification.UNNECESSARY_REVIEW
    assert by_field[("LINE-A", "species")] is EvalClassification.WRONG_ENTITY_ASSOCIATION
    assert by_field[("LINE-A", "quantity")] is EvalClassification.FALSE_MISSING
    assert by_field[("LINE-B", "hts")] is EvalClassification.FALSE_SAFE
    assert by_field[("LINE-B", "species")] is EvalClassification.WRONG_ENTITY_ASSOCIATION
    assert by_field[("LINE-B", "quantity")] is EvalClassification.OTHER_REVIEW


def test_expected_null_values_are_not_evaluated() -> None:
    expected = ShipmentTruth(
        plant_lines=[
            line(
                "LINE-A",
                hts="4407110190",
                genus=None,
                species=None,
                quantity=None,
            )
        ]
    )
    actual = ShipmentTruth(
        plant_lines=[
            line(
                "LINE-A",
                hts="4407110190",
                genus="Unexpected",
                species="unexpected",
                quantity=Decimal("123"),
            )
        ]
    )

    scorecard = SemanticEvaluator.evaluate(expected, actual)

    assert scorecard.total_fields == 1
    assert scorecard.count(EvalClassification.TRUE_SAFE) == 1


def test_missing_expected_line_marks_all_relevant_fields_false_missing() -> None:
    expected = ShipmentTruth(
        plant_lines=[
            line(
                "LINE-A",
                hts="4407110190",
                genus="Eucalyptus",
                species="grandis",
                quantity=Decimal("10"),
            ),
            line(
                "LINE-B",
                hts="4407990190",
                genus="Pinus",
                species="taeda",
                quantity=Decimal("20"),
            ),
        ]
    )
    actual = ShipmentTruth(
        plant_lines=[
            # Values from LINE-B are present globally, but LINE-B itself is absent.
            # The missing-line rule must win over wrong-entity detection.
            line(
                "LINE-A",
                hts="4407990190",
                genus="Pinus",
                species="taeda",
                quantity=Decimal("20"),
            )
        ]
    )

    scorecard = SemanticEvaluator.evaluate(expected, actual)
    missing_line_results = [result for result in scorecard.results if result.line_id == "LINE-B"]

    assert len(missing_line_results) == 4
    assert {result.classification for result in missing_line_results} == {
        EvalClassification.FALSE_MISSING
    }


def test_scorecard_exposes_required_rates_and_anomalies() -> None:
    expected = ShipmentTruth(
        plant_lines=[
            line(
                "LINE-A",
                hts="4407110190",
                genus="Eucalyptus",
                species=None,
                quantity=None,
            )
        ]
    )
    actual = ShipmentTruth(
        plant_lines=[
            line(
                "LINE-A",
                hts="4407110190",
                genus="Wrong genus",
                species=None,
                quantity=None,
                genus_status=FieldStatus.AUTO_SUPPORTED,
            )
        ]
    )

    scorecard = SemanticEvaluator.evaluate(expected, actual)

    assert scorecard.total_fields == 2
    assert scorecard.automation_rate == 0.5
    assert scorecard.false_safe_rate == 0.5
    assert scorecard.unnecessary_review_rate == 0.0
    assert scorecard.false_missing_rate == 0.0
    assert scorecard.wrong_entity_association_rate == 0.0
    assert len(scorecard.anomalies) == 1
    assert "FALSE_SAFE" in scorecard.render_console()


def test_directory_runner_aggregates_matching_relative_json_files(tmp_path) -> None:
    expected_dir = tmp_path / "expected"
    actual_dir = tmp_path / "actual"
    nested_expected = expected_dir / "supplier-a"
    nested_actual = actual_dir / "supplier-a"
    nested_expected.mkdir(parents=True)
    nested_actual.mkdir(parents=True)

    expected = ShipmentTruth(
        plant_lines=[
            line(
                "LINE-A",
                hts="4407110190",
                genus="Eucalyptus",
                species=None,
                quantity=None,
            )
        ]
    )
    actual = ShipmentTruth(
        plant_lines=[
            line(
                "LINE-A",
                hts="4407110190",
                genus="Eucalyptus",
                species=None,
                quantity=None,
                genus_status=FieldStatus.REVIEW_REQUIRED,
            )
        ]
    )
    (nested_expected / "pack.json").write_text(
        json.dumps(expected.model_dump(mode="json")), encoding="utf-8"
    )
    (nested_actual / "pack.json").write_text(
        json.dumps(actual.model_dump(mode="json")), encoding="utf-8"
    )

    scorecard = run_benchmark(expected_dir, actual_dir)

    assert scorecard.total_fields == 2
    assert scorecard.count(EvalClassification.TRUE_SAFE) == 1
    assert scorecard.count(EvalClassification.UNNECESSARY_REVIEW) == 1
    assert {result.case_id for result in scorecard.results} == {"supplier-a/pack.json"}
