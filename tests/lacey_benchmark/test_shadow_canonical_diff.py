from __future__ import annotations

from io import StringIO
from pathlib import Path

from litoral_trace.lacey_benchmark.shadow_canonical_diff import (
    BenchmarkFixture,
    ComparisonOutcome,
    evaluate_shadow_canonical_diff,
    load_benchmark_fixture,
    print_report,
)


ROOT = Path(__file__).resolve().parents[2]
ENGLISH = ROOT / "benchmarks/lacey/v2/pack_english_shadow_canonical.json"
MULTILINGUAL = ROOT / "benchmarks/lacey/v2/pack_multilingual_es_pt_en_shadow_canonical.json"


def test_english_pack_diff_matches_expected_regression_scorecard() -> None:
    fixture = load_benchmark_fixture(ENGLISH)
    report = evaluate_shadow_canonical_diff(fixture.shadow, fixture.canonical)

    assert fixture.pack_id == "pack-english"
    assert report.comparable_slots == 11
    assert report.agreement_count == 8
    assert report.shadow_supported_but_canonical_missing == 2
    assert report.canonical_supported_but_shadow_missing == 0
    assert report.false_conflict_count == 1
    assert report.agreement_rate == 8 / 11
    assert report.false_conflict_rate == 1 / 11

    species = report.by_field["species"]
    assert species.comparable_slots == 2
    assert species.agreement_count == 2
    assert species.agreement_rate == 1.0

    quantity = report.by_field["plant_quantity"]
    assert quantity.comparable_slots == 2
    assert quantity.shadow_supported_but_canonical_missing == 1

    assert report.matches_expected(fixture.expected_diff)


def test_multilingual_pack_exposes_binding_loss_and_canonical_only_anomaly() -> None:
    fixture = load_benchmark_fixture(MULTILINGUAL)
    report = evaluate_shadow_canonical_diff(fixture.shadow, fixture.canonical)

    assert fixture.pack_id == "pack-multilingual-es-pt-en"
    assert report.comparable_slots == 10
    assert report.agreement_count == 6
    assert report.shadow_supported_but_canonical_missing == 2
    assert report.canonical_supported_but_shadow_missing == 1
    assert report.false_conflict_count == 1
    assert report.agreement_rate == 0.6
    assert report.false_conflict_rate == 0.1

    species = report.by_field["species"]
    assert species.comparable_slots == 3
    assert species.agreement_count == 3
    assert species.agreement_rate == 1.0

    quantity = report.by_field["plant_quantity"]
    assert quantity.comparable_slots == 3
    assert quantity.shadow_supported_but_canonical_missing == 2
    assert quantity.agreement_count == 1

    assert report.matches_expected(fixture.expected_diff)


def test_comparison_is_line_scoped_and_never_matches_same_value_across_lines() -> None:
    fixture = BenchmarkFixture(
        version="p2-shadow-canonical-v1",
        pack_id="line-scope-proof",
        description="Same value on different lines must not count as agreement.",
        shadow={
            "availability": "CURRENT",
            "fields": [
                {
                    "field_key": "species",
                    "state": "SUPPORTED",
                    "values": ["Pinus taeda"],
                    "evidence": [
                        {
                            "scope": "PLANT_COMPONENT",
                            "line_key": "SKU:A",
                            "component_key": None,
                            "normalized_value": "Pinus taeda",
                            "source_filename": "shadow.pdf",
                            "page": 1,
                            "evidence_verified": True,
                        }
                    ],
                }
            ],
        },
        canonical={
            "shipment_fields": {},
            "plant_lines": [
                {
                    "entity_key": "SKU:B",
                    "fields": {
                        "species": {
                            "state": "SUPPORTED",
                            "values": ["Pinus taeda"],
                            "publication_status": "FOUND",
                            "issue_types": [],
                        }
                    },
                }
            ],
        },
        expected_diff={
            "comparable_slots": 2,
            "agreement_count": 0,
            "shadow_supported_but_canonical_missing": 1,
            "canonical_supported_but_shadow_missing": 1,
            "false_conflict_count": 0,
        },
    )

    report = evaluate_shadow_canonical_diff(fixture.shadow, fixture.canonical)

    assert report.comparable_slots == 2
    assert report.agreement_count == 0
    assert {
        item.outcome
        for item in report.entries
    } == {
        ComparisonOutcome.SHADOW_SUPPORTED_BUT_CANONICAL_MISSING,
        ComparisonOutcome.CANONICAL_SUPPORTED_BUT_SHADOW_MISSING,
    }


def test_inconsistent_set_is_false_conflict_even_when_shadow_value_is_present() -> None:
    fixture = BenchmarkFixture(
        version="p2-shadow-canonical-v1",
        pack_id="inconsistent-set-proof",
        description="Canonical ambiguity remains measurable.",
        shadow={
            "availability": "CURRENT",
            "fields": [
                {
                    "field_key": "country_of_harvest",
                    "state": "SUPPORTED",
                    "values": ["Brazil"],
                    "evidence": [
                        {
                            "scope": "PLANT_COMPONENT",
                            "line_key": "SKU:A",
                            "component_key": None,
                            "normalized_value": "Brazil",
                            "source_filename": "shadow.pdf",
                            "page": 1,
                            "evidence_verified": True,
                        }
                    ],
                }
            ],
        },
        canonical={
            "shipment_fields": {},
            "plant_lines": [
                {
                    "entity_key": "SKU:A",
                    "fields": {
                        "country_of_harvest": {
                            "state": "REVIEW_REQUIRED",
                            "values": ["Brazil"],
                            "publication_status": "REVIEW",
                            "issue_types": ["INCONSISTENT_SET"],
                        }
                    },
                }
            ],
        },
        expected_diff={
            "comparable_slots": 1,
            "agreement_count": 0,
            "shadow_supported_but_canonical_missing": 0,
            "canonical_supported_but_shadow_missing": 0,
            "false_conflict_count": 1,
        },
    )

    report = evaluate_shadow_canonical_diff(fixture.shadow, fixture.canonical)

    assert report.false_conflict_count == 1
    assert report.entries[0].outcome is ComparisonOutcome.FALSE_CONFLICT


def test_scorecard_console_report_is_deterministic_and_field_breakdown_is_readable() -> None:
    fixture = load_benchmark_fixture(MULTILINGUAL)
    report = evaluate_shadow_canonical_diff(fixture.shadow, fixture.canonical)

    first = StringIO()
    second = StringIO()
    print_report(report, pack_id=fixture.pack_id, stream=first)
    print_report(report, pack_id=fixture.pack_id, stream=second)

    assert first.getvalue() == second.getvalue()
    output = first.getvalue()
    assert "Shadow vs Canonical Diff" in output
    assert "pack-multilingual-es-pt-en" in output
    assert "shadow_supported_but_canonical_missing" in output
    assert "canonical_supported_but_shadow_missing" in output
    assert "false_conflict_rate" in output
    assert "Quantity" in output
    assert "Species" in output
    assert "100.00%" in output
