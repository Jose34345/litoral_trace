from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

from litoral_trace.lacey_benchmark.adversarial_fixtures import (
    AUTHORITY_CONFLICT_FILE,
    BROKEN_ARITHMETIC_FILE,
    ORPHAN_POSITIONAL_FILE,
    adversarial_fixture_seeds,
)
from litoral_trace.lacey_benchmark.baseline_refresh import (
    regenerate_canonical_from_shadow,
    regenerate_truth_from_shadow,
    refresh_adversarial_fixtures,
    refreshed_fixture_from_model,
    refreshed_fixture_payload,
)
from litoral_trace.lacey_benchmark.shadow_canonical_diff import (
    BenchmarkFixture,
    evaluate_shadow_canonical_diff,
    load_benchmark_fixture,
)


def load_benchmark_fixture_from_payload(payload: dict) -> BenchmarkFixture:
    return BenchmarkFixture(**payload)


ROOT = Path(__file__).resolve().parents[2]
MULTILINGUAL = ROOT / "benchmarks/lacey/v2/pack_multilingual_es_pt_en_shadow_canonical.json"


def test_multilingual_shadow_refresh_runs_current_canonical_pipeline() -> None:
    fixture = load_benchmark_fixture(MULTILINGUAL)
    canonical = regenerate_canonical_from_shadow(fixture.shadow)

    assert len(canonical.plant_lines) == 3
    by_line = {line.entity_key: line for line in canonical.plant_lines}

    expected = {
        "SKU:ACT-TRAY-18": {
            "plant_quantity": ("315",),
            "metric_unit": ("KG",),
            "entered_value": ("18900",),
        },
        "SKU:RUB-CB-32": {
            "plant_quantity": ("510",),
            "metric_unit": ("KG",),
            "entered_value": ("17760",),
        },
        "SKU:TEK-SRV-04": {
            "plant_quantity": ("145",),
            "metric_unit": ("KG",),
            "entered_value": ("11200",),
        },
    }
    for line_key, fields in expected.items():
        line = by_line[line_key]
        for field_key, values in fields.items():
            assert line.fields[field_key].values == values
            assert line.fields[field_key].state == "SUPPORTED"


def test_checked_in_multilingual_baseline_is_generated_not_hand_edited() -> None:
    current = json.loads(MULTILINGUAL.read_text(encoding="utf-8"))
    refreshed = refreshed_fixture_payload(current)

    assert current["canonical"] == refreshed["canonical"], (
        "Canonical benchmark snapshot is stale. Regenerated value:\n"
        + json.dumps(refreshed["canonical"], indent=2, ensure_ascii=False)
    )
    assert current["expected_diff"] == refreshed["expected_diff"], (
        "expected_diff is stale. Regenerated value:\n"
        + json.dumps(refreshed["expected_diff"], indent=2, ensure_ascii=False)
    )


def test_multilingual_quantitative_diff_is_zero_and_cli_accepts_baseline(tmp_path) -> None:
    current = json.loads(MULTILINGUAL.read_text(encoding="utf-8"))
    refreshed = refreshed_fixture_payload(current)
    refreshed_path = tmp_path / "multilingual-refreshed.json"
    refreshed_path.write_text(
        json.dumps(refreshed, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    fixture = load_benchmark_fixture(refreshed_path)
    report = evaluate_shadow_canonical_diff(fixture.shadow, fixture.canonical)

    for field_key in ("plant_quantity", "metric_unit", "entered_value"):
        summary = report.by_field[field_key]
        assert summary.shadow_supported_but_canonical_missing == 0
        assert summary.false_conflict_count == 0

    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        filter(
            None,
            (
                str(ROOT / "src"),
                env.get("PYTHONPATH"),
            ),
        )
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "litoral_trace.lacey_benchmark.shadow_canonical_diff",
            str(refreshed_path),
        ],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr + "\n" + completed.stdout
    assert "shadow_supported_but_canonical_missing" in completed.stdout
    assert "false_conflict_rate" in completed.stdout

ADVERSARIAL_DIR = ROOT / "benchmarks/lacey/v2"


def test_adversarial_fixture_files_are_generated_from_typed_pydantic_seeds() -> None:
    for filename, seed in adversarial_fixture_seeds().items():
        current = json.loads((ADVERSARIAL_DIR / filename).read_text(encoding="utf-8"))
        regenerated = refreshed_fixture_from_model(seed)
        assert current == regenerated, (
            f"{filename} is stale or hand-edited. Regenerated fixture:\n"
            + json.dumps(regenerated, indent=2, ensure_ascii=False)
        )


def test_adversarial_refresh_pipeline_materializes_all_three_fixtures(tmp_path) -> None:
    generated = refresh_adversarial_fixtures(tmp_path)

    assert set(generated) == {
        ORPHAN_POSITIONAL_FILE,
        AUTHORITY_CONFLICT_FILE,
        BROKEN_ARITHMETIC_FILE,
    }
    for filename, payload in generated.items():
        assert json.loads((tmp_path / filename).read_text(encoding="utf-8")) == payload


def test_orphan_positional_attack_fails_closed_without_bad_quantity_binding() -> None:
    seed = adversarial_fixture_seeds()[ORPHAN_POSITIONAL_FILE]
    truth = regenerate_truth_from_shadow(seed.shadow)
    refreshed = refreshed_fixture_from_model(seed)
    fixture = load_benchmark_fixture_from_payload(refreshed)
    report = evaluate_shadow_canonical_diff(fixture.shadow, fixture.canonical)

    assert {line.entity_key for line in truth.plant_lines} == {
        "invoice:items:row:1",
        "invoice:items:row:2",
    }
    assert truth.unresolved_component_keys == (
        "taxon:eucalyptus:grandis",
        "taxon:pinus:taeda",
    )
    assert all("plant_quantity" not in line.fields for line in truth.plant_lines)
    assert all("metric_unit" not in line.fields for line in truth.plant_lines)
    assert report.comparable_slots == 8
    assert report.agreement_count == 2
    assert report.shadow_supported_but_canonical_missing == 6
    assert report.false_conflict_count == 0
    assert report.safe_review_count == 0


def test_authority_conflict_is_safe_review_not_false_conflict() -> None:
    seed = adversarial_fixture_seeds()[AUTHORITY_CONFLICT_FILE]
    truth = regenerate_truth_from_shadow(seed.shadow)
    refreshed = refreshed_fixture_from_model(seed)
    fixture = load_benchmark_fixture_from_payload(refreshed)
    report = evaluate_shadow_canonical_diff(fixture.shadow, fixture.canonical)

    assert len(truth.plant_lines) == 1
    line = truth.plant_lines[0]
    assert line.fields["genus"].state.value == "CONFLICT"
    assert line.fields["species"].state.value == "CONFLICT"
    assert set(line.fields["species"].values) == {
        "Eucalyptus grandis",
        "Tectona grandis",
    }

    canonical_line = fixture.canonical.plant_lines[0]
    assert canonical_line.fields["species"].issue_types == ("INCONSISTENT_SET",)
    assert report.comparable_slots == 4
    assert report.agreement_count == 2
    assert report.safe_review_count == 2
    assert report.false_conflict_count == 0


def test_broken_arithmetic_aborts_single_line_total_inference() -> None:
    seed = adversarial_fixture_seeds()[BROKEN_ARITHMETIC_FILE]
    truth = regenerate_truth_from_shadow(seed.shadow)
    refreshed = refreshed_fixture_from_model(seed)
    fixture = load_benchmark_fixture_from_payload(refreshed)
    report = evaluate_shadow_canonical_diff(fixture.shadow, fixture.canonical)

    assert len(truth.plant_lines) == 1
    line = truth.plant_lines[0]
    assert line.entity_key == "SKU:EG-ONE-01"
    assert "entered_value" not in line.fields
    assert truth.unresolved_component_keys == ()
    assert report.comparable_slots == 5
    assert report.agreement_count == 4
    assert report.shadow_supported_but_canonical_missing == 1
    assert report.false_conflict_count == 0
    assert report.safe_review_count == 0


def test_adversarial_cli_baselines_are_accepted(tmp_path) -> None:
    generated = refresh_adversarial_fixtures(tmp_path)
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, (str(ROOT / "src"), env.get("PYTHONPATH")))
    )

    for filename in generated:
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "litoral_trace.lacey_benchmark.shadow_canonical_diff",
                str(tmp_path / filename),
            ],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr + "\n" + completed.stdout
        assert "false_conflict_rate" in completed.stdout
        assert "safe_review_rate" in completed.stdout

