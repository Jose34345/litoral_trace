from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

from litoral_trace.lacey_benchmark.baseline_refresh import (
    regenerate_canonical_from_shadow,
    refreshed_fixture_payload,
)
from litoral_trace.lacey_benchmark.shadow_canonical_diff import (
    evaluate_shadow_canonical_diff,
    load_benchmark_fixture,
)


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
