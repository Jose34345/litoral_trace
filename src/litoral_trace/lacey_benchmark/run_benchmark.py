from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from pydantic import ValidationError

from .contracts import ShipmentTruth
from .evaluator import SemanticEvaluator
from .scorecard import FieldEvaluation, Scorecard


def _load_truth(path: Path) -> ShipmentTruth:
    try:
        return ShipmentTruth.model_validate_json(path.read_text(encoding="utf-8"))
    except (OSError, ValidationError, ValueError) as exc:
        raise ValueError(f"Invalid ShipmentTruth JSON at {path}: {exc}") from exc


def run_benchmark(expected_dir: Path, actual_dir: Path) -> Scorecard:
    """Evaluate every expected JSON against the same relative actual JSON path."""
    expected_dir = Path(expected_dir)
    actual_dir = Path(actual_dir)
    if not expected_dir.is_dir():
        raise ValueError(f"Expected corpus directory does not exist: {expected_dir}")
    if not actual_dir.is_dir():
        raise ValueError(f"Actual output directory does not exist: {actual_dir}")

    expected_files = sorted(
        expected_dir.rglob("*.json"),
        key=lambda path: path.relative_to(expected_dir).as_posix(),
    )
    if not expected_files:
        raise ValueError(f"Expected corpus contains no JSON files: {expected_dir}")

    global_results: list[FieldEvaluation] = []
    for expected_path in expected_files:
        relative = expected_path.relative_to(expected_dir)
        case_id = relative.as_posix()
        expected = _load_truth(expected_path)
        actual_path = actual_dir / relative
        actual = _load_truth(actual_path) if actual_path.is_file() else ShipmentTruth()
        case_scorecard = SemanticEvaluator.evaluate(
            expected,
            actual,
            case_id=case_id,
        )
        global_results.extend(case_scorecard.results)

    return Scorecard.from_results(global_results)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lacey-golden-benchmark",
        description=(
            "Compare Golden ShipmentTruth JSON files with actual Engine 2 output "
            "JSON files and print one aggregate semantic scorecard."
        ),
    )
    parser.add_argument(
        "expected_dir",
        type=Path,
        help="Directory containing immutable expected ShipmentTruth JSON files.",
    )
    parser.add_argument(
        "actual_dir",
        type=Path,
        help="Directory containing actual ShipmentTruth JSON files at matching paths.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        scorecard = run_benchmark(args.expected_dir, args.actual_dir)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    print(scorecard.render_console())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
