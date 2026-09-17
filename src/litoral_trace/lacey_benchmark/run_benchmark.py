from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from pydantic import ValidationError

from .contracts import ShipmentTruth
from .evaluator import SemanticEvaluator
from .scorecard import FieldEvaluation, Scorecard
from .taxonomy_benchmark import TaxonomyAwareSemanticEvaluator
from .taxonomy_resolver import TaxonomyResolver
from .taxonomy_snapshot import TaxonomySnapshot


def _load_truth(path: Path) -> ShipmentTruth:
    try:
        return ShipmentTruth.model_validate_json(path.read_text(encoding="utf-8"))
    except (OSError, ValidationError, ValueError) as exc:
        raise ValueError(f"Invalid ShipmentTruth JSON at {path}: {exc}") from exc


def run_benchmark(
    expected_dir: Path,
    actual_dir: Path,
    *,
    taxonomy_csv: Path | None = None,
    taxonomy_manifest: Path | None = None,
) -> Scorecard:
    """Evaluate expected JSON against matching actual JSON paths.

    Taxonomy-aware comparison is opt-in. When enabled, only authoritative
    resolver outcomes normalize actual botanical values; aliases and fuzzy
    candidates remain review-only and are scored as submitted.
    """
    expected_dir = Path(expected_dir)
    actual_dir = Path(actual_dir)
    if not expected_dir.is_dir():
        raise ValueError(f"Expected corpus directory does not exist: {expected_dir}")
    if not actual_dir.is_dir():
        raise ValueError(f"Actual output directory does not exist: {actual_dir}")

    if (taxonomy_csv is None) != (taxonomy_manifest is None):
        raise ValueError("taxonomy_csv and taxonomy_manifest must be provided together")

    resolver: TaxonomyResolver | None = None
    if taxonomy_csv is not None and taxonomy_manifest is not None:
        resolver = TaxonomyResolver(
            TaxonomySnapshot.load(Path(taxonomy_csv), Path(taxonomy_manifest))
        )

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
        if resolver is None:
            case_scorecard = SemanticEvaluator.evaluate(
                expected,
                actual,
                case_id=case_id,
            )
        else:
            case_scorecard = TaxonomyAwareSemanticEvaluator.evaluate(
                expected,
                actual,
                resolver=resolver,
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
    parser.add_argument(
        "--taxonomy-csv",
        type=Path,
        default=None,
        help="Optional authoritative taxonomy CSV snapshot.",
    )
    parser.add_argument(
        "--taxonomy-manifest",
        type=Path,
        default=None,
        help="Manifest for --taxonomy-csv; both options are required together.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        scorecard = run_benchmark(
            args.expected_dir,
            args.actual_dir,
            taxonomy_csv=args.taxonomy_csv,
            taxonomy_manifest=args.taxonomy_manifest,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    print(scorecard.render_console())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
