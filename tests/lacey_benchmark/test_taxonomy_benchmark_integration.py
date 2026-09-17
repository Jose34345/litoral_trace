from __future__ import annotations

import ast
from pathlib import Path

import pytest

from litoral_trace.lacey_benchmark.contracts import (
    CanonicalField,
    EvalClassification,
    FieldStatus,
    PlantLine,
    ShipmentTruth,
)
from litoral_trace.lacey_benchmark.regulatory_ruleset import RegulatoryRuleSet
from litoral_trace.lacey_benchmark.run_benchmark import run_benchmark
from litoral_trace.lacey_benchmark.special_use import SpecialUseRegistry
from litoral_trace.lacey_benchmark.taxonomy_benchmark import TaxonomyAwareSemanticEvaluator
from litoral_trace.lacey_benchmark.taxonomy_resolver import TaxonomyResolver
from litoral_trace.lacey_benchmark.taxonomy_snapshot import TaxonomySnapshot, TaxonomySnapshotError


REFERENCE_ROOT = Path("benchmarks/lacey/reference")


def _resolver() -> TaxonomyResolver:
    snapshot = TaxonomySnapshot.load(
        REFERENCE_ROOT / "grin" / "taxa.csv",
        REFERENCE_ROOT / "grin" / "manifest.json",
    )
    return TaxonomyResolver(snapshot, candidate_review_threshold=0.85)


def _field(value: str | None, status: FieldStatus = FieldStatus.AUTO_SUPPORTED) -> CanonicalField[str]:
    return CanonicalField[str](value=value, status=status, confidence=1.0)


def _line(line_id: str, *, genus: str, species: str) -> PlantLine:
    return PlantLine(
        line_id=line_id,
        hts=_field(None, FieldStatus.MISSING),
        genus=_field(genus),
        species=_field(species),
        quantity=CanonicalField(value=None, status=FieldStatus.MISSING, confidence=1.0),
    )


def test_taxonomy_aware_benchmark_collapses_only_authoritative_equivalence() -> None:
    expected = ShipmentTruth(plant_lines=[_line("1", genus="Eucalyptus", species="grandis")])
    actual = ShipmentTruth(
        plant_lines=[_line("1", genus="Eucalyptus", species="Eucalyptus grandis")]
    )

    scorecard = TaxonomyAwareSemanticEvaluator.evaluate(expected, actual, resolver=_resolver())
    by_field = {result.field_name: result for result in scorecard.results}

    assert by_field["genus"].classification is EvalClassification.TRUE_SAFE
    assert by_field["species"].classification is EvalClassification.TRUE_SAFE
    assert by_field["species"].actual_value == "grandis"


def test_taxonomy_aware_benchmark_never_promotes_alias_or_fuzzy_candidate() -> None:
    expected = ShipmentTruth(plant_lines=[_line("1", genus="Eucalyptus", species="grandis")])

    alias_actual = ShipmentTruth(
        plant_lines=[_line("1", genus="Eucalyptus", species="rose gum")]
    )
    alias_score = TaxonomyAwareSemanticEvaluator.evaluate(expected, alias_actual, resolver=_resolver())
    alias_species = next(item for item in alias_score.results if item.field_name == "species")
    assert alias_species.classification is EvalClassification.FALSE_SAFE
    assert alias_species.actual_value == "rose gum"

    typo_actual = ShipmentTruth(
        plant_lines=[_line("1", genus="Eucalyptus", species="Eucaliptus grandis")]
    )
    typo_score = TaxonomyAwareSemanticEvaluator.evaluate(expected, typo_actual, resolver=_resolver())
    typo_species = next(item for item in typo_score.results if item.field_name == "species")
    assert typo_species.classification is EvalClassification.FALSE_SAFE
    assert typo_species.actual_value == "Eucaliptus grandis"


def test_runner_can_opt_in_to_taxonomy_aware_comparison(tmp_path: Path) -> None:
    expected_dir = tmp_path / "expected"
    actual_dir = tmp_path / "actual"
    expected_dir.mkdir()
    actual_dir.mkdir()

    expected = ShipmentTruth(plant_lines=[_line("1", genus="Eucalyptus", species="grandis")])
    actual = ShipmentTruth(
        plant_lines=[_line("1", genus="Eucalyptus", species="Eucalyptus grandis")]
    )
    (expected_dir / "pack.json").write_text(expected.model_dump_json(), encoding="utf-8")
    (actual_dir / "pack.json").write_text(actual.model_dump_json(), encoding="utf-8")

    raw = run_benchmark(expected_dir, actual_dir)
    assert raw.counts[EvalClassification.FALSE_SAFE] == 1

    taxonomy_aware = run_benchmark(
        expected_dir,
        actual_dir,
        taxonomy_csv=REFERENCE_ROOT / "grin" / "taxa.csv",
        taxonomy_manifest=REFERENCE_ROOT / "grin" / "manifest.json",
    )
    assert taxonomy_aware.counts[EvalClassification.FALSE_SAFE] == 0
    assert taxonomy_aware.counts[EvalClassification.TRUE_SAFE] == 2


def test_v1_snapshot_loaders_reject_unsupported_schema_versions(tmp_path: Path) -> None:
    grin_csv = REFERENCE_ROOT / "grin" / "taxa.csv"
    manifest = (REFERENCE_ROOT / "grin" / "manifest.json").read_text(encoding="utf-8")
    bad_manifest = tmp_path / "manifest.json"
    bad_manifest.write_text(manifest.replace('"schema_version": 1', '"schema_version": 2'), encoding="utf-8")

    with pytest.raises(TaxonomySnapshotError, match="schema_version"):
        TaxonomySnapshot.load(grin_csv, bad_manifest)

    ruleset_text = (REFERENCE_ROOT / "aphis" / "ruleset.json").read_text(encoding="utf-8")
    bad_ruleset = tmp_path / "ruleset.json"
    bad_ruleset.write_text(ruleset_text.replace('"schema_version": 1', '"schema_version": 2'), encoding="utf-8")
    with pytest.raises(ValueError, match="schema_version"):
        RegulatoryRuleSet.load(bad_ruleset)

    sud_text = (REFERENCE_ROOT / "aphis" / "sud.json").read_text(encoding="utf-8")
    bad_sud = tmp_path / "sud.json"
    bad_sud.write_text(sud_text.replace('"schema_version": 1', '"schema_version": 2'), encoding="utf-8")
    with pytest.raises(ValueError, match="schema_version"):
        SpecialUseRegistry.load(bad_sud)


def test_taxonomy_snapshot_rejects_non_boolean_acceptance_flag(tmp_path: Path) -> None:
    source = REFERENCE_ROOT / "grin" / "taxa.csv"
    invalid_csv = tmp_path / "taxa.csv"
    invalid_csv.write_text(
        source.read_text(encoding="utf-8").replace(
            "15924,Eucalyptus grandis,Eucalyptus,grandis,true,,",
            "15924,Eucalyptus grandis,Eucalyptus,grandis,maybe,28556,",
        ),
        encoding="utf-8",
    )

    import hashlib
    import json

    manifest_payload = json.loads(
        (REFERENCE_ROOT / "grin" / "manifest.json").read_text(encoding="utf-8")
    )
    manifest_payload["sha256"] = hashlib.sha256(invalid_csv.read_bytes()).hexdigest()
    invalid_manifest = tmp_path / "manifest.json"
    invalid_manifest.write_text(json.dumps(manifest_payload), encoding="utf-8")

    with pytest.raises(TaxonomySnapshotError, match="is_accepted"):
        TaxonomySnapshot.load(invalid_csv, invalid_manifest)


def test_new_benchmark_kernel_has_no_runtime_or_network_imports() -> None:
    root = Path("src/litoral_trace/lacey_benchmark")
    kernel_files = [
        "contracts.py",
        "evaluator.py",
        "scorecard.py",
        "run_benchmark.py",
        "taxonomy_contracts.py",
        "grin_importer.py",
        "taxonomy_snapshot.py",
        "taxonomy_resolver.py",
        "taxonomy_benchmark.py",
        "special_use.py",
        "regulatory_contracts.py",
        "regulatory_ruleset.py",
        "regulatory_engine.py",
    ]
    forbidden_prefixes = (
        "litoral_trace.lacey_engine",
        "litoral_trace.us_lacey",
        "litoral_trace.db",
        "fastapi",
        "sqlalchemy",
        "requests",
        "httpx",
        "urllib.request",
    )

    violations: list[str] = []
    for filename in kernel_files:
        path = root / filename
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            imported: list[str] = []
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)
            for module in imported:
                if module.startswith(forbidden_prefixes):
                    violations.append(f"{filename}: {module}")

    assert violations == []
