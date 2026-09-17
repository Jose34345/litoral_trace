from __future__ import annotations

from decimal import Decimal
import hashlib
import json
from pathlib import Path

from litoral_trace.lacey_benchmark.grin_importer import GrinImporter
from litoral_trace.lacey_benchmark.special_use import SpecialUseRegistry
from litoral_trace.lacey_benchmark.taxonomy_contracts import (
    TaxonResolutionStatus,
    TaxonomySnapshotManifest,
)
from litoral_trace.lacey_benchmark.taxonomy_resolver import TaxonomyResolver
from litoral_trace.lacey_benchmark.taxonomy_snapshot import TaxonomySnapshot, TaxonomySnapshotError


def _write_manifest(csv_path: Path, manifest_path: Path, *, version: str = "test-v1") -> None:
    digest = hashlib.sha256(csv_path.read_bytes()).hexdigest()
    manifest = TaxonomySnapshotManifest(
        source="USDA GRIN Taxonomy test export",
        snapshot_version=version,
        snapshot_date="2026-09-16",
        source_url="https://npgsweb.ars-grin.gov/gringlobal/taxon/taxonomysearch",
        sha256=digest,
        schema_version=1,
    )
    manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")


def _snapshot(tmp_path: Path) -> TaxonomySnapshot:
    raw = tmp_path / "grin_raw.csv"
    normalized = tmp_path / "taxa.csv"
    manifest = tmp_path / "manifest.json"
    raw.write_text(
        "taxno,scientific_name,genus,species,is_accepted,accepted_taxno,common_names,commercial_aliases\n"
        "15924,Eucalyptus grandis,Eucalyptus,grandis,true,,rose gum,blue gum trade\n"
        "28556,Pinus taeda,Pinus,taeda,true,,loblolly pine,trade pine\n"
        "30000,Pinus contorta,Pinus,contorta,true,,lodgepole pine,trade pine\n"
        "SYN-1,Eucalyptus oldname,Eucalyptus,oldname,false,15924,,\n",
        encoding="utf-8",
    )
    assert GrinImporter.import_csv(raw, normalized) == 4
    _write_manifest(normalized, manifest)
    return TaxonomySnapshot.load(normalized, manifest)


def test_grin_importer_and_snapshot_are_checksum_verified(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path)
    assert snapshot.manifest.snapshot_version == "test-v1"
    assert snapshot.accepted_by_id["15924"].scientific_name == "Eucalyptus grandis"

    csv_path = tmp_path / "taxa.csv"
    csv_path.write_text(csv_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    try:
        TaxonomySnapshot.load(csv_path, tmp_path / "manifest.json")
    except TaxonomySnapshotError as exc:
        assert "sha256" in str(exc).lower()
    else:
        raise AssertionError("checksum mismatch must fail closed")


def test_resolver_exact_synonym_alias_candidate_and_ambiguity(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path)
    resolver = TaxonomyResolver(snapshot, candidate_review_threshold=0.75)

    accepted = resolver.resolve(scientific_name="Eucalyptus grandis")
    assert accepted.status is TaxonResolutionStatus.EXACT_ACCEPTED
    assert accepted.canonical_source_id == "15924"
    assert accepted.review_required is False

    synonym = resolver.resolve(scientific_name="Eucalyptus oldname")
    assert synonym.status is TaxonResolutionStatus.EXACT_SYNONYM
    assert synonym.canonical_source_id == "15924"
    assert synonym.review_required is False

    pair = resolver.resolve(genus="Eucalyptus", species="grandis")
    assert pair.status is TaxonResolutionStatus.GENUS_SPECIES
    assert pair.canonical_source_id == "15924"

    full_species = resolver.resolve(genus="Eucalyptus", species="Eucalyptus grandis")
    assert full_species.canonical_source_id == "15924"

    common_alias = resolver.resolve(scientific_name="rose gum")
    assert common_alias.status is TaxonResolutionStatus.EXACT_ALIAS
    assert common_alias.canonical_source_id == "15924"
    assert common_alias.review_required is True

    ambiguous_alias = resolver.resolve(scientific_name="trade pine")
    assert ambiguous_alias.status is TaxonResolutionStatus.AMBIGUOUS
    assert ambiguous_alias.canonical_source_id is None
    assert ambiguous_alias.review_required is True
    assert len(ambiguous_alias.candidates) == 2

    candidate = resolver.resolve(scientific_name="Eucaliptus grandis")
    assert candidate.status is TaxonResolutionStatus.CANDIDATE
    assert candidate.canonical_source_id is None
    assert candidate.review_required is True
    assert candidate.candidates[0].canonical_source_id == "15924"

    epithet_only = resolver.resolve(species="grandis")
    assert epithet_only.status is TaxonResolutionStatus.AMBIGUOUS
    assert epithet_only.canonical_source_id is None
    assert epithet_only.review_required is True


def test_review_threshold_filters_candidates_without_promoting_them(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path)
    permissive = TaxonomyResolver(snapshot, candidate_review_threshold=0.70)
    strict = TaxonomyResolver(snapshot, candidate_review_threshold=0.9999)

    assert permissive.resolve(scientific_name="Eucaliptus grandis").status is TaxonResolutionStatus.CANDIDATE
    strict_result = strict.resolve(scientific_name="Eucaliptus grandis")
    assert strict_result.status is TaxonResolutionStatus.NOT_FOUND
    assert strict_result.canonical_source_id is None


def test_aphis_species_groups_and_suds_are_versioned_reference_data() -> None:
    path = Path("benchmarks/lacey/reference/aphis/sud.json")
    registry = SpecialUseRegistry.load(path)

    spf = registry.resolve("SPECIAL", "SPF")
    assert spf is not None
    assert "Pinus contorta" in spf.members
    assert "Picea glauca" in spf.members

    aath = registry.resolve("TEMP", "AATH")
    assert aath is not None
    assert set(aath.members) == {"Abies amabilis", "Tsuga heterophylla"}

    assert registry.resolve("SPECIAL", "NOT-A-REAL-SUD") is None


def test_repository_golden_taxonomy_snapshot_resolves_known_benchmark_taxa() -> None:
    snapshot = TaxonomySnapshot.load(
        Path("benchmarks/lacey/reference/grin/taxa.csv"),
        Path("benchmarks/lacey/reference/grin/manifest.json"),
    )
    resolver = TaxonomyResolver(snapshot, candidate_review_threshold=0.85)

    assert resolver.resolve(scientific_name="Pinus taeda").canonical_source_id == "28556"
    assert resolver.resolve(genus="Eucalyptus", species="grandis").canonical_source_id == "15924"
    assert resolver.resolve(scientific_name="loblolly pine").review_required is True
