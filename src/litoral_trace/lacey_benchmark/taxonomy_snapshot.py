from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from types import MappingProxyType
from typing import Mapping
import unicodedata

from litoral_trace.lacey_benchmark.taxonomy_contracts import (
    TaxonRecord,
    TaxonomySnapshotManifest,
)


class TaxonomySnapshotError(ValueError):
    pass


def normalize_taxon_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value)
    return " ".join(normalized.split()).casefold()


def _split_aliases(value: str) -> tuple[str, ...]:
    normalized = value.replace(";", "|")
    return tuple(part.strip() for part in normalized.split("|") if part.strip())


class TaxonomySnapshot:
    """Checksum-verified immutable indexes over a normalized taxonomy export."""

    REQUIRED_COLUMNS = {
        "source_id",
        "scientific_name",
        "genus",
        "species",
        "is_accepted",
        "accepted_source_id",
        "common_names",
        "commercial_aliases",
    }

    def __init__(
        self,
        *,
        manifest: TaxonomySnapshotManifest,
        records_by_id: Mapping[str, TaxonRecord],
        accepted_by_id: Mapping[str, TaxonRecord],
        accepted_name_index: Mapping[str, str],
        synonym_name_index: Mapping[str, str],
        pair_index: Mapping[tuple[str, str], str],
        alias_index: Mapping[str, tuple[str, ...]],
        epithet_index: Mapping[str, tuple[str, ...]],
    ) -> None:
        self.manifest = manifest
        self.records_by_id = records_by_id
        self.accepted_by_id = accepted_by_id
        self.accepted_name_index = accepted_name_index
        self.synonym_name_index = synonym_name_index
        self.pair_index = pair_index
        self.alias_index = alias_index
        self.epithet_index = epithet_index

    @classmethod
    def load(cls, csv_path: Path, manifest_path: Path) -> "TaxonomySnapshot":
        try:
            manifest = TaxonomySnapshotManifest.model_validate_json(
                manifest_path.read_text(encoding="utf-8")
            )
        except Exception as exc:  # pydantic/json/path errors become one fail-closed contract
            raise TaxonomySnapshotError(f"invalid taxonomy manifest: {exc}") from exc

        if manifest.schema_version != 1:
            raise TaxonomySnapshotError(
                f"unsupported taxonomy schema_version: {manifest.schema_version}"
            )

        actual_sha = hashlib.sha256(csv_path.read_bytes()).hexdigest()
        if actual_sha != manifest.sha256:
            raise TaxonomySnapshotError(
                f"taxonomy snapshot sha256 mismatch: expected {manifest.sha256}, got {actual_sha}"
            )

        records: dict[str, TaxonRecord] = {}
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            fields = set(reader.fieldnames or ())
            missing = cls.REQUIRED_COLUMNS - fields
            if missing:
                raise TaxonomySnapshotError(
                    f"taxonomy snapshot missing columns: {', '.join(sorted(missing))}"
                )
            for row_number, row in enumerate(reader, start=2):
                raw_is_accepted = (row["is_accepted"] or "").strip().casefold()
                if raw_is_accepted not in {"true", "false"}:
                    raise TaxonomySnapshotError(
                        f"invalid taxonomy row {row_number}: is_accepted must be true or false"
                    )
                try:
                    record = TaxonRecord(
                        source_id=(row["source_id"] or "").strip(),
                        scientific_name=(row["scientific_name"] or "").strip(),
                        genus=(row["genus"] or "").strip(),
                        species=(row["species"] or "").strip(),
                        is_accepted=raw_is_accepted == "true",
                        accepted_source_id=(row["accepted_source_id"] or "").strip() or None,
                        common_names=_split_aliases(row["common_names"] or ""),
                        commercial_aliases=_split_aliases(row["commercial_aliases"] or ""),
                    )
                except Exception as exc:
                    raise TaxonomySnapshotError(f"invalid taxonomy row {row_number}: {exc}") from exc
                if record.source_id in records:
                    raise TaxonomySnapshotError(f"duplicate taxonomy source_id: {record.source_id}")
                records[record.source_id] = record

        accepted = {key: record for key, record in records.items() if record.is_accepted}
        if not accepted:
            raise TaxonomySnapshotError("taxonomy snapshot contains no accepted taxa")

        for record in records.values():
            if not record.is_accepted:
                target = records.get(record.accepted_source_id or "")
                if target is None or not target.is_accepted:
                    raise TaxonomySnapshotError(
                        f"synonym {record.source_id} targets missing/non-accepted taxon "
                        f"{record.accepted_source_id}"
                    )

        accepted_names: dict[str, str] = {}
        synonym_names: dict[str, str] = {}
        pairs: dict[tuple[str, str], str] = {}
        aliases: dict[str, set[str]] = {}
        epithets: dict[str, set[str]] = {}

        def put_unique(index: dict, key, canonical_id: str, label: str) -> None:
            existing = index.get(key)
            if existing is not None and existing != canonical_id:
                raise TaxonomySnapshotError(
                    f"conflicting canonical targets for {label} {key!r}: {existing}, {canonical_id}"
                )
            index[key] = canonical_id

        for record in records.values():
            canonical_id = record.source_id if record.is_accepted else record.accepted_source_id
            assert canonical_id is not None
            name_key = normalize_taxon_text(record.scientific_name)
            if record.is_accepted:
                put_unique(accepted_names, name_key, canonical_id, "accepted name")
                pair_key = (
                    normalize_taxon_text(record.genus),
                    normalize_taxon_text(record.species),
                )
                put_unique(pairs, pair_key, canonical_id, "genus/species pair")
                epithets.setdefault(normalize_taxon_text(record.species), set()).add(canonical_id)
            else:
                put_unique(synonym_names, name_key, canonical_id, "synonym")
            for alias in (*record.common_names, *record.commercial_aliases):
                aliases.setdefault(normalize_taxon_text(alias), set()).add(canonical_id)

        return cls(
            manifest=manifest,
            records_by_id=MappingProxyType(dict(records)),
            accepted_by_id=MappingProxyType(dict(accepted)),
            accepted_name_index=MappingProxyType(dict(accepted_names)),
            synonym_name_index=MappingProxyType(dict(synonym_names)),
            pair_index=MappingProxyType(dict(pairs)),
            alias_index=MappingProxyType(
                {key: tuple(sorted(values)) for key, values in aliases.items()}
            ),
            epithet_index=MappingProxyType(
                {key: tuple(sorted(values)) for key, values in epithets.items()}
            ),
        )

    def canonical_record(self, source_id: str) -> TaxonRecord:
        try:
            return self.accepted_by_id[source_id]
        except KeyError as exc:
            raise TaxonomySnapshotError(f"unknown accepted taxon id: {source_id}") from exc

    def candidate_terms(self) -> tuple[tuple[str, str, str], ...]:
        """Return stable (term, canonical_id, provenance) tuples for review-only matching."""
        candidates: set[tuple[str, str, str]] = set()
        for term, source_id in self.accepted_name_index.items():
            candidates.add((term, source_id, "accepted_name"))
        for term, source_id in self.synonym_name_index.items():
            candidates.add((term, source_id, "synonym"))
        for term, source_ids in self.alias_index.items():
            for source_id in source_ids:
                candidates.add((term, source_id, "alias"))
        return tuple(sorted(candidates))
