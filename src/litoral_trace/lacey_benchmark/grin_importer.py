from __future__ import annotations

import csv
from pathlib import Path
from typing import ClassVar


class GrinImportError(ValueError):
    pass


class GrinImporter:
    """Normalize an offline GRIN-style CSV export into the benchmark schema.

    Real GRIN exports can vary in column naming. V1 deliberately supports only
    explicit aliases and fails closed when mandatory semantics cannot be mapped.
    """

    OUTPUT_COLUMNS: ClassVar[tuple[str, ...]] = (
        "source_id",
        "scientific_name",
        "genus",
        "species",
        "is_accepted",
        "accepted_source_id",
        "common_names",
        "commercial_aliases",
    )
    COLUMN_ALIASES: ClassVar[dict[str, tuple[str, ...]]] = {
        "source_id": ("source_id", "taxno", "nomen_number", "taxon_id"),
        "scientific_name": ("scientific_name", "taxon_name", "name"),
        "genus": ("genus",),
        "species": ("species", "specific_epithet"),
        "is_accepted": ("is_accepted", "accepted", "is_accepted_name"),
        "accepted_source_id": (
            "accepted_source_id",
            "accepted_taxno",
            "accepted_nomen_number",
        ),
        "common_names": ("common_names", "common_name"),
        "commercial_aliases": ("commercial_aliases", "commercial_alias"),
    }
    REQUIRED: ClassVar[tuple[str, ...]] = (
        "source_id",
        "scientific_name",
        "genus",
        "species",
        "is_accepted",
    )

    @classmethod
    def import_csv(cls, source_path: Path, output_path: Path) -> int:
        with source_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                raise GrinImportError("GRIN CSV has no header")
            normalized_headers = {name.strip().casefold(): name for name in reader.fieldnames}
            mapping: dict[str, str | None] = {}
            for target, aliases in cls.COLUMN_ALIASES.items():
                matches = [normalized_headers[a.casefold()] for a in aliases if a.casefold() in normalized_headers]
                if len(matches) > 1:
                    raise GrinImportError(f"ambiguous GRIN columns for {target}: {matches}")
                mapping[target] = matches[0] if matches else None
            missing = [name for name in cls.REQUIRED if mapping[name] is None]
            if missing:
                raise GrinImportError(f"GRIN CSV missing required semantic columns: {', '.join(missing)}")

            rows: list[dict[str, str]] = []
            for row_number, row in enumerate(reader, start=2):
                normalized: dict[str, str] = {}
                for target in cls.OUTPUT_COLUMNS:
                    source_col = mapping[target]
                    normalized[target] = (row.get(source_col, "") if source_col else "").strip()
                for key in ("source_id", "scientific_name", "genus", "species"):
                    if not normalized[key]:
                        raise GrinImportError(f"row {row_number} has blank {key}")
                normalized["is_accepted"] = cls._parse_bool(
                    normalized["is_accepted"], row_number=row_number
                )
                if normalized["is_accepted"] == "true":
                    normalized["accepted_source_id"] = ""
                elif not normalized["accepted_source_id"]:
                    raise GrinImportError(
                        f"row {row_number} is a synonym without accepted_source_id"
                    )
                rows.append(normalized)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(cls.OUTPUT_COLUMNS), lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
        return len(rows)

    @staticmethod
    def _parse_bool(value: str, *, row_number: int) -> str:
        normalized = value.strip().casefold()
        if normalized in {"true", "1", "yes", "y", "accepted"}:
            return "true"
        if normalized in {"false", "0", "no", "n", "synonym"}:
            return "false"
        raise GrinImportError(f"row {row_number} has invalid is_accepted value: {value!r}")
