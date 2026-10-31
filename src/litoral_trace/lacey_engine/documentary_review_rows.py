"""Literal party/description observations for the existing U.S. review projector.

Views retain their persisted source ID and locator. They neither mutate evidence
nor grant authority; the existing per-field validation/review writer still decides.
"""
from __future__ import annotations

from dataclasses import dataclass
import re

from .semantic_graph import party_address, party_core

_RAW = re.compile(r"^raw\.table\.(\d+)\.(.+)$")
_ROW = re.compile(r"(?:^|;)data_row:(\d+)(?:;|$)")
_LABELS = {
    "consignee": "Consignee Name",
    "consignee name": "Consignee Name",
    "consignee's name": "Consignee Name",
    "consignee address": "Consignee Address",
    "consignee's address": "Consignee Address",
    "description of merchandise": "Merchandise Description",
}


@dataclass(frozen=True)
class DocumentaryReviewRow:
    source: object
    field_name: str
    original_value: str
    normalized_value: str

    def __getattr__(self, name):
        return getattr(self.source, name)


def _view(source, table: str, label: str, value: str) -> DocumentaryReviewRow:
    return DocumentaryReviewRow(source, f"raw.table.{table}.{label}", value, value)


def documentary_review_rows(rows):
    """Separate only literal labeled fields; keep uncertain sources unchanged.

    A sparse PDF key/value form and an explicit spreadsheet Field/Value row are
    allowed. Dense product, botanical and BOM rows are not scalar descriptions.
    A combined party splits only at its explicit pipe delimiter when each side
    independently matches the existing name/address parser without loss.
    """
    rows = list(rows)
    tables: dict[str, dict[str, list]] = {}
    for source in rows:
        match, row = _RAW.match(str(source.field_name or "")), _ROW.search(str(source.source_locator or ""))
        if match and row:
            tables.setdefault(match[1], {}).setdefault(row[1], []).append(source)
    replacements: dict[int, list] = {}
    for table, records in tables.items():
        sparse = all(len(record) == 1 for record in records.values())
        for record in records.values():
            labels = {_RAW.match(str(source.field_name))[2].strip().casefold(): source for source in record}
            if set(labels) == {"field", "value"} and len(record) == 2:
                label_source, source = labels["field"], labels["value"]
                label = str(label_source.original_value or "").strip().casefold()
            elif sparse:
                source = record[0]
                label = _RAW.match(str(source.field_name))[2].strip().casefold()
            else:
                continue
            target = _LABELS.get(label)
            value = str(source.original_value or source.normalized_value or "").strip()
            if target is None or not value:
                continue
            if target == "Consignee Name" and value.count("|") == 1:
                name, address = (part.strip() for part in value.split("|"))
                if name and address and party_core(value) == name and party_address(value) == address:
                    replacements[id(source)] = [
                        _view(source, table, "Consignee Name", name),
                        _view(source, table, "Consignee Address", address),
                    ]
                    continue
            replacements[id(source)] = [_view(source, table, target, value)]
    return [view for source in rows for view in replacements.get(id(source), [source])]
