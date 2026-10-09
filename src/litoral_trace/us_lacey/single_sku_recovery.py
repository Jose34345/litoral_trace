"""Conservative identity recovery for colonless *single-SKU* PDF shipments.

This runs only after Engine 2 reconciles independent source documents. It never
infers the identity of a multi-line shipment, a source without an invoice row,
or a botanical claim with ambiguous genus/species. All original evidence and
provenance remain unchanged; only the missing line/component associations are
filled when independently corroborated by explicit SKU labels.
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Mapping

_INVOICE_ROW = re.compile(
    r"^\s*(?P<ordinal>[1-9][0-9]*)\s*/\s*(?P<sku>[A-Z][A-Z0-9]*[-_][A-Z0-9][A-Z0-9._-]{2,40})(?=\s|$)",
    re.I,
)
_MANUFACTURER_SKU = re.compile(
    r"^\s*manufacturer\s+sku\s+(?P<sku>[A-Z][A-Z0-9]*[-_][A-Z0-9][A-Z0-9._-]{2,40})(?=\s|$)",
    re.I,
)
_COMPONENT_FIELDS = frozenset({
    "article_component", "genus", "species", "country_of_harvest",
    "plant_quantity", "metric_unit", "percent_recycled",
})
_LINE_FIELDS = frozenset({"hts_code", "description", "entered_value"})
_SUPPORTED = frozenset({"SUPPORTED", "SUPPORTED_MULTIPLE"})


def _source_lines(document: Mapping) -> tuple[str, ...]:
    resolution = document.get("resolution")
    if not isinstance(resolution, Mapping):
        return ()
    layout = resolution.get("layout")
    if not isinstance(layout, Mapping):
        return ()
    blocks = layout.get("blocks")
    if not isinstance(blocks, (list, tuple)):
        return ()
    return tuple(
        str(block.get("text") or "")
        for block in blocks
        if isinstance(block, Mapping) and str(block.get("block_type") or "") in {"TEXT_LINE", "OCR_LINE"}
    )


def _single_supported_value(field: object) -> str | None:
    if not isinstance(field, Mapping) or field.get("state") not in _SUPPORTED:
        return None
    values = field.get("values")
    if not isinstance(values, list) or len(values) != 1:
        return None
    value = values[0].get("value") if isinstance(values[0], Mapping) else None
    return str(value).strip() if value and str(value).strip() else None


def bind_single_sku_evidence(fields: dict, documents: object) -> bool:
    """Associate currently unbound, uniquely supported fields with a *proven* SKU.

    The commercial invoice must have exactly one explicit slash-form SKU row,
    preceded by its line/SKU heading. At least two other source documents must
    independently label the very same manufacturer's SKU. This ensures a single
    component's botanicals cannot drift onto a different commercial product.
    """
    if not isinstance(documents, (list, tuple)) or not documents:
        return False
    # Do not reassign partially bound shipment evidence or an existing key.
    for field_name in _COMPONENT_FIELDS | _LINE_FIELDS:
        field = fields.get(field_name)
        if not isinstance(field, Mapping):
            continue
        for row in field.get("supporting_evidence") or ():
            if not isinstance(row, Mapping) or row.get("line_key") or row.get("component_key"):
                return False

    invoice_rows: list[tuple[int, str]] = []
    sku_documents: dict[str, set[str]] = defaultdict(set)
    worksheet_ordinals: set[int] = set()
    for index, document in enumerate(documents):
        if not isinstance(document, Mapping):
            return False
        resolution = document.get("resolution")
        if not isinstance(resolution, Mapping):
            return False
        lines = _source_lines(document)
        doc_id = str(document.get("document_id") or f"doc:{index}")
        if str(resolution.get("document_type") or "") == "COMMERCIAL_INVOICE":
            if not any("LINE / SKU" in line.upper() for line in lines):
                return False
            invoice_rows.extend(
                (int(match.group("ordinal")), match.group("sku").upper())
                for line in lines if (match := _INVOICE_ROW.match(line))
            )
        for line in lines:
            if match := _MANUFACTURER_SKU.match(line):
                sku_documents[match.group("sku").upper()].add(doc_id)
            if match := re.fullmatch(r"\s*MERCHANDISE LINE\s*-\s*ARTICLE\s+([1-9][0-9]*)\s*", line, re.I):
                worksheet_ordinals.add(int(match.group(1)))

    # More than one invoice row, even repeating an SKU, is multiple merchandise
    # lines. Never collapse them just because their materials happen to match.
    if len(invoice_rows) != 1 or len(sku_documents) != 1:
        return False
    ordinal, sku = invoice_rows[0]
    if sku not in sku_documents or len(sku_documents[sku]) < 2:
        return False
    if worksheet_ordinals and worksheet_ordinals != {ordinal}:
        return False

    genus = _single_supported_value(fields.get("genus"))
    species = _single_supported_value(fields.get("species"))
    hts = _single_supported_value(fields.get("hts_code"))
    if not genus or not species or not hts:
        return False

    # Fail closed on any other contradictory plant/commercial attribute. Missing
    # evidence remains missing; review-required values are *not* force-promoted.
    for field_name in _COMPONENT_FIELDS | _LINE_FIELDS:
        field = fields.get(field_name)
        if not isinstance(field, Mapping):
            continue
        if field.get("state") == "CONFLICT":
            return False
        values = field.get("values") or []
        if len(values) > 1:
            return False

    line_key = f"sku:{sku.casefold()}"
    component_key = f"taxon:{genus.casefold()}:{species.casefold()}"
    changed = False
    for field_name in _COMPONENT_FIELDS | _LINE_FIELDS:
        field = fields.get(field_name)
        if _single_supported_value(field) is None:
            continue
        for row in field.get("supporting_evidence") or ():
            if not isinstance(row, dict):
                continue
            if field_name in _LINE_FIELDS:
                row["line_key"] = line_key
            else:
                row["line_key"] = line_key
                row["component_key"] = component_key
            changed = True
    return changed
