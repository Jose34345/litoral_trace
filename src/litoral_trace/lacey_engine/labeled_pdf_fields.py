"""Strict, source-anchored quantities from explicit PDF document labels.

Only produces candidate observations from a self-contained labeled TEXT_LINE.
Never infer that shipping gross weight, carton count or unit price represents
plant mass or entered line value. Association and human authority are downstream.
"""
from __future__ import annotations

from dataclasses import dataclass
import re

from litoral_trace.lacey_engine.domain import DocumentType, LayoutBlock, ParsedLayout

EXTRACTOR_VERSION = "lacey.labeled_pdf_field.v1"

# These labels are explicit vocabulary, not loose keyword proximity.
_ENTERED = re.compile(
    r"^Entered value\s+(?P<value>USD\s+(?:[0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)(?:\.[0-9]{1,2})?)\s*$",
    re.I,
)
# This is a *documented* entry number, not a shipment reference, invoice,
# purchase order or entry TYPE. It still requires authenticated review.
_ENTRY_REFERENCE = re.compile(
    r"^Entry (?:number|no\.)\s*[:#-]?\s*(?P<reference>[A-Z0-9]{3}-[A-Z0-9]{7}-[A-Z0-9])\s*$",
    re.I,
)
_DESCRIPTION = re.compile(
    r"^(?:Description of merchandise|Merchandise description)\s*[:#-]?\s*(?P<value>[A-Za-z][^\r\n]{5,190})$",
    re.I,
)
# Packing lists can explicitly label their goods, but not the shipping total.
_PACKING_GOODS = re.compile(
    r"^Goods\s+(?P<value>[A-Za-z][^\r\n]{5,190})$",
    re.I,
)
_PLANT = re.compile(
    r"^Quantity of plant material\s+(?P<amount>(?:[0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)(?:\.[0-9]+)?)"
    r"\s+(?P<unit>KG|G|M3|M2|L)\s*$",
    re.I,
)


@dataclass(frozen=True, slots=True)
class LiteralFieldSpan:
    field_name: str
    value: str
    block: LayoutBlock
    start: int
    end: int


def extract_explicit_field_spans(
    layout: ParsedLayout, *, document_type: DocumentType | None = None,
) -> tuple[LiteralFieldSpan, ...]:
    """Match complete label/value rows, preserving exact literal subspans.

    This intentionally abstains on OCR ambiguity, unmatched units, multiple
    numerical quantities, free text, unlabeled table totals and inferred lines.
    """
    results: list[LiteralFieldSpan] = []
    for block in layout.blocks:
        if block.block_type != "TEXT_LINE":
            continue
        raw = block.text
        entered = _ENTERED.fullmatch(raw.strip())
        quantity = _PLANT.fullmatch(raw.strip())
        entry_ref = _ENTRY_REFERENCE.fullmatch(raw.strip())
        description = _DESCRIPTION.fullmatch(raw.strip())
        if document_type is DocumentType.PACKING_LIST and not description:
            description = _PACKING_GOODS.fullmatch(raw.strip())
        if not any((entered, quantity, entry_ref, description)):
            continue
        trim_start = len(raw) - len(raw.lstrip())
        source = entered or quantity or entry_ref or description
        if entered:
            keys = (("entered_value", "value"),)
        elif quantity:
            keys = (("plant_quantity", "amount"), ("metric_unit", "unit"))
        elif entry_ref:
            keys = (("filing_entry_reference", "reference"),)
        else:
            keys = (("merchandise_description", "value"),)
        for field_name, group in keys:
            start, end = source.span(group)
            start += trim_start
            end += trim_start
            literal = raw[start:end]
            if not literal or literal != source.group(group):
                continue
            results.append(LiteralFieldSpan(field_name, literal, block, start, end))
    return tuple(results)
