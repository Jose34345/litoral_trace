"""Strict, source-anchored quantities from explicit PDF document labels.

Only produces candidate observations from a self-contained labeled TEXT_LINE.
Never infer that shipping gross weight, carton count or unit price represents
plant mass or entered line value. Association and human authority are downstream.
"""
from __future__ import annotations

from dataclasses import dataclass
import re

from litoral_trace.lacey_engine.domain import LayoutBlock, ParsedLayout

EXTRACTOR_VERSION = "lacey.labeled_pdf_field.v1"

# These labels are explicit vocabulary, not loose keyword proximity.
_ENTERED = re.compile(
    r"^Entered value\s+(?P<value>USD\s+(?:[0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)(?:\.[0-9]{1,2})?)\s*$",
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


def extract_explicit_field_spans(layout: ParsedLayout) -> tuple[LiteralFieldSpan, ...]:
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
        if not entered and not quantity:
            continue
        trim_start = len(raw) - len(raw.lstrip())
        source = entered if entered else quantity
        keys = (("entered_value", "value"),) if entered else (
            ("plant_quantity", "amount"), ("metric_unit", "unit"),
        )
        for field_name, group in keys:
            start, end = source.span(group)
            start += trim_start
            end += trim_start
            literal = raw[start:end]
            if not literal or literal != source.group(group):
                continue
            results.append(LiteralFieldSpan(field_name, literal, block, start, end))
    return tuple(results)
