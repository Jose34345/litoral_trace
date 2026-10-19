"""Original invoice source value is a candidate, never filing approval."""
from dataclasses import replace
from pathlib import Path

import pytest

from litoral_trace.lacey_engine.commercial_invoice_line_value import (
    extract_invoice_line_extended_value,
)
from litoral_trace.lacey_engine.domain import DocumentType
from litoral_trace.lacey_engine.layout_parser import parse_layout
from litoral_trace.us_lacey.ppq505 import validate_ppq_value

ROOT = Path(__file__).resolve().parents[2]
INVOICE = ROOT / "tests/fixtures/assurance_v2_pdf_e2e/pack2/01_Commercial_Invoice_Shipment2.pdf"


def _layout():
    return parse_layout(INVOICE.name, INVOICE.read_bytes())


def _replace(text, new):
    layout = _layout()
    return replace(layout, blocks=tuple(
        replace(b, text=b.text.replace(text, new)) if text in b.text else b
        for b in layout.blocks
    ))


def _extract(layout):
    return extract_invoice_line_extended_value(layout, document_type=DocumentType.COMMERCIAL_INVOICE)


def test_pack2_exact_original_invoice_line_value_with_literal_anchor():
    source = _extract(_layout())
    assert source is not None
    assert source.line_reference == "1"
    assert source.sku == "BAM-COAST-04"
    assert source.amount == "USD 11,250.00"
    assert source.source_block.page == 1
    assert source.source_block.block_id == "p1-l23"
    assert source.amount in source.source_block.text
    assert validate_ppq_value("entered_value", source.amount).normalized_value == "11250"


@pytest.mark.parametrize(("old", "new"), [
    ("USD 12.50 USD 11,250.00", "USD 12.50 USD 12,250.00"),
    ("USD 12.50 USD 11,250.00", "USD 13.50 USD 11,250.00"),
    ("Total merchandise value USD 11,250.00", "Total merchandise value USD 12,250.00"),
    ("Product / SKU Bamboo coaster set (4 pieces) / BAM-COAST-04",
     "Product / SKU Bamboo coaster set (4 pieces) / OTHER-SKU-01"),
    ("LINE / SKU DESCRIPTION / HTS QTY UNIT VALUE EXTENDED VALUE",
     "NOT A COMMERCIAL INVOICE LINE"),
])
def test_invoice_refuses_wrong_arithmetic_identity_or_missing_line(old, new):
    assert _extract(_replace(old, new)) is None


def test_invoice_rejects_multiple_lines_even_when_total_happens_to_match():
    layout = _layout()
    row = next(b for b in layout.blocks if b.block_type == "TEXT_LINE"
               and b.text.startswith("1 / BAM-COAST-04"))
    blocks = list(layout.blocks)
    blocks.insert(blocks.index(row) + 1, replace(
        row, block_id="p1-extra", text=row.text.replace("1 /", "2 /"),
    ))
    layout = replace(layout, blocks=tuple(blocks))
    assert _extract(layout) is None


def test_invoice_rejects_duplicate_merchandise_total_and_wrong_document_type():
    layout = _layout()
    total = next(b for b in layout.blocks if b.text.startswith("Total merchandise value"))
    assert _extract(replace(layout, blocks=tuple(layout.blocks) + (
        replace(total, block_id="extra-total"),
    ))) is None
    assert extract_invoice_line_extended_value(
        layout, document_type=DocumentType.PACKING_LIST,
    ) is None
