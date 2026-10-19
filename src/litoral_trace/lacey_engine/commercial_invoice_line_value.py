"""Exact, line-local commercial-invoice extended-value observation.

Only an original single-SKU invoice with explicit line, quantity, unit price,
extended amount and matching merchandise total qualifies. This produces no
approval or allocation for multi-line invoices; remaining cases need review.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import re

from litoral_trace.lacey_engine.domain import DocumentType, LayoutBlock, ParsedLayout

_MONEY = r"(?:[0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)(?:\.[0-9]{1,2})?"
_HEADER = re.compile(
    r"^LINE\s*/\s*SKU\s+DESCRIPTION\s*/\s*HTS\s+QTY\s+UNIT VALUE\s+EXTENDED VALUE$", re.I,
)
_ROW = re.compile(
    rf"^(?P<line>[1-9][0-9]{{0,4}})\s*/\s*(?P<sku>[A-Z0-9]+(?:-[A-Z0-9]+)+)"
    rf"\s+(?P<description>.+?)\s+(?P<qty>[0-9]+)\s+(?P<unit>SET|SETS|PCS|PIECES)"
    rf"\s+USD\s+(?P<price>{_MONEY})\s+(?P<amount>USD\s+{_MONEY})$", re.I,
)
_TOTAL = re.compile(rf"^Total merchandise value\s+(?P<amount>USD\s+{_MONEY})$", re.I)
_PRODUCT_SKU = re.compile(r"^Product\s*/\s*SKU\s+.+?\s*/\s*(?P<sku>[A-Z0-9]+(?:-[A-Z0-9]+)+)$", re.I)
_ROW_PREFIX = re.compile(r"^[1-9][0-9]{0,4}\s*/\s*[A-Z0-9]+(?:-[A-Z0-9]+)+\b", re.I)

@dataclass(frozen=True, slots=True)
class CommercialInvoiceLineValue:
    line_reference: str
    sku: str
    amount: str
    source_block: LayoutBlock


def _decimal(raw: str) -> Decimal:
    return Decimal(str(raw).replace("USD", "").replace(",", "").strip())


def extract_invoice_line_extended_value(
    layout: ParsedLayout, *, document_type: DocumentType,
) -> CommercialInvoiceLineValue | None:
    """Abstain on more than one line, missing labels or arithmetic ambiguity."""
    if document_type is not DocumentType.COMMERCIAL_INVOICE:
        return None
    lines = [block for block in layout.blocks if block.block_type == "TEXT_LINE"]
    headers = [i for i, b in enumerate(lines) if _HEADER.fullmatch(b.text.strip())]
    totals = [(i, _TOTAL.fullmatch(b.text.strip())) for i, b in enumerate(lines)]
    totals = [(i, m) for i, m in totals if m is not None]
    if len(headers) != 1 or len(totals) != 1:
        return None
    start, end = headers[0], totals[0][0]
    if start >= end or end - start > 9:
        return None
    rows = [b for b in lines[start + 1:end] if _ROW.fullmatch(b.text.strip())]
    # An unmatched second row is ambiguity too. Refuse rather than allocate
    # a shipment total to the first product of a multi-SKU invoice.
    starters = [b for b in lines[start + 1:end] if _ROW_PREFIX.match(b.text.strip())]
    if len(rows) != 1 or len(starters) != 1:
        return None
    block = rows[0]
    match = _ROW.fullmatch(block.text.strip())
    assert match is not None
    explicit_skus = [_PRODUCT_SKU.fullmatch(b.text.strip()) for b in lines[:start]]
    explicit_skus = [m.group("sku").upper() for m in explicit_skus if m is not None]
    if len(explicit_skus) != 1 or explicit_skus[0] != match.group("sku").upper():
        return None
    try:
        quantity = _decimal(match.group("qty"))
        unit_price = _decimal(match.group("price"))
        extended = _decimal(match.group("amount"))
        total = _decimal(totals[0][1].group("amount"))
    except InvalidOperation:
        return None
    if quantity <= 0 or unit_price <= 0 or extended <= 0 or quantity * unit_price != extended or extended != total:
        return None
    return CommercialInvoiceLineValue(
        line_reference=match.group("line"),
        sku=match.group("sku").upper(),
        amount=match.group("amount"),
        source_block=block,
    )
