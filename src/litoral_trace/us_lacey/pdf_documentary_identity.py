"""Fail-closed identity support for colonless PDF-only single-product shipments.

This is deliberately NOT a Bill of Materials extractor. It identifies a single
supplier/SKU and connects one *existing* shipment line to that product only when
independent uploaded documents explicitly corroborate the identity. Original
text never enters the product-intelligence snapshot or evidence catalog.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable

_SKU = r"[A-Z][A-Z0-9]*[-_][A-Z0-9][A-Z0-9._-]{2,40}"
_INVOICE_LINE = re.compile(rf"^\s*([1-9]\d*)\s*/\s*({_SKU})(?=\s|$)", re.I)
_MANUFACTURER_SKU = re.compile(rf"^\s*Manufacturer\s+SKU\s+({_SKU})(?=\s|$)", re.I)
_NAME = re.compile(r"^\s*(?:Supplier\s*/\s*manufacturer|Supplier\s+legal\s+name)\s+(.+?)\s*$", re.I)
_ADDRESS = re.compile(r"^\s*(?:Supplier\s+address|Manufacturer\s+address|Manufacturing\s+site)\s+(.+?)\s*$", re.I)
_DESCR = re.compile(rf"^\s*Product\s*/\s*SKU\s+(.+?)\s*/\s*({_SKU})\s*$", re.I)


def _norm(value: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", value.casefold()))


def _clean_address(value: str) -> str:
    # Layout wrapping can split an explicit "(fictional address)" annotation.
    return re.sub(r"\s*\(fictional\s*(?:address)?\s*$", "", value, flags=re.I).strip()


@dataclass(frozen=True, slots=True)
class DocumentaryField:
    assurance_document_id: int
    field_name: str
    normalized_value: str
    source_locator: str = ""


@dataclass(frozen=True, slots=True)
class PdfDocumentaryIdentity:
    line_reference: str
    sku: str
    product_name: str | None
    virtual_fields: tuple[DocumentaryField, ...]
    supplier_document_count: int
    corroborating_sku_document_count: int


def discover_single_product_pdf_identity(
    fields: Iterable[object], *, line_references: Iterable[str],
) -> PdfDocumentaryIdentity | None:
    """Produce identity metadata, never botanical facts or synthetic evidence."""
    lines = tuple(str(item).strip() for item in line_references)
    if len(lines) != 1 or not lines[0]:
        return None
    raw_by_doc: dict[int, str] = {}
    for field in fields:
        if str(getattr(field, "field_name", "")) != "raw.document_text":
            continue
        doc_id = int(field.assurance_document_id)
        if doc_id in raw_by_doc:
            return None
        text = str(getattr(field, "original_value", "") or getattr(field, "normalized_value", "") or "")
        if text:
            raw_by_doc[doc_id] = text
    if len(raw_by_doc) < 3:
        return None

    invoice_rows: list[tuple[int, str, int]] = []
    supplier_doc_ids: set[int] = set()
    sku_doc_ids: dict[str, set[int]] = {}
    recognized_pairs: set[tuple[str, str]] = set()
    result_fields: list[DocumentaryField] = []
    descriptions: set[str] = set()
    for doc_id, text in raw_by_doc.items():
        text_lines = [line.strip() for line in text.splitlines()]
        doc_name: set[str] = set()
        doc_address: set[str] = set()
        if any(line.upper() == "COMMERCIAL INVOICE" for line in text_lines) and any(
            "LINE / SKU" in line.upper() for line in text_lines
        ):
            for line in text_lines:
                match = _INVOICE_LINE.match(line)
                if match:
                    invoice_rows.append((int(match[1]), match[2].upper(), doc_id))
        for line in text_lines:
            if match := _MANUFACTURER_SKU.match(line):
                sku_doc_ids.setdefault(match[1].upper(), set()).add(doc_id)
            if match := _NAME.match(line):
                doc_name.add(match[1].strip())
            if match := _ADDRESS.match(line):
                doc_address.add(_clean_address(match[1]))
            if match := _DESCR.match(line):
                descriptions.add(match[1].strip())
        if len(doc_name) > 1 or len(doc_address) > 1:
            return None
        if doc_name and doc_address:
            name, address = next(iter(doc_name)), next(iter(doc_address))
            if name and address:
                recognized_pairs.add((_norm(name), _norm(address)))
                supplier_doc_ids.add(doc_id)
                result_fields.extend((
                    DocumentaryField(doc_id, "supplier_name", name),
                    DocumentaryField(doc_id, "supplier_address", address),
                ))

    # Strictly no ambiguity: one invoice item, one supplier/site identity,
    # at least two source documents with that supplier identity, and at least
    # two OTHER documents identifying exactly the invoice SKU as manufacturer's.
    if len(invoice_rows) != 1 or len(recognized_pairs) != 1:
        return None
    ordinal, sku, invoice_doc = invoice_rows[0]
    if str(ordinal) != lines[0] or len(supplier_doc_ids) < 2:
        return None
    if len(sku_doc_ids) != 1 or len(sku_doc_ids.get(sku, set()) - {invoice_doc}) < 2:
        return None
    product_name = next(iter(descriptions)) if len(descriptions) == 1 else None
    result_fields.extend((
        DocumentaryField(invoice_doc, "line", str(ordinal), f"data_row:{ordinal}"),
        DocumentaryField(invoice_doc, "sku", sku, f"data_row:{ordinal}"),
    ))
    return PdfDocumentaryIdentity(
        line_reference=lines[0], sku=sku, product_name=product_name,
        virtual_fields=tuple(result_fields),
        supplier_document_count=len(supplier_doc_ids),
        corroborating_sku_document_count=len(sku_doc_ids[sku] - {invoice_doc}),
    )
