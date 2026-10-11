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

from litoral_trace.us_lacey.single_sku_recovery import _explicit_document_skus

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


_NAME_LABELS = frozenset({
    "SUPPLIER / MANUFACTURER", "SUPPLIER LEGAL NAME", "MANUFACTURER",
})
_ADDRESS_LABELS = frozenset({
    "SUPPLIER ADDRESS", "MANUFACTURER ADDRESS", "MANUFACTURING SITE",
})


def _labeled_supplier_identity(lines: list[str]) -> tuple[set[str], set[str], set[str]]:
    """Return source-literal supplier/name/address and product descriptions.

    Labels may be on the same layout line or immediately precede the value.
    This is bounded, document-local evidence reading, not fuzzy entity merging.
    """
    names: set[str] = set()
    addresses: set[str] = set()
    descriptions: set[str] = set()
    for index, original_line in enumerate(lines):
        line = str(original_line).strip()
        next_line = str(lines[index + 1]).strip() if index + 1 < len(lines) else ""
        name = _NAME.match(line)
        address = _ADDRESS.match(line)
        product = _DESCR.match(line)
        if name:
            names.add(name[1].strip())
        if address:
            addresses.add(_clean_address(address[1]))
        if product:
            descriptions.add(product[1].strip())
        if line.upper() in _NAME_LABELS and next_line:
            # A document may contain a table header sequence MANUFACTURER,
            # SKU, PRODUCT. Never interpret the next column header as a
            # supplier name or allow it to contradict the real legal name.
            if line.upper() != "MANUFACTURER" or next_line.upper() not in {
                "SKU", "PRODUCT", "DESCRIPTION", "QUANTITY", "QTY", "UNIT",
            }:
                names.add(next_line)
        elif line.upper() in _ADDRESS_LABELS and next_line:
            addresses.add(_clean_address(next_line))
        elif line.upper() == "PRODUCT / SKU" and next_line:
            tail = re.match(rf"^(.+?)\s*/\s*({_SKU})\s*$", next_line, re.I)
            if tail is not None:
                descriptions.add(tail[1].strip())
    return names, addresses, descriptions


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
    observed_names: set[str] = set()
    observed_addresses: set[str] = set()
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
        source_skus = _explicit_document_skus(tuple(text_lines))
        if len(source_skus) > 1:
            return None
        for sku in source_skus:
            sku_doc_ids.setdefault(sku, set()).add(doc_id)
        doc_name, doc_address, doc_descriptions = _labeled_supplier_identity(text_lines)
        descriptions.update(doc_descriptions)
        if len(doc_name) > 1 or len(doc_address) > 1:
            return None
        observed_names.update(_norm(name) for name in doc_name if name)
        observed_addresses.update(_norm(address) for address in doc_address if address)
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
    if (len(invoice_rows) != 1 or len(recognized_pairs) != 1
            or len(observed_names) != 1 or len(observed_addresses) != 1):
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
