from __future__ import annotations
from .domain import DocumentType, ParsedLayout

_SIGNALS = ((DocumentType.BILL_OF_LADING, ("master bol", "master b/l", "bill of lading", "house bol")),
            (DocumentType.ARRIVAL_NOTICE, ("estimated arrival date", "estimated date of arrival", " eta")),
            (DocumentType.COMMERCIAL_INVOICE, ("commercial invoice", "invoice number")),
            (DocumentType.PACKING_LIST, ("packing list",)),)


def classify(layout: ParsedLayout, role_hint: str | None = None) -> tuple[DocumentType, float]:
    text = " ".join(block.text.casefold() for block in layout.blocks)
    for document_type, signals in _SIGNALS:
        if any(signal in text for signal in signals):
            return document_type, 0.90
    hint = str(role_hint or "").strip().upper()
    try:
        return DocumentType(hint), 0.40
    except ValueError:
        return DocumentType.UNKNOWN, 0.0
