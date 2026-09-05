from __future__ import annotations
from .domain import DocumentType, ParsedLayout

_TITLES = {DocumentType.COMMERCIAL_INVOICE: ("commercial invoice",), DocumentType.ARRIVAL_NOTICE: ("arrival notice",), DocumentType.BILL_OF_LADING: ("ocean bill of lading", "bill of lading"), DocumentType.PACKING_LIST: ("packing list",), DocumentType.CUSTOMS_ENTRY_SUMMARY: ("cbp form 7501",)}
_REFERENCES = {DocumentType.BILL_OF_LADING: ("master bol", "master b/l", "house bol", "b/l no"), DocumentType.COMMERCIAL_INVOICE: ("invoice number",), DocumentType.ARRIVAL_NOTICE: ("estimated arrival date", " eta")}


def classify_text(text: str, role_hint: str | None = None) -> tuple[DocumentType, float]:
    text = text.casefold()
    scores = {kind: 0 for kind in DocumentType}
    for kind, signals in _TITLES.items(): scores[kind] += sum(100 for signal in signals if signal in text)
    for kind, signals in _REFERENCES.items(): scores[kind] += sum(5 for signal in signals if signal in text)
    try:
        scores[DocumentType(str(role_hint or "").strip().upper())] += 2
    except ValueError:
        pass
    ordered = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    winner, score = ordered[0]
    return (DocumentType.UNKNOWN, 0.0) if score < 20 else (winner, min(0.99, 0.55 + min(0.4, (score - ordered[1][1]) / 200)))


def classify(layout: ParsedLayout, role_hint: str | None = None) -> tuple[DocumentType, float]:
    return classify_text(" ".join(block.text for block in layout.blocks), role_hint)
