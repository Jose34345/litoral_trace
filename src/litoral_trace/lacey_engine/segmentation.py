from __future__ import annotations

from dataclasses import dataclass
import re

from .classifier import classify_text
from .domain import DocumentSection, DocumentType, ParsedLayout


_STRONG_ANCHORS: tuple[tuple[re.Pattern[str], DocumentType], ...] = (
    (
        re.compile(r"\bOCEAN\s+BILL\s+OF\s+LADING\b", re.I),
        DocumentType.BILL_OF_LADING,
    ),
    (
        re.compile(r"\bBILL\s+OF\s+LADING\b", re.I),
        DocumentType.BILL_OF_LADING,
    ),
    (
        re.compile(r"\bCOMMERCIAL\s+INVOICE\b", re.I),
        DocumentType.COMMERCIAL_INVOICE,
    ),
    (
        re.compile(r"\bPACKING\s+LIST\b", re.I),
        DocumentType.PACKING_LIST,
    ),
)

_INVOICE_NUMBER = re.compile(
    r"\b(?:commercial\s+)?invoice\s*(?:number|no\.?|#)\s*[:#-]?\s*"
    r"([A-Z0-9][A-Z0-9/-]{3,})\b",
    re.I,
)
_BILL_NUMBER = re.compile(
    r"\b(?:master\s+|house\s+)?(?:b/?l|bol|bill\s+of\s+lading)"
    r"\s*(?:number|no\.?|#)\s*[:#-]?\s*([A-Z0-9][A-Z0-9/-]{3,})\b",
    re.I,
)
_CONTAINER_NUMBER = re.compile(r"\b([A-Z]{4}\d{7})\b")
_PAGINATION = re.compile(
    r"\b(?:p[a-z]{0,4}ge\s*)?(\d{1,4})\s*(?:of|/)\s*(\d{1,4})\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class PageClassification:
    page: int
    document_type: DocumentType
    confidence: float
    block_ids: tuple[str, ...]
    strong_anchor: DocumentType | None = None
    invoice_numbers: frozenset[str] = frozenset()
    bill_numbers: frozenset[str] = frozenset()
    containers: frozenset[str] = frozenset()
    page_number: int | None = None
    page_total: int | None = None


def _normalized_tokens(pattern: re.Pattern[str], text: str) -> frozenset[str]:
    return frozenset(
        match.group(1).strip().upper()
        for match in pattern.finditer(text)
        if match.group(1).strip()
    )


def _strong_anchor(text: str) -> DocumentType | None:
    for pattern, document_type in _STRONG_ANCHORS:
        if pattern.search(text):
            return document_type
    return None


def _pagination(text: str) -> tuple[int | None, int | None]:
    matches = list(_PAGINATION.finditer(text))
    if not matches:
        return None, None
    match = matches[-1]
    page_number = int(match.group(1))
    page_total = int(match.group(2))
    if page_number < 1 or page_total < page_number:
        return None, None
    return page_number, page_total


def _page_classification(
    *,
    page: int,
    text: str,
    block_ids: tuple[str, ...],
    fallback_type: DocumentType,
) -> PageClassification:
    document_type, confidence = classify_text(text)
    anchor = _strong_anchor(text)
    if anchor is not None:
        document_type = anchor
        confidence = max(confidence, 0.95)
    elif document_type is DocumentType.UNKNOWN:
        document_type = fallback_type
        confidence = 0.5

    page_number, page_total = _pagination(text)
    return PageClassification(
        page=page,
        document_type=document_type,
        confidence=confidence,
        block_ids=block_ids,
        strong_anchor=anchor,
        invoice_numbers=_normalized_tokens(_INVOICE_NUMBER, text),
        bill_numbers=_normalized_tokens(_BILL_NUMBER, text),
        containers=_normalized_tokens(_CONTAINER_NUMBER, text),
        page_number=page_number,
        page_total=page_total,
    )


def _shares_document_fingerprint(
    previous: PageClassification,
    current: PageClassification,
) -> bool:
    if previous.invoice_numbers & current.invoice_numbers:
        return True
    if previous.bill_numbers & current.bill_numbers:
        return True
    if previous.containers & current.containers:
        return True

    if (
        previous.page_number is not None
        and previous.page_total is not None
        and current.page_number is not None
        and current.page_total is not None
        and previous.page_total == current.page_total
        and current.page_number == previous.page_number + 1
    ):
        return True

    return False


def starts_new_document(
    previous: PageClassification | None,
    current: PageClassification,
) -> bool:
    """Return True only when deterministic evidence supports a document boundary.

    Continuity fingerprints intentionally have precedence over repeated titles.
    This keeps repeated headers on multipage invoices/B/Ls inside one logical
    document while still allowing strong titles to start a genuinely new source.
    """

    if previous is None:
        return True

    if _shares_document_fingerprint(previous, current):
        return False

    if current.strong_anchor is not None:
        return True

    if (
        current.document_type is not previous.document_type
        and current.confidence >= 0.80
    ):
        return True

    return False


def segment(
    layout: ParsedLayout,
    document_type: DocumentType,
) -> tuple[DocumentSection, ...]:
    sections: list[DocumentSection] = []
    previous_page: PageClassification | None = None
    current_type = document_type

    for page in range(1, layout.page_count + 1):
        blocks = tuple(
            block
            for block in layout.blocks
            if block.page == page
        )
        text = " ".join(block.text for block in blocks)
        ids = tuple(block.block_id for block in blocks)

        current = _page_classification(
            page=page,
            text=text,
            block_ids=ids,
            fallback_type=current_type,
        )

        if starts_new_document(previous_page, current):
            sections.append(
                DocumentSection(
                    section_id=f"section-{len(sections) + 1}",
                    page_start=page,
                    page_end=page,
                    document_type=current.document_type,
                    confidence=current.confidence,
                    block_ids=ids,
                )
            )
        else:
            if not sections:
                sections.append(
                    DocumentSection(
                        section_id="section-1",
                        page_start=page,
                        page_end=page,
                        document_type=current.document_type,
                        confidence=current.confidence,
                        block_ids=ids,
                    )
                )
            else:
                old = sections[-1]
                sections[-1] = DocumentSection(
                    section_id=old.section_id,
                    page_start=old.page_start,
                    page_end=page,
                    document_type=old.document_type,
                    confidence=max(old.confidence, current.confidence),
                    block_ids=old.block_ids + ids,
                )

        current_type = sections[-1].document_type
        previous_page = current

    return tuple(sections)
