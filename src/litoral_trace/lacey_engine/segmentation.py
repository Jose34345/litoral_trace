from __future__ import annotations

from dataclasses import dataclass
import re

from .classifier import classify_text
from .domain import DocumentSection, DocumentType, ParsedLayout


_STRONG_TITLE_PATTERNS: tuple[tuple[DocumentType, re.Pattern[str]], ...] = (
    (
        DocumentType.COMMERCIAL_INVOICE,
        re.compile(r"\bCOMMERCIAL\s+INVOICE\b", re.I),
    ),
    (
        DocumentType.PACKING_LIST,
        re.compile(r"\bPACKING\s+LIST\b", re.I),
    ),
    (
        DocumentType.BILL_OF_LADING,
        re.compile(r"\b(?:OCEAN\s+)?BILL\s+OF\s+LADING\b", re.I),
    ),
)

_INVOICE_NUMBER = re.compile(
    r"\bINVOICE\s*(?:NUMBER|NO\.?|#)\s*[:#-]?\s*([A-Z0-9][A-Z0-9./-]{2,})",
    re.I,
)
_BILL_OF_LADING_NUMBER = re.compile(
    r"\b(?:MASTER\s+|HOUSE\s+)?(?:BILL\s+OF\s+LADING|B/?L|BOL)"
    r"\s*(?:NUMBER|NO\.?)?\s*[:#-]?\s*([A-Z0-9][A-Z0-9./-]{4,})",
    re.I,
)
_CONTAINER_NUMBER = re.compile(r"\b([A-Z]{4}\d{7})\b", re.I)
_PAGE_NUMBER = re.compile(
    r"\bPAGE\s+(\d{1,4})\s+(?:OF|/)\s+(\d{1,4})\b",
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
    bill_of_lading_numbers: frozenset[str] = frozenset()
    container_numbers: frozenset[str] = frozenset()
    pagination: tuple[int, int] | None = None

    def __post_init__(self) -> None:
        if self.page < 1:
            raise ValueError("PageClassification.page must be >= 1.")
        if not 0.0 <= float(self.confidence) <= 1.0:
            raise ValueError(
                "PageClassification.confidence must be between 0 and 1."
            )


def _normalized_identifiers(
    pattern: re.Pattern[str],
    text: str,
) -> frozenset[str]:
    return frozenset(
        match.group(1).strip().upper()
        for match in pattern.finditer(text)
        if match.group(1).strip()
    )


def _strong_anchor(text: str) -> DocumentType | None:
    for document_type, pattern in _STRONG_TITLE_PATTERNS:
        if pattern.search(text):
            return document_type
    return None


def _pagination(text: str) -> tuple[int, int] | None:
    match = _PAGE_NUMBER.search(text)
    if match is None:
        return None
    page_number = int(match.group(1))
    page_total = int(match.group(2))
    if page_number < 1 or page_total < page_number:
        return None
    return page_number, page_total


def _classify_page(
    *,
    page: int,
    text: str,
    block_ids: tuple[str, ...],
    fallback_type: DocumentType,
) -> PageClassification:
    kind, confidence = classify_text(text)
    anchor = _strong_anchor(text)
    if anchor is not None:
        kind = anchor
        confidence = max(confidence, 0.95)
    elif kind is DocumentType.UNKNOWN:
        kind = fallback_type
        confidence = 0.5

    return PageClassification(
        page=page,
        document_type=kind,
        confidence=confidence,
        block_ids=block_ids,
        strong_anchor=anchor,
        invoice_numbers=_normalized_identifiers(_INVOICE_NUMBER, text),
        bill_of_lading_numbers=_normalized_identifiers(
            _BILL_OF_LADING_NUMBER,
            text,
        ),
        container_numbers=_normalized_identifiers(_CONTAINER_NUMBER, text),
        pagination=_pagination(text),
    )


def _shares_fingerprint(
    previous: PageClassification,
    current: PageClassification,
) -> bool:
    if previous.invoice_numbers & current.invoice_numbers:
        return True
    if previous.bill_of_lading_numbers & current.bill_of_lading_numbers:
        return True
    if previous.container_numbers & current.container_numbers:
        return True

    if previous.pagination is not None and current.pagination is not None:
        previous_page, previous_total = previous.pagination
        current_page, current_total = current.pagination
        if (
            previous_total == current_total
            and current_page == previous_page + 1
        ):
            return True

    return False


def starts_new_document(
    previous: PageClassification | None,
    current: PageClassification,
) -> bool:
    """Return whether *current* begins a new logical document.

    Proven continuity has precedence over repeated titles. Strong document-title
    anchors otherwise create a boundary. A high-confidence semantic type change
    also creates a boundary when no continuity fingerprint connects the pages.
    """

    if previous is None:
        return True

    if _shares_fingerprint(previous, current):
        return False

    if current.strong_anchor is not None:
        return True

    return (
        current.document_type is not previous.document_type
        and current.confidence >= 0.80
    )


def segment(
    layout: ParsedLayout,
    document_type: DocumentType,
) -> tuple[DocumentSection, ...]:
    sections: list[DocumentSection] = []
    previous_page: PageClassification | None = None
    active_type = document_type

    for page in range(1, layout.page_count + 1):
        blocks = tuple(
            block
            for block in layout.blocks
            if block.page == page
        )
        text = " ".join(block.text for block in blocks)
        current = _classify_page(
            page=page,
            text=text,
            block_ids=tuple(block.block_id for block in blocks),
            fallback_type=active_type,
        )

        if starts_new_document(previous_page, current):
            active_type = current.document_type
            sections.append(
                DocumentSection(
                    section_id=f"section-{len(sections) + 1}",
                    page_start=page,
                    page_end=page,
                    document_type=active_type,
                    confidence=current.confidence,
                    block_ids=current.block_ids,
                )
            )
        else:
            if not sections:
                active_type = current.document_type
                sections.append(
                    DocumentSection(
                        section_id="section-1",
                        page_start=page,
                        page_end=page,
                        document_type=active_type,
                        confidence=current.confidence,
                        block_ids=current.block_ids,
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
                    block_ids=old.block_ids + current.block_ids,
                )
                active_type = old.document_type

        previous_page = current

    return tuple(sections)
