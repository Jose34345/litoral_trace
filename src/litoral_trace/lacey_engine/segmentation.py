from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import re

from .classifier import classify_text
from .domain import DocumentSection, DocumentType, ParsedLayout


class DocumentDomain(StrEnum):
    COMMERCIAL_TRADE = "COMMERCIAL_TRADE"
    COURT_PLEADING = "COURT_PLEADING"
    LEGAL_DECISION = "LEGAL_DECISION"
    EMAIL_THREAD = "EMAIL_THREAD"
    UNSUPPORTED = "UNSUPPORTED"
    UNDETERMINED = "UNDETERMINED"


@dataclass(frozen=True, slots=True)
class DomainClassification:
    domain: DocumentDomain
    confidence: float
    matched_anchor: str | None = None

    @property
    def rejected(self) -> bool:
        # Domain rejection is opt-in and must be backed by an explicit negative
        # legal/email anchor. Unknown or unclassified commercial support documents
        # are allowed to continue through deterministic extraction.
        return self.domain in {
            DocumentDomain.COURT_PLEADING,
            DocumentDomain.LEGAL_DECISION,
            DocumentDomain.EMAIL_THREAD,
        }


SUPPORTED_DOCUMENT_TYPES = frozenset(
    kind
    for kind in DocumentType
    if kind not in {DocumentType.UNKNOWN, DocumentType.OTHER}
)

_LEGAL_DECISION_ANCHORS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("initial_decision", re.compile(r"\bINITIAL\s+DECISION\b", re.I)),
    (
        "administrative_law_judge",
        re.compile(r"\b(?:ADMINISTRATIVE\s+LAW\s+JUDGE|OFFICE\s+OF\s+ADMINISTRATIVE\s+LAW\s+JUDGES)\b", re.I),
    ),
)
_COURT_PLEADING_ANCHORS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "before_federal_maritime_commission",
        re.compile(r"\bBEFORE\s+THE\s+FEDERAL\s+MARITIME\s+COMMISSION\b", re.I),
    ),
    ("docket_number", re.compile(r"\bDOCKET\s+NO\.?\s*[A-Z0-9-]+", re.I)),
    (
        "adverse_parties",
        re.compile(
            r"\b(?:PLAINTIFFS?|COMPLAINANTS?)\b[\s\S]{0,240}\b(?:DEFENDANTS?|RESPONDENTS?)\b",
            re.I,
        ),
    ),
    (
        "plaintiff_defendant",
        re.compile(r"\bPLAINTIFFS?\s*/\s*DEFENDANTS?\b", re.I),
    ),
)
_FEDERAL_MARITIME_COMMISSION = re.compile(
    r"\bFEDERAL\s+MARITIME\s+COMMISSION\b",
    re.I,
)
_EMAIL_HEADER_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("from", re.compile(r"(?im)^\s*from\s*:\s*\S+")),
    ("sent", re.compile(r"(?im)^\s*sent\s*:\s*\S+")),
    ("to", re.compile(r"(?im)^\s*to\s*:\s*\S+")),
    ("subject", re.compile(r"(?im)^\s*subject\s*:\s*\S+")),
)
_ADDITIONAL_TRADE_ANCHORS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bARRIVAL\s+NOTICE\b", re.I),
    re.compile(r"\b(?:SEA|AIR)\s+WAYBILL\b", re.I),
    re.compile(r"\bCERTIFICATE\s+OF\s+ORIGIN\b", re.I),
    re.compile(r"\bPHYTOSANITARY\s+CERTIFICATE\b", re.I),
    re.compile(r"\bCBP\s+FORM\s+7501\b", re.I),
    re.compile(r"\bIMPORTER\s+SECURITY\s+FILING\b", re.I),
    re.compile(r"\bSUPPLIER\s+DECLARATION\b", re.I),
    re.compile(r"\bHARVEST\s+DECLARATION\b", re.I),
    re.compile(r"\bSPECIES\s+DECLARATION\b", re.I),
)


def _role_hint_document_type(role_hint: str | None) -> DocumentType | None:
    try:
        value = DocumentType(str(role_hint or "").strip().upper())
    except ValueError:
        return None
    return value if value in SUPPORTED_DOCUMENT_TYPES else None


def classify_domain_text(
    text: str,
    *,
    role_hint: str | None = None,
) -> DomainClassification:
    """Classify manifest document domain before extraction.

    Strong negative legal/admin anchors deliberately have precedence over
    commercial vocabulary appearing in quoted evidence. This prevents a court
    decision that discusses invoices, bills of lading or wood products from
    entering the trade-document extraction path.
    """

    source = str(text or "")
    folded = " ".join(source.split())
    if not folded:
        return DomainClassification(DocumentDomain.UNDETERMINED, 0.0)

    decision_hits = [
        name for name, pattern in _LEGAL_DECISION_ANCHORS if pattern.search(source)
    ]
    if (
        decision_hits
        and _FEDERAL_MARITIME_COMMISSION.search(source)
    ):
        return DomainClassification(
            DocumentDomain.LEGAL_DECISION,
            0.99,
            decision_hits[0],
        )

    pleading_hits = [
        name for name, pattern in _COURT_PLEADING_ANCHORS if pattern.search(source)
    ]
    if (
        "before_federal_maritime_commission" in pleading_hits
        or "plaintiff_defendant" in pleading_hits
        or (
            "docket_number" in pleading_hits
            and "adverse_parties" in pleading_hits
        )
    ):
        return DomainClassification(
            DocumentDomain.COURT_PLEADING,
            0.99,
            pleading_hits[0] if pleading_hits else "court_pleading",
        )

    email_hits = [
        name for name, pattern in _EMAIL_HEADER_PATTERNS if pattern.search(source)
    ]
    if len(email_hits) >= 3:
        return DomainClassification(
            DocumentDomain.EMAIL_THREAD,
            0.98,
            "+".join(email_hits),
        )

    if _role_hint_document_type(role_hint) is not None:
        return DomainClassification(
            DocumentDomain.COMMERCIAL_TRADE,
            0.95,
            "role_hint",
        )

    if _strong_anchor(source) is not None or any(
        pattern.search(source) for pattern in _ADDITIONAL_TRADE_ANCHORS
    ):
        return DomainClassification(
            DocumentDomain.COMMERCIAL_TRADE,
            0.95,
            "trade_anchor",
        )

    return DomainClassification(DocumentDomain.UNDETERMINED, 0.25)


def classify_pdf_first_page_domain(
    content: bytes,
    *,
    role_hint: str | None = None,
) -> DomainClassification:
    """Inspect only page one with PDFium for a cheap pre-extraction domain gate."""

    if not bytes(content or b"").startswith(b"%PDF-"):
        return DomainClassification(DocumentDomain.UNDETERMINED, 0.0)

    try:
        import pypdfium2 as pdfium
    except ImportError:
        return DomainClassification(DocumentDomain.UNDETERMINED, 0.0)

    document = None
    page = None
    text_page = None
    try:
        document = pdfium.PdfDocument(content)
        if len(document) < 1:
            # Empty/corrupt inputs are parser failures, not domain-policy failures.
            return DomainClassification(DocumentDomain.UNDETERMINED, 0.0, "empty_pdf")
        page = document[0]
        text_page = page.get_textpage()
        text = str(text_page.get_text_range() or "")
        return classify_domain_text(text, role_hint=role_hint)
    except Exception:
        # Domain classification must not turn an otherwise parseable document into
        # a false rejection. The normal parser remains authoritative for corruption.
        return DomainClassification(DocumentDomain.UNDETERMINED, 0.0)
    finally:
        for value in (text_page, page, document):
            close = getattr(value, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass


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
        re.compile(r"\b(?:SEA|AIR)\s+WAYBILL\b", re.I),
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
    (
        re.compile(r"\bENTRY\s+WORKSHEET\b", re.I),
        DocumentType.CUSTOMS_ENTRY_SUMMARY,
    ),
    (
        re.compile(r"\bBOTANICAL\s+DECLARATION\b", re.I),
        DocumentType.SPECIES_DECLARATION,
    ),
    (
        re.compile(r"\bARRIVAL\s+NOTICE\b", re.I),
        DocumentType.ARRIVAL_NOTICE,
    ),
    (
        re.compile(r"\bPLANT\s+DATA\s+WORKSHEET\b", re.I),
        DocumentType.SPECIES_DECLARATION,
    ),
    (
        re.compile(r"\bSUPPLIER\s+DECLARATION\b", re.I),
        DocumentType.SUPPLIER_DECLARATION,
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
    domain: DocumentDomain = DocumentDomain.UNDETERMINED


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
    domain = classify_domain_text(text)
    document_type, confidence = classify_text(text)
    anchor = _strong_anchor(text)
    if domain.rejected:
        document_type = DocumentType.UNKNOWN
        confidence = max(confidence, domain.confidence)
        anchor = None
    elif anchor is not None:
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
        domain=domain,
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

    A strong document title is a boundary signal, even when adjacent shipment
    documents repeat the same B/L, invoice or container identifier. The narrow
    exception is a repeated anchor for the current logical document type that
    also shares a deterministic fingerprint, which preserves multipage invoices
    and B/Ls even when an intermediate page omits the repeated title.
    """

    if previous is None:
        return True

    if current.strong_anchor is not None:
        repeated_same_document_anchor = (
            previous.document_type == current.strong_anchor
            and _shares_document_fingerprint(previous, current)
        )
        if not repeated_same_document_anchor:
            return True

    if _shares_document_fingerprint(previous, current):
        return False

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
