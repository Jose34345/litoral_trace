"""Read-only presentation descriptors for applicability evidence cards."""
from __future__ import annotations

from dataclasses import dataclass

from litoral_trace.us_lacey.regulatory.applicability.attestation import ApplicabilitySource


@dataclass(frozen=True, slots=True)
class EvidenceCardDescriptor:
    source_id: int
    filename: str
    classification: str
    availability: str
    review_scope: str
    page: int
    is_selectable: bool
    is_supplier_evidence: bool
    source_set_fingerprint: str


def evidence_cards(sources: tuple[ApplicabilitySource, ...]) -> tuple[EvidenceCardDescriptor, ...]:
    """Project source-reader facts without making source class an authority grant."""
    cards: list[EvidenceCardDescriptor] = []
    for source in sources:
        supplier = source.classification == "Supplier evidence"
        derived = source.classification == "Derived worksheet"
        selectable = bool(source.is_primary_candidate and source.page_available and not derived)
        if derived:
            availability = "Derived worksheet"
            scope = "Supporting context only"
        elif not source.page_available:
            availability = "Original unavailable"
            scope = "Supplier cultivation evidence" if supplier else "Customs entry evidence"
        else:
            availability = "Evidence available"
            scope = "Supplier cultivation evidence" if supplier else "Customs entry evidence"
        cards.append(EvidenceCardDescriptor(
            source_id=source.id,
            filename=source.filename,
            classification=source.classification,
            availability=availability,
            review_scope=scope,
            page=1,
            is_selectable=selectable,
            is_supplier_evidence=supplier,
            source_set_fingerprint=source.source_set_fingerprint,
        ))
    return tuple(cards)
