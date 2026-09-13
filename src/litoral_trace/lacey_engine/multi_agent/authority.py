"""Deterministic source authority for multi-extractor candidate fusion."""
from __future__ import annotations

import hashlib

from ..ai_shadow import comparison_key
from .contracts import CandidateEnvelope, DocumentType, SpecialistRole


FIELD_SPECIALIST: dict[str, SpecialistRole] = {
    "importer_name": SpecialistRole.CUSTOMS_IDENTITY,
    "importer_address": SpecialistRole.CUSTOMS_IDENTITY,
    "consignee_name": SpecialistRole.CUSTOMS_IDENTITY,
    "consignee_address": SpecialistRole.CUSTOMS_IDENTITY,
    "filing_entry_reference": SpecialistRole.CUSTOMS_IDENTITY,
    "manufacturer_id": SpecialistRole.CUSTOMS_IDENTITY,
    "bill_of_lading": SpecialistRole.LOGISTICS,
    "container_number": SpecialistRole.LOGISTICS,
    "estimated_arrival_date": SpecialistRole.LOGISTICS,
    "description": SpecialistRole.COMMERCIAL_LINES,
    "article_component": SpecialistRole.COMMERCIAL_LINES,
    "hts_code": SpecialistRole.COMMERCIAL_LINES,
    "entered_value": SpecialistRole.COMMERCIAL_LINES,
    "genus": SpecialistRole.BOTANICAL,
    "species": SpecialistRole.BOTANICAL,
    "country_of_harvest": SpecialistRole.BOTANICAL,
    "plant_quantity": SpecialistRole.BOTANICAL,
    "metric_unit": SpecialistRole.BOTANICAL,
}


# Higher number means greater documentary authority for this field.  Equal numbers
# intentionally encode equal authority where the brief uses "/" rather than ">".
_DOCUMENT_AUTHORITY: dict[str, dict[DocumentType, int]] = {
    "filing_entry_reference": {DocumentType.ENTRY_WORKSHEET: 100},
    "manufacturer_id": {
        DocumentType.ENTRY_WORKSHEET: 100,
        DocumentType.COMMERCIAL_INVOICE: 80,
    },
    "importer_name": {
        DocumentType.ENTRY_WORKSHEET: 100,
        DocumentType.COMMERCIAL_INVOICE: 80,
        DocumentType.BILL_OF_LADING: 60,
        DocumentType.ARRIVAL_NOTICE: 40,
    },
    "importer_address": {
        DocumentType.ENTRY_WORKSHEET: 100,
        DocumentType.COMMERCIAL_INVOICE: 80,
        DocumentType.BILL_OF_LADING: 60,
        DocumentType.ARRIVAL_NOTICE: 40,
    },
    "consignee_name": {
        DocumentType.ENTRY_WORKSHEET: 100,
        DocumentType.BILL_OF_LADING: 100,
        DocumentType.ARRIVAL_NOTICE: 80,
    },
    "consignee_address": {
        DocumentType.ENTRY_WORKSHEET: 100,
        DocumentType.BILL_OF_LADING: 100,
        DocumentType.ARRIVAL_NOTICE: 80,
    },
    "hts_code": {
        DocumentType.ENTRY_WORKSHEET: 100,
        DocumentType.COMMERCIAL_INVOICE: 80,
    },
    "entered_value": {
        DocumentType.ENTRY_WORKSHEET: 100,
        DocumentType.COMMERCIAL_INVOICE: 100,
    },
    "bill_of_lading": {
        DocumentType.BILL_OF_LADING: 100,
        DocumentType.ENTRY_WORKSHEET: 60,
        DocumentType.ARRIVAL_NOTICE: 60,
    },
    "container_number": {
        DocumentType.BILL_OF_LADING: 100,
        DocumentType.ARRIVAL_NOTICE: 80,
        DocumentType.ENTRY_WORKSHEET: 70,
        DocumentType.PACKING_LIST: 50,
    },
    "estimated_arrival_date": {
        DocumentType.ARRIVAL_NOTICE: 100,
        DocumentType.BILL_OF_LADING: 100,
        DocumentType.ENTRY_WORKSHEET: 60,
    },
    "genus": {
        DocumentType.BOTANICAL_DECLARATION: 100,
        DocumentType.SUPPLIER_ORIGIN: 80,
    },
    "species": {
        DocumentType.BOTANICAL_DECLARATION: 100,
        DocumentType.SUPPLIER_ORIGIN: 80,
    },
    "country_of_harvest": {
        DocumentType.BOTANICAL_DECLARATION: 100,
        DocumentType.SUPPLIER_ORIGIN: 100,
    },
    "plant_quantity": {
        DocumentType.BOTANICAL_DECLARATION: 100,
        DocumentType.SUPPLIER_ORIGIN: 80,
    },
    "metric_unit": {
        DocumentType.BOTANICAL_DECLARATION: 100,
        DocumentType.SUPPLIER_ORIGIN: 80,
    },
    "description": {
        DocumentType.COMMERCIAL_INVOICE: 100,
        DocumentType.ENTRY_WORKSHEET: 80,
        DocumentType.PACKING_LIST: 50,
        DocumentType.BILL_OF_LADING: 30,
    },
    "article_component": {
        DocumentType.COMMERCIAL_INVOICE: 100,
        DocumentType.ENTRY_WORKSHEET: 80,
        DocumentType.PACKING_LIST: 50,
    },
}


def document_authority(field_key: str, document_type: DocumentType) -> int:
    return _DOCUMENT_AUTHORITY.get(field_key, {}).get(document_type, 0)


def correct_specialist(candidate: CandidateEnvelope) -> bool:
    expected = FIELD_SPECIALIST.get(candidate.candidate.field_key)
    return expected is not None and candidate.specialist is expected


def normalized_candidate_value(candidate: CandidateEnvelope) -> str:
    value = comparison_key(candidate.candidate.field_key, candidate.candidate.normalized_value)
    return value or candidate.candidate.normalized_value


def authority_tuple(
    candidate: CandidateEnvelope,
    *,
    corroborating_documents: int,
) -> tuple[int, int, int, int, float]:
    """Return the required lexicographic priority; model confidence is last."""
    return (
        int(candidate.candidate.evidence_verified),
        document_authority(candidate.candidate.field_key, candidate.document_type),
        int(correct_specialist(candidate)),
        corroborating_documents,
        candidate.candidate.confidence,
    )


def candidate_identity(candidate: CandidateEnvelope) -> str:
    """Stable opaque id for deterministic tie-breaking and Phase 5 resolver choices."""
    raw = "|".join(
        (
            str(candidate.document_id),
            candidate.document_type.value,
            candidate.specialist.value,
            candidate.candidate.field_key,
            candidate.line_item_key or "",
            str(candidate.candidate.page),
            candidate.candidate.normalized_value,
            candidate.candidate.source_text,
        )
    )
    return "cand_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]
