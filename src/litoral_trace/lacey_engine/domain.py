"""Typed values and invariants for Lacey Engine 2.0."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class EvidenceClass(str, Enum):
    EXPLICIT = "EXPLICIT"
    DERIVED = "DERIVED"
    INFERRED = "INFERRED"


class FieldStatus(str, Enum):
    MATCHED = "MATCHED"
    CONFLICT = "CONFLICT"
    MISSING = "MISSING"


class DocumentType(str, Enum):
    ARRIVAL_NOTICE = "ARRIVAL_NOTICE"
    COMMERCIAL_INVOICE = "COMMERCIAL_INVOICE"
    PACKING_LIST = "PACKING_LIST"
    BILL_OF_LADING = "BILL_OF_LADING"
    SUPPLIER_DECLARATION = "SUPPLIER_DECLARATION"
    HARVEST_DECLARATION = "HARVEST_DECLARATION"
    SPECIES_DECLARATION = "SPECIES_DECLARATION"
    CERTIFICATE_OF_ORIGIN = "CERTIFICATE_OF_ORIGIN"
    CUSTOMS_ENTRY_SUMMARY = "CUSTOMS_ENTRY_SUMMARY"
    ISF = "ISF"
    PHYTOSANITARY_CERTIFICATE = "PHYTOSANITARY_CERTIFICATE"
    CITES_DOCUMENT = "CITES_DOCUMENT"
    WEB_PRINT_MANIFEST = "WEB_PRINT_MANIFEST"
    SPREADSHEET = "SPREADSHEET"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"


class LayoutStructureType(str, Enum):
    FREE_TEXT = "FREE_TEXT"
    KEY_VALUE_TABLE = "KEY_VALUE_TABLE"
    MATRIX_TABLE = "MATRIX_TABLE"
    LINE_ITEM_TABLE = "LINE_ITEM_TABLE"
    MULTI_HEADER_TABLE = "MULTI_HEADER_TABLE"


@dataclass(frozen=True, slots=True)
class BoundingBox:
    x0: float
    top: float
    x1: float
    bottom: float


@dataclass(frozen=True, slots=True)
class LayoutBlock:
    block_id: str
    page: int
    bbox: BoundingBox | None
    text: str
    block_type: str
    structure_type: LayoutStructureType = LayoutStructureType.FREE_TEXT
    table_id: str | None = None
    row_index: int | None = None
    column_index: int | None = None
    table_header: str | None = None
    key_text: str | None = None
    value_text: str | None = None


@dataclass(frozen=True, slots=True)
class ParsedLayout:
    blocks: tuple[LayoutBlock, ...]
    page_count: int


@dataclass(frozen=True, slots=True)
class DocumentSection:
    section_id: str
    page_start: int
    page_end: int
    document_type: DocumentType
    confidence: float
    block_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Provenance:
    filename: str
    page: int
    bbox: BoundingBox | None
    block_id: str
    source_text: str
    extractor_name: str
    extractor_version: str
    evidence_class: EvidenceClass


@dataclass(frozen=True, slots=True)
class RawCandidate:
    field_key: str
    raw_text: str
    normalized_value: str
    source_block: LayoutBlock
    evidence_class: EvidenceClass
    extractor_name: str
    extractor_version: str
    derived_from_field_key: str | None = None
    label: str | None = None


@dataclass(frozen=True, slots=True)
class AdmittedCandidate:
    raw: RawCandidate
    provenance: Provenance
    score: float
    document_type: DocumentType


@dataclass(frozen=True, slots=True)
class ResolvedField:
    field_key: str
    status: FieldStatus
    effective_value: str | None
    winning_candidate: AdmittedCandidate | None
    candidates: tuple[AdmittedCandidate, ...] = ()

    def __post_init__(self) -> None:
        distinct = {candidate.raw.normalized_value for candidate in self.candidates}
        if self.status is FieldStatus.MATCHED:
            if self.effective_value is None or self.winning_candidate is None:
                raise ValueError("MATCHED requires value and winning candidate")
            if self.winning_candidate.provenance is None:
                raise ValueError("MATCHED requires provenance")
        elif self.status is FieldStatus.MISSING:
            if self.effective_value is not None or self.winning_candidate is not None or self.candidates:
                raise ValueError("MISSING cannot contain a usable candidate")
        elif self.status is FieldStatus.CONFLICT:
            if self.effective_value is not None or self.winning_candidate is not None or len(distinct) < 2:
                raise ValueError("CONFLICT requires at least two admitted distinct values")


@dataclass(frozen=True, slots=True)
class DocumentResolution:
    filename: str
    engine_version: str
    document_type: DocumentType
    type_confidence: float
    layout: ParsedLayout
    sections: tuple[DocumentSection, ...]
    fields: dict[str, ResolvedField] = field(default_factory=dict)

    def field(self, key: str) -> ResolvedField:
        return self.fields[key]
