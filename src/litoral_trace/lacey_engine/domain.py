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

@dataclass(frozen=True, slots=True)
class LogicalDocumentResolution:
    """Logical document discovered inside one immutable physical source."""

    logical_document_id: str
    parent_filename: str
    page_start: int
    page_end: int
    document_type: DocumentType
    type_confidence: float
    resolution: DocumentResolution

    def __post_init__(self) -> None:
        if not str(self.logical_document_id or "").strip():
            raise ValueError(
                "LogicalDocumentResolution.logical_document_id must be non-empty."
            )
        if not str(self.parent_filename or "").strip():
            raise ValueError(
                "LogicalDocumentResolution.parent_filename must be non-empty."
            )
        if self.page_start < 1:
            raise ValueError(
                "LogicalDocumentResolution.page_start must be >= 1."
            )
        if self.page_end < self.page_start:
            raise ValueError(
                "LogicalDocumentResolution.page_end cannot precede page_start."
            )
        if not 0.0 <= float(self.type_confidence) <= 1.0:
            raise ValueError(
                "LogicalDocumentResolution.type_confidence must be between 0 and 1."
            )
        if self.resolution.filename != self.parent_filename:
            raise ValueError(
                "Logical document resolution must preserve the physical filename."
            )
        if self.resolution.document_type is not self.document_type:
            raise ValueError(
                "Logical document type must match DocumentResolution.document_type."
            )
        if abs(
            float(self.resolution.type_confidence) - float(self.type_confidence)
        ) > 1e-9:
            raise ValueError(
                "Logical document confidence must match DocumentResolution."
            )

        for block in self.resolution.layout.blocks:
            if not self.page_start <= block.page <= self.page_end:
                raise ValueError(
                    "LayoutBlock.page must preserve the original physical page "
                    "within the logical document range."
                )

        for section in self.resolution.sections:
            if (
                section.page_start < self.page_start
                or section.page_end > self.page_end
            ):
                raise ValueError(
                    "DocumentResolution section escapes the logical page range."
                )

    @property
    def page_count(self) -> int:
        return self.page_end - self.page_start + 1

    @property
    def virtual_filename(self) -> str:
        return (
            f"{self.parent_filename}"
            f"#pages={self.page_start}-{self.page_end}"
        )


@dataclass(frozen=True, slots=True)
class BundleResolution:
    """Logical decomposition of one immutable physical source document."""

    filename: str
    engine_version: str
    page_count: int
    documents: tuple[LogicalDocumentResolution, ...]

    def __post_init__(self) -> None:
        if not str(self.filename or "").strip():
            raise ValueError("BundleResolution.filename must be non-empty.")
        if self.page_count < 1:
            raise ValueError("BundleResolution.page_count must be >= 1.")
        if not self.documents:
            raise ValueError(
                "BundleResolution requires at least one logical document."
            )

        logical_ids = tuple(
            item.logical_document_id
            for item in self.documents
        )
        if len(logical_ids) != len(set(logical_ids)):
            raise ValueError(
                "BundleResolution logical_document_id values must be unique."
            )

        ordered = tuple(
            sorted(
                self.documents,
                key=lambda item: (
                    item.page_start,
                    item.page_end,
                    item.logical_document_id,
                ),
            )
        )

        expected_page = 1
        for item in ordered:
            if item.parent_filename != self.filename:
                raise ValueError(
                    "Logical document parent filename does not match bundle."
                )
            if item.resolution.layout.page_count != self.page_count:
                raise ValueError(
                    "Logical document layout must retain the physical page_count."
                )
            if item.page_start < expected_page:
                raise ValueError(
                    "BundleResolution logical documents overlap."
                )
            if item.page_start > expected_page:
                raise ValueError(
                    "BundleResolution logical documents contain a page gap."
                )
            if item.page_end > self.page_count:
                raise ValueError(
                    "Logical document exceeds the physical page range."
                )

            for block in item.resolution.layout.blocks:
                if not item.page_start <= block.page <= item.page_end:
                    raise ValueError(
                        "LayoutBlock.page must preserve the original physical page."
                    )

            expected_page = item.page_end + 1

        if expected_page != self.page_count + 1:
            raise ValueError(
                "BundleResolution does not cover every physical page."
            )

