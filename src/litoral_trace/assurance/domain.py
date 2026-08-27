"""Stable domain catalogs for Assurance document intelligence."""
from __future__ import annotations

from enum import StrEnum


class AssuranceDocumentType(StrEnum):
    INVOICE = "INVOICE"
    DELIVERY_NOTE = "DELIVERY_NOTE"
    FOREST_GUIDE = "FOREST_GUIDE"
    PHYTOSANITARY_CERTIFICATE = "PHYTOSANITARY_CERTIFICATE"
    CUSTOMS_DOCUMENT = "CUSTOMS_DOCUMENT"
    SPREADSHEET = "SPREADSHEET"
    UNKNOWN = "UNKNOWN"


class DocumentProcessingStatus(StrEnum):
    UPLOADED = "UPLOADED"
    PROCESSING = "PROCESSING"
    EXTRACTED = "EXTRACTED"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    FAILED = "FAILED"


class ExtractionRunStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    FAILED = "FAILED"


class ConfidenceLevel(StrEnum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class DocumentLinkedEntityType(StrEnum):
    SUPPLIER = "SUPPLIER"
    LOT = "LOT"
    ORDER = "ORDER"
    SHIPMENT = "SHIPMENT"
    OPERATION = "OPERATION"


class DocumentLinkMethod(StrEnum):
    EXACT_IDENTIFIER = "EXACT_IDENTIFIER"
    NORMALIZED_IDENTIFIER = "NORMALIZED_IDENTIFIER"
    HEURISTIC = "HEURISTIC"
    HUMAN_CONFIRMED = "HUMAN_CONFIRMED"
