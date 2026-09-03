"""Deterministic, infrastructure-independent Lacey document intelligence."""

from .domain import DocumentResolution, EvidenceClass, FieldStatus
from .pipeline import process_document

__all__ = ("DocumentResolution", "EvidenceClass", "FieldStatus", "process_document")
