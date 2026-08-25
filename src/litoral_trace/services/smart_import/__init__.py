"""Smart Excel intake for mapping heterogeneous workbooks to LT canonical schemas."""

from .canonicalize import (
    ConfirmedMapping,
    SmartCanonicalizationError,
    canonicalize_workbook,
    default_confirmed_mapping,
)
from .contracts import (
    CanonicalFieldSpec,
    ColumnMapping,
    DatasetCandidate,
    MappingDecision,
    MappingStatus,
    SmartWorkbookAnalysis,
)
from .engine import SmartImportEngine, SmartImportError

__all__ = [
    "CanonicalFieldSpec",
    "ColumnMapping",
    "ConfirmedMapping",
    "DatasetCandidate",
    "MappingDecision",
    "MappingStatus",
    "SmartCanonicalizationError",
    "SmartImportEngine",
    "SmartImportError",
    "SmartWorkbookAnalysis",
    "canonicalize_workbook",
    "default_confirmed_mapping",
]
