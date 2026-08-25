"""Contracts shared by the Smart Import discovery and mapping engine."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class MappingStatus(StrEnum):
    """Human-safety state for one proposed source-to-canonical mapping."""

    AUTO = "AUTO"
    CONFIRM = "CONFIRM"
    MANUAL = "MANUAL"
    IGNORED = "IGNORED"


@dataclass(frozen=True)
class CanonicalFieldSpec:
    """Description of one canonical LT field and its matching behavior."""

    name: str
    required: bool
    aliases: frozenset[str]
    semantic_type: str
    high_risk: bool = False


@dataclass(frozen=True)
class MappingDecision:
    """A scored candidate mapping for one source column."""

    canonical_field: str | None
    confidence: float
    status: MappingStatus
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class ColumnMapping:
    """Mapping proposal for a source workbook column."""

    source_column: str
    source_index: int
    normalized_source: str
    decision: MappingDecision
    sample_values: tuple[Any, ...] = ()


@dataclass(frozen=True)
class DatasetCandidate:
    """One tabular region discovered inside a worksheet."""

    sheet_name: str
    header_row: int
    first_data_row: int
    estimated_rows: int
    estimated_columns: int
    score: float
    mappings: tuple[ColumnMapping, ...]
    missing_required_fields: tuple[str, ...]

    @property
    def mapped_fields(self) -> tuple[str, ...]:
        return tuple(
            mapping.decision.canonical_field
            for mapping in self.mappings
            if mapping.decision.canonical_field is not None
        )


@dataclass(frozen=True)
class SmartWorkbookAnalysis:
    """Non-persistent discovery result for an uploaded workbook."""

    filename: str
    sha256: str
    sheet_names: tuple[str, ...]
    candidates: tuple[DatasetCandidate, ...]

    @property
    def best_candidate(self) -> DatasetCandidate | None:
        if not self.candidates:
            return None
        return max(self.candidates, key=lambda candidate: candidate.score)
