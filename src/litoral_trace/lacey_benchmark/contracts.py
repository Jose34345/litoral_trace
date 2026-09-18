"""Stable contracts for the U.S. Lacey Golden Benchmark.

These contracts are evaluation-only. They do not become canonical shipment truth and
must not be imported by production extraction paths.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from litoral_trace.lacey_engine.multi_agent.contracts import DocumentType


class FieldDecisionStatus(str, Enum):
    SAFE_SUGGESTION = "SAFE_SUGGESTION"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    MISSING = "MISSING"


@dataclass(frozen=True, slots=True)
class BenchmarkTelemetry:
    latency_ms: int
    cost_usd: float = 0.0


@dataclass(frozen=True, slots=True)
class DocumentPrediction:
    document_id: str
    predicted_type: DocumentType


@dataclass(frozen=True, slots=True)
class RouterCorpusVariant:
    variant_id: str
    language: str
    modality: str
    transform: str


@dataclass(frozen=True, slots=True)
class RouterCorpusManifest:
    version: str
    corpus_kind: str
    source_fixture: str
    documents_per_variant: int
    document_count: int
    variants: tuple[RouterCorpusVariant, ...]


@dataclass(frozen=True, slots=True)
class RouterCorpusDocument:
    document_id: str
    filename: str
    expected_type: DocumentType
    language: str
    modality: str
    pages: dict[int, str]


@dataclass(frozen=True, slots=True)
class FieldTruth:
    field_key: str
    value: str
    document_id: str
    page: int
    line_item_key: str | None
    evidence_text: str


@dataclass(frozen=True, slots=True)
class FieldTruthCorpus:
    version: str
    fields: tuple[FieldTruth, ...]


@dataclass(frozen=True, slots=True)
class FieldObservation:
    field_key: str
    value: str | None
    status: FieldDecisionStatus
    document_id: str | None
    page: int | None
    line_item_key: str | None
    evidence_text: str | None
    evidence_verified: bool
