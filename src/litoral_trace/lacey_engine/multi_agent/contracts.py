"""Typed contracts for the Lacey multi-extractor architecture.

Phase 0 intentionally defines data boundaries only.  It does not change the legacy
AI extraction path or mutate :class:`AICandidate`; specialist output wraps the existing
candidate type in :class:`CandidateEnvelope` so the current evidence and reconciliation
safety layer remains authoritative during the migration.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from uuid import UUID

from ..ai_shadow import AICandidate


class DocumentType(str, Enum):
    COMMERCIAL_INVOICE = "COMMERCIAL_INVOICE"
    ENTRY_WORKSHEET = "ENTRY_WORKSHEET"
    BILL_OF_LADING = "BILL_OF_LADING"
    ARRIVAL_NOTICE = "ARRIVAL_NOTICE"
    BOTANICAL_DECLARATION = "BOTANICAL_DECLARATION"
    SUPPLIER_ORIGIN = "SUPPLIER_ORIGIN"
    PACKING_LIST = "PACKING_LIST"
    UNKNOWN = "UNKNOWN"


class SpecialistRole(str, Enum):
    CUSTOMS_IDENTITY = "CUSTOMS_IDENTITY"
    LOGISTICS = "LOGISTICS"
    COMMERCIAL_LINES = "COMMERCIAL_LINES"
    BOTANICAL = "BOTANICAL"


class OperationStatus(str, Enum):
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class SpecialistExecutionState(str, Enum):
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class RoutedDocument:
    document_id: UUID
    document_type: DocumentType
    pages: tuple[int, ...]
    confidence: float
    signals: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AgentTask:
    role: SpecialistRole
    documents: tuple[RoutedDocument, ...]
    allowed_fields: frozenset[str]


@dataclass(frozen=True, slots=True)
class CandidateEnvelope:
    candidate: AICandidate
    document_id: UUID
    document_type: DocumentType
    specialist: SpecialistRole
    agent_run_id: UUID
    line_item_key: str | None
    source_span_id: UUID | None


@dataclass(frozen=True, slots=True)
class SpecialistResult:
    role: SpecialistRole
    candidates: tuple[CandidateEnvelope, ...]
    provider: str
    model: str
    latency_ms: int
    warnings: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SpecialistFailure:
    role: SpecialistRole
    error: str


@dataclass(frozen=True, slots=True)
class SpecialistExecutionStatus:
    role: SpecialistRole
    status: SpecialistExecutionState
    error: str | None = None


@dataclass(frozen=True, slots=True)
class MultiAgentExtractionResult:
    # RoutingPlan is introduced by Phase 1.  Keeping this as a postponed annotation
    # avoids inventing router semantics in the Phase 0 contract layer.
    routing_plan: RoutingPlan
    specialist_results: tuple[SpecialistResult, ...]
    fused_candidates: tuple[CandidateEnvelope, ...]
    partial_failures: tuple[SpecialistFailure, ...]
    # Phase 6 appends defaults so all earlier construction sites remain compatible.
    operation: OperationStatus = OperationStatus.COMPLETED
    specialist_statuses: tuple[SpecialistExecutionStatus, ...] = ()
