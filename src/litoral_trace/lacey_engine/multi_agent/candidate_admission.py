"""Deterministic admission for verified specialized Lacey candidates.

This layer is intentionally narrower than fusion. A candidate must first prove that it
is safe enough to participate in Judge/fusion; admission never makes the candidate
canonical or human-reviewed truth.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable
from uuid import UUID

from ..domain import EvidenceClass
from .authority import (
    FIELD_SPECIALIST,
    candidate_identity,
    correct_specialist,
    document_authority,
)
from .contracts import CandidateEnvelope
from .line_binding import LINE_SCOPED_FIELDS


CANDIDATE_ADMISSION_VERSION = "lacey_verified_candidate_admission_v1"


class CandidateAdmissionDecision(str, Enum):
    ADMITTED = "ADMITTED"
    BLOCKED = "BLOCKED"


class CandidateAdmissionReason(str, Enum):
    VERIFIED_SUPPORTED = "VERIFIED_SUPPORTED"
    EVIDENCE_UNVERIFIED = "EVIDENCE_UNVERIFIED"
    EVIDENCE_NOT_EXPLICIT = "EVIDENCE_NOT_EXPLICIT"
    FIELD_NOT_ALLOWED = "FIELD_NOT_ALLOWED"
    WRONG_SPECIALIST = "WRONG_SPECIALIST"
    SOURCE_AUTHORITY_UNSUPPORTED = "SOURCE_AUTHORITY_UNSUPPORTED"
    LINE_BINDING_AMBIGUOUS = "LINE_BINDING_AMBIGUOUS"
    INVALID_VALUE = "INVALID_VALUE"
    SEMANTIC_ROLE_MISMATCH = "SEMANTIC_ROLE_MISMATCH"


@dataclass(frozen=True, slots=True)
class CandidateAdmissionRecord:
    candidate_id: str
    document_id: UUID
    field_key: str
    line_item_key: str | None
    decision: CandidateAdmissionDecision
    reason: CandidateAdmissionReason


@dataclass(frozen=True, slots=True)
class CandidateAdmissionEvaluation:
    version: str
    admitted_candidates: tuple[CandidateEnvelope, ...]
    records: tuple[CandidateAdmissionRecord, ...]

    @property
    def admitted_count(self) -> int:
        return sum(
            record.decision is CandidateAdmissionDecision.ADMITTED
            for record in self.records
        )

    @property
    def blocked_count(self) -> int:
        return sum(
            record.decision is CandidateAdmissionDecision.BLOCKED
            for record in self.records
        )


CandidateAdmissionValidator = Callable[
    [CandidateEnvelope], CandidateAdmissionReason | None
]


def _stable_line_binding(candidate: CandidateEnvelope) -> bool:
    field_key = candidate.candidate.field_key
    line_key = candidate.line_item_key

    if field_key not in LINE_SCOPED_FIELDS:
        return line_key is None
    if not line_key:
        return False
    return line_key.startswith(("SKU:", "LINE:", "ROW:"))


def _structural_reason(candidate: CandidateEnvelope) -> CandidateAdmissionReason | None:
    evidence = candidate.candidate
    if not evidence.evidence_verified:
        return CandidateAdmissionReason.EVIDENCE_UNVERIFIED
    if evidence.evidence_class is not EvidenceClass.EXPLICIT:
        return CandidateAdmissionReason.EVIDENCE_NOT_EXPLICIT
    if evidence.field_key not in FIELD_SPECIALIST:
        return CandidateAdmissionReason.FIELD_NOT_ALLOWED
    if not correct_specialist(candidate):
        return CandidateAdmissionReason.WRONG_SPECIALIST
    if document_authority(evidence.field_key, candidate.document_type) <= 0:
        return CandidateAdmissionReason.SOURCE_AUTHORITY_UNSUPPORTED
    if not _stable_line_binding(candidate):
        return CandidateAdmissionReason.LINE_BINDING_AMBIGUOUS
    return None


def evaluate_verified_candidate_admission(
    candidates: tuple[CandidateEnvelope, ...],
    *,
    extra_validator: CandidateAdmissionValidator | None = None,
) -> CandidateAdmissionEvaluation:
    """Evaluate candidates in stable order and fail closed on every admission gate."""
    admitted: list[CandidateEnvelope] = []
    records: list[CandidateAdmissionRecord] = []

    for candidate in candidates:
        reason = _structural_reason(candidate)
        if reason is None and extra_validator is not None:
            reason = extra_validator(candidate)

        decision = (
            CandidateAdmissionDecision.ADMITTED
            if reason is None
            else CandidateAdmissionDecision.BLOCKED
        )
        if reason is None:
            reason = CandidateAdmissionReason.VERIFIED_SUPPORTED
            admitted.append(candidate)

        records.append(
            CandidateAdmissionRecord(
                candidate_id=candidate_identity(candidate),
                document_id=candidate.document_id,
                field_key=candidate.candidate.field_key,
                line_item_key=candidate.line_item_key,
                decision=decision,
                reason=reason,
            )
        )

    return CandidateAdmissionEvaluation(
        version=CANDIDATE_ADMISSION_VERSION,
        admitted_candidates=tuple(admitted),
        records=tuple(records),
    )
