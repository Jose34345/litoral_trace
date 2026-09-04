"""Pure, deterministic shipment-level evidence reconciliation for Gate 2."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import re

from .domain import AdmittedCandidate, DocumentResolution, EvidenceClass, FieldStatus
from .pipeline import ENGINE_VERSION, process_document


class ReconciliationState(str, Enum):
    SUPPORTED = "SUPPORTED"
    SUPPORTED_MULTIPLE = "SUPPORTED_MULTIPLE"
    NEAR_MATCH = "NEAR_MATCH"
    CONFLICT = "CONFLICT"
    MISSING = "MISSING"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"


class ShipmentReadiness(str, Enum):
    READY = "READY"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True, slots=True)
class ShipmentDocumentInput:
    document_id: str
    filename: str
    content: bytes | None = None
    role_hint: str | None = None
    resolution: DocumentResolution | None = None


@dataclass(frozen=True, slots=True)
class ShipmentEvidence:
    candidate_id: str
    document_id: str
    field_key: str
    normalized_value: str
    candidate: AdmittedCandidate
    authority: float


@dataclass(frozen=True, slots=True)
class CanonicalFieldCandidate:
    value: str
    evidence_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    field_key: str
    state: ReconciliationState
    values: tuple[CanonicalFieldCandidate, ...]
    supporting_evidence: tuple[ShipmentEvidence, ...]


@dataclass(frozen=True, slots=True)
class ShipmentIssue:
    issue_id: str
    field_key: str
    scope: str
    severity: str
    issue_type: str
    message: str
    candidate_ids: tuple[str, ...]
    document_ids: tuple[str, ...]
    requires_human_review: bool = True


@dataclass(frozen=True, slots=True)
class ShipmentResolution:
    engine_version: str
    documents: tuple[DocumentResolution, ...]
    canonical_fields: dict[str, ReconciliationResult]
    issues: tuple[ShipmentIssue, ...]
    readiness: ShipmentReadiness
    metrics: dict[str, int]


_SET_FIELDS = frozenset({"container_number", "species", "genus", "country_of_harvest"})
_CRITICAL = frozenset({"bill_of_lading", "container_number", "species", "country_of_harvest"})


def _normal(field_key: str, value: str) -> str:
    value = " ".join(value.upper().split())
    if field_key in {"consignee_name", "importer_name"}:
        return re.sub(r"\b(?:LLC|INC|LTD)\b", "", re.sub(r"[^A-Z0-9 ]", "", value)).strip()
    if field_key == "hts_code": return re.sub(r"\D", "", value)
    return value


def _reconcile(field_key: str, evidence: list[ShipmentEvidence]) -> ReconciliationResult:
    if not evidence:
        return ReconciliationResult(field_key, ReconciliationState.MISSING, (), ())
    groups: dict[str, list[ShipmentEvidence]] = {}
    for item in evidence: groups.setdefault(_normal(field_key, item.normalized_value), []).append(item)
    values = tuple(CanonicalFieldCandidate(key, tuple(item.candidate_id for item in group)) for key, group in groups.items())
    if field_key == "species" and len(groups) > 1:
        # Without a reliable merchandise-line association, contradictory taxa
        # cannot be safely treated as independent plant components.
        state = ReconciliationState.CONFLICT
    elif field_key in _SET_FIELDS:
        state = ReconciliationState.SUPPORTED_MULTIPLE if len(evidence) > 1 else ReconciliationState.SUPPORTED
    elif len(groups) == 1:
        state = ReconciliationState.SUPPORTED_MULTIPLE if len(evidence) > 1 else ReconciliationState.SUPPORTED
    else:
        # Cosmetic party suffixes normalize together; remaining scalar facts are reviewable conflicts.
        state = ReconciliationState.CONFLICT
    return ReconciliationResult(field_key, state, values, tuple(evidence))


def process_shipment(*, documents: list[ShipmentDocumentInput]) -> ShipmentResolution:
    """Combine independently processed documents without any infrastructure dependency."""
    resolved: list[DocumentResolution] = []
    evidence_by_field: dict[str, list[ShipmentEvidence]] = {}
    for item in documents:
        resolution = item.resolution or process_document(filename=item.filename, content=item.content or b"", role_hint=item.role_hint)
        resolved.append(resolution)
        for field_key, field in resolution.fields.items():
            if field.status is not FieldStatus.MATCHED or field.winning_candidate is None:
                continue
            candidate = field.winning_candidate
            if candidate.raw.evidence_class is EvidenceClass.INFERRED:
                continue
            evidence = ShipmentEvidence(f"{item.document_id}:{field_key}:{candidate.provenance.block_id}", item.document_id, field_key, field.effective_value or "", candidate, candidate.score)
            evidence_by_field.setdefault(field_key, []).append(evidence)
    fields = {key: _reconcile(key, values) for key, values in evidence_by_field.items()}
    for key in _CRITICAL: fields.setdefault(key, _reconcile(key, []))
    issues: list[ShipmentIssue] = []
    for key, result in fields.items():
        if result.state is ReconciliationState.CONFLICT:
            issues.append(ShipmentIssue(f"conflict:{key}", key, "shipment", "HIGH", "CONFLICT", f"Conflicting admissible {key} evidence.", tuple(item.candidate_id for item in result.supporting_evidence), tuple(sorted({item.document_id for item in result.supporting_evidence}))))
        elif key in _CRITICAL and result.state is ReconciliationState.MISSING:
            issues.append(ShipmentIssue(f"missing:{key}", key, "shipment", "HIGH", "MISSING_REQUIRED", f"Required baseline evidence missing for {key}.", (), ()))
    readiness = ShipmentReadiness.BLOCKED if any(issue.severity == "HIGH" for issue in issues) else ShipmentReadiness.READY
    metrics = {"documents_processed": len(resolved), "fields_supported": sum(result.state in {ReconciliationState.SUPPORTED, ReconciliationState.SUPPORTED_MULTIPLE} for result in fields.values()), "fields_conflicting": sum(result.state is ReconciliationState.CONFLICT for result in fields.values()), "fields_missing": sum(result.state is ReconciliationState.MISSING for result in fields.values()), "fields_review_required": 0, "rejected_candidates": 0, "inferred_candidates_not_used": 0}
    return ShipmentResolution(ENGINE_VERSION, tuple(resolved), fields, tuple(issues), readiness, metrics)
