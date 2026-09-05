"""AI Extraction Shadow v1: provider-neutral evidence extraction and Engine 2 comparison.

This module is deliberately infrastructure-independent. AI output is never authoritative:
it is validated against Engine 2 page text, INFERRED candidates are excluded from trusted
comparison, and reconciliation produces metrics only.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
import json
import re
import unicodedata
from typing import Mapping, Protocol

from .domain import BoundingBox, DocumentResolution, EvidenceClass, FieldStatus

AI_SHADOW_SCHEMA_VERSION = "lacey_ai_shadow_v1"
AI_FIELDS = (
    "estimated_arrival_date", "bill_of_lading", "container_number", "consignee_name",
    "consignee_address", "description", "species", "genus", "filing_entry_reference",
    "manufacturer_id", "hts_code", "country_of_harvest", "plant_quantity", "metric_unit",
)

class AIShadowError(RuntimeError):
    """Safe AI-shadow failure."""

class ReconciliationStatus(str, Enum):
    AGREEMENT = "AGREEMENT"
    ENGINE2_ONLY = "ENGINE2_ONLY"
    AI_ONLY = "AI_ONLY"
    CONFLICT = "CONFLICT"
    BOTH_MISSING = "BOTH_MISSING"
    AI_AMBIGUOUS = "AI_AMBIGUOUS"
    ENGINE2_CONFLICT = "ENGINE2_CONFLICT"
    AI_REJECTED = "AI_REJECTED"

@dataclass(frozen=True, slots=True)
class AICandidate:
    field_key: str
    value: str
    normalized_value: str
    evidence_class: EvidenceClass
    page: int
    source_text: str
    confidence: float
    provider: str
    model: str
    bbox: BoundingBox | None = None
    reason: str | None = None
    evidence_verified: bool = False

@dataclass(frozen=True, slots=True)
class AIExtractionResult:
    provider: str
    model: str
    schema_version: str
    candidates: tuple[AICandidate, ...]
    page_count: int | None = None
    latency_ms: int | None = None

@dataclass(frozen=True, slots=True)
class ReconciledField:
    field_key: str
    status: ReconciliationStatus
    engine2_value: str | None
    ai_value: str | None
    ai_candidate_count: int
    rejected_ai_candidate_count: int

@dataclass(frozen=True, slots=True)
class AIShadowComparison:
    provider: str
    model: str
    schema_version: str
    fields: tuple[ReconciledField, ...]
    verified_evidence_rate: float
    inferred_candidate_rate: float

    def field(self, key: str) -> ReconciledField:
        return next(item for item in self.fields if item.field_key == key)

@dataclass(frozen=True, slots=True)
class GoldenMetrics:
    engine2_precision: float
    engine2_recall: float
    ai_precision: float
    ai_recall: float
    agreement_rate: float
    ai_false_candidate_rate: float
    ai_unverified_evidence_rate: float
    ai_inferred_candidate_rate: float
    reconciliation_conflicts: int
    evaluated_fields: int

class AIExtractionProvider(Protocol):
    name: str
    model: str
    def extract(self, *, filename: str, content: bytes) -> AIExtractionResult: ...

_IDENTIFIER_FIELDS = {"bill_of_lading", "container_number", "filing_entry_reference", "manufacturer_id", "hts_code"}
_CASE_INSENSITIVE_FIELDS = {"consignee_name", "consignee_address", "description", "species", "genus", "country_of_harvest", "metric_unit"}

def _fold(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", text.casefold()).strip()

def normalize_ai_value(field_key: str, value: str) -> str:
    text = " ".join(str(value or "").split()).strip()
    if not text:
        raise AIShadowError("AI candidate value is empty.")
    if field_key in _IDENTIFIER_FIELDS:
        return re.sub(r"[^A-Z0-9.-]+", "", text.upper())
    return text

def comparison_key(field_key: str, value: str | None) -> str | None:
    if value is None:
        return None
    normalized = normalize_ai_value(field_key, value)
    return normalized.upper() if field_key in _IDENTIFIER_FIELDS else _fold(normalized)

def _bbox_from_payload(payload: object) -> BoundingBox | None:
    if payload is None:
        return None
    if not isinstance(payload, (list, tuple)) or len(payload) != 4:
        raise AIShadowError("AI candidate bbox must contain four numbers.")
    try:
        x0, top, x1, bottom = (float(item) for item in payload)
    except (TypeError, ValueError) as exc:
        raise AIShadowError("AI candidate bbox contains an invalid coordinate.") from exc
    if x1 < x0 or bottom < top:
        raise AIShadowError("AI candidate bbox is inverted.")
    return BoundingBox(x0=x0, top=top, x1=x1, bottom=bottom)

def candidate_from_payload(*, payload: Mapping[str, object], provider: str, model: str) -> AICandidate:
    field_key = str(payload.get("field_key") or "").strip()
    if field_key not in AI_FIELDS:
        raise AIShadowError(f"Unsupported AI field: {field_key or '<empty>'}")
    value = str(payload.get("value") or "").strip()
    if not value:
        raise AIShadowError("AI candidate value is empty.")
    try:
        evidence_class = EvidenceClass(str(payload.get("evidence_class") or ""))
    except ValueError as exc:
        raise AIShadowError("AI candidate evidence_class is invalid.") from exc
    try:
        page = int(payload.get("page") or 0)
    except (TypeError, ValueError) as exc:
        raise AIShadowError("AI candidate page is invalid.") from exc
    if page < 1:
        raise AIShadowError("AI candidate page must be 1-indexed.")
    source_text = " ".join(str(payload.get("source_text") or "").split()).strip()
    if not source_text:
        raise AIShadowError("AI candidate requires exact source_text.")
    try:
        confidence = float(payload.get("confidence", 0.0))
    except (TypeError, ValueError) as exc:
        raise AIShadowError("AI candidate confidence is invalid.") from exc
    if not 0.0 <= confidence <= 1.0:
        raise AIShadowError("AI candidate confidence must be between 0 and 1.")
    return AICandidate(
        field_key=field_key, value=value, normalized_value=normalize_ai_value(field_key, value),
        evidence_class=evidence_class, page=page, source_text=source_text, confidence=confidence,
        provider=provider, model=model, bbox=_bbox_from_payload(payload.get("bbox")),
        reason=str(payload.get("reason") or "").strip() or None,
    )

def extraction_result_from_payload(*, payload: Mapping[str, object], provider: str, model: str, page_count: int | None = None, latency_ms: int | None = None) -> AIExtractionResult:
    raw_candidates = payload.get("candidates")
    if not isinstance(raw_candidates, list):
        raise AIShadowError("AI response must contain a candidates array.")
    if not all(isinstance(item, Mapping) for item in raw_candidates):
        raise AIShadowError("AI response contains a non-object candidate.")
    candidates = tuple(candidate_from_payload(payload=item, provider=provider, model=model) for item in raw_candidates)
    return AIExtractionResult(provider, model, AI_SHADOW_SCHEMA_VERSION, candidates, page_count, latency_ms)

def verify_ai_evidence(*, engine2: DocumentResolution, ai: AIExtractionResult) -> AIExtractionResult:
    page_blocks: dict[int, list[object]] = {}
    for block in engine2.layout.blocks:
        if block.page >= 1:
            page_blocks.setdefault(block.page, []).append(block)
    verified: list[AICandidate] = []
    for candidate in ai.candidates:
        source = _fold(candidate.source_text)
        blocks = page_blocks.get(candidate.page, [])
        page_text = _fold("\n".join(block.text for block in blocks))
        evidence_verified = bool(source) and bool(page_text) and source in page_text
        # Provider-supplied coordinates are never trusted. When the exact source
        # text maps to one Engine 2 layout block, inherit that deterministic bbox;
        # otherwise keep the evidence text verified but expose no rectangle.
        source_block = next((block for block in blocks if source and source in _fold(block.text)), None)
        bbox = source_block.bbox if evidence_verified and source_block is not None else None
        verified.append(replace(candidate, evidence_verified=evidence_verified, bbox=bbox))
    return replace(ai, candidates=tuple(verified))

def _engine2_value(engine2: DocumentResolution, field_key: str) -> tuple[str | None, bool]:
    field = engine2.fields.get(field_key)
    if field is None or field.status is FieldStatus.MISSING:
        return None, False
    if field.status is FieldStatus.CONFLICT:
        return None, True
    return field.effective_value, False

def _ai_resolution(candidates: tuple[AICandidate, ...], field_key: str) -> tuple[str | None, bool, int, int]:
    relevant = [candidate for candidate in candidates if candidate.field_key == field_key]
    trusted = [candidate for candidate in relevant if candidate.evidence_verified and candidate.evidence_class is not EvidenceClass.INFERRED]
    rejected = len(relevant) - len(trusted)
    distinct: dict[str, list[AICandidate]] = {}
    for candidate in trusted:
        key = comparison_key(field_key, candidate.normalized_value)
        if key is not None:
            distinct.setdefault(key, []).append(candidate)
    if len(distinct) > 1:
        return None, True, len(relevant), rejected
    if not distinct:
        return None, False, len(relevant), rejected
    winner = max(next(iter(distinct.values())), key=lambda candidate: candidate.confidence)
    return winner.normalized_value, False, len(relevant), rejected

def reconcile_engine2_with_ai(*, engine2: DocumentResolution, ai: AIExtractionResult) -> AIShadowComparison:
    ai = verify_ai_evidence(engine2=engine2, ai=ai)
    rows: list[ReconciledField] = []
    for field_key in AI_FIELDS:
        engine_value, engine_conflict = _engine2_value(engine2, field_key)
        ai_value, ai_ambiguous, candidate_count, rejected_count = _ai_resolution(ai.candidates, field_key)
        if engine_conflict:
            status = ReconciliationStatus.ENGINE2_CONFLICT
        elif ai_ambiguous:
            status = ReconciliationStatus.AI_AMBIGUOUS
        elif engine_value is not None and ai_value is not None:
            status = ReconciliationStatus.AGREEMENT if comparison_key(field_key, engine_value) == comparison_key(field_key, ai_value) else ReconciliationStatus.CONFLICT
        elif engine_value is not None:
            status = ReconciliationStatus.ENGINE2_ONLY
        elif ai_value is not None:
            status = ReconciliationStatus.AI_ONLY
        elif candidate_count and rejected_count == candidate_count:
            status = ReconciliationStatus.AI_REJECTED
        else:
            status = ReconciliationStatus.BOTH_MISSING
        rows.append(ReconciledField(field_key, status, engine_value, ai_value, candidate_count, rejected_count))
    total = len(ai.candidates)
    verified = sum(candidate.evidence_verified for candidate in ai.candidates)
    inferred = sum(candidate.evidence_class is EvidenceClass.INFERRED for candidate in ai.candidates)
    return AIShadowComparison(ai.provider, ai.model, AI_SHADOW_SCHEMA_VERSION, tuple(rows), verified / total if total else 0.0, inferred / total if total else 0.0)

def _precision_and_recall(*, expected: Mapping[str, str | None], values: Mapping[str, str | None]) -> tuple[float, float, int, int]:
    claims = correct = expected_present = 0
    for field_key, expected_value in expected.items():
        actual = values.get(field_key)
        if expected_value is not None:
            expected_present += 1
        if actual is not None:
            claims += 1
            if expected_value is not None and comparison_key(field_key, actual) == comparison_key(field_key, expected_value):
                correct += 1
    return (correct / claims if claims else 0.0, correct / expected_present if expected_present else 1.0, claims, correct)

def evaluate_golden(*, engine2: DocumentResolution, ai: AIExtractionResult, expected: Mapping[str, str | None]) -> GoldenMetrics:
    unknown = set(expected) - set(AI_FIELDS)
    if unknown:
        raise AIShadowError(f"Golden expectation contains unsupported fields: {sorted(unknown)}")
    verified_ai = verify_ai_evidence(engine2=engine2, ai=ai)
    comparison = reconcile_engine2_with_ai(engine2=engine2, ai=verified_ai)
    engine_values = {key: _engine2_value(engine2, key)[0] for key in expected}
    ai_values = {key: _ai_resolution(verified_ai.candidates, key)[0] for key in expected}
    engine_precision, engine_recall, _, _ = _precision_and_recall(expected=expected, values=engine_values)
    ai_precision, ai_recall, ai_claims, ai_correct = _precision_and_recall(expected=expected, values=ai_values)
    comparable = [row for row in comparison.fields if row.field_key in expected and row.engine2_value is not None and row.ai_value is not None]
    agreements = sum(row.status is ReconciliationStatus.AGREEMENT for row in comparable)
    all_candidates = [candidate for candidate in verified_ai.candidates if candidate.field_key in expected]
    unverified = sum(not candidate.evidence_verified for candidate in all_candidates)
    inferred = sum(candidate.evidence_class is EvidenceClass.INFERRED for candidate in all_candidates)
    conflicts = sum(row.field_key in expected and row.status in {ReconciliationStatus.CONFLICT, ReconciliationStatus.AI_AMBIGUOUS, ReconciliationStatus.ENGINE2_CONFLICT} for row in comparison.fields)
    return GoldenMetrics(
        engine_precision, engine_recall, ai_precision, ai_recall,
        agreements / len(comparable) if comparable else 0.0,
        (ai_claims - ai_correct) / ai_claims if ai_claims else 0.0,
        unverified / len(all_candidates) if all_candidates else 0.0,
        inferred / len(all_candidates) if all_candidates else 0.0,
        conflicts, len(expected),
    )

def serialize_ai_shadow_run(*, ai: AIExtractionResult, comparison: AIShadowComparison) -> dict[str, object]:
    return {
        "schema_version": AI_SHADOW_SCHEMA_VERSION,
        "provider": ai.provider,
        "model": ai.model,
        "page_count": ai.page_count,
        "latency_ms": ai.latency_ms,
        "candidates": [
            {
                "field_key": c.field_key, "value": c.value, "normalized_value": c.normalized_value,
                "evidence_class": c.evidence_class.value, "page": c.page, "source_text": c.source_text,
                "confidence": c.confidence,
                "bbox": [c.bbox.x0, c.bbox.top, c.bbox.x1, c.bbox.bottom] if c.bbox else None,
                "reason": c.reason, "evidence_verified": c.evidence_verified,
            } for c in ai.candidates
        ],
        "reconciliation": [
            {
                "field_key": row.field_key, "status": row.status.value,
                "engine2_value": row.engine2_value, "ai_value": row.ai_value,
                "ai_candidate_count": row.ai_candidate_count,
                "rejected_ai_candidate_count": row.rejected_ai_candidate_count,
            } for row in comparison.fields
        ],
        "summary": {
            "verified_evidence_rate": comparison.verified_evidence_rate,
            "inferred_candidate_rate": comparison.inferred_candidate_rate,
            "agreement_count": sum(row.status is ReconciliationStatus.AGREEMENT for row in comparison.fields),
            "conflict_count": sum(row.status is ReconciliationStatus.CONFLICT for row in comparison.fields),
            "ai_only_count": sum(row.status is ReconciliationStatus.AI_ONLY for row in comparison.fields),
            "engine2_only_count": sum(row.status is ReconciliationStatus.ENGINE2_ONLY for row in comparison.fields),
        },
    }

def golden_metrics_to_dict(metrics: GoldenMetrics) -> dict[str, object]:
    return {
        "engine2_precision": round(metrics.engine2_precision, 6),
        "engine2_recall": round(metrics.engine2_recall, 6),
        "ai_precision": round(metrics.ai_precision, 6),
        "ai_recall": round(metrics.ai_recall, 6),
        "agreement_rate": round(metrics.agreement_rate, 6),
        "ai_false_candidate_rate": round(metrics.ai_false_candidate_rate, 6),
        "ai_unverified_evidence_rate": round(metrics.ai_unverified_evidence_rate, 6),
        "ai_inferred_candidate_rate": round(metrics.ai_inferred_candidate_rate, 6),
        "reconciliation_conflicts": metrics.reconciliation_conflicts,
        "evaluated_fields": metrics.evaluated_fields,
    }

def dump_golden_metrics(metrics: GoldenMetrics) -> str:
    return json.dumps(golden_metrics_to_dict(metrics), indent=2, sort_keys=True)
