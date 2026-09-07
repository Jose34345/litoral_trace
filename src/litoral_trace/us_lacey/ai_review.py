"""Bounded AI reconciliation/adjudication for U.S. Lacey human review.

This layer never invents or writes a declaration value. It may only recommend one of
already persisted, evidence-backed field candidates. Recommendations live inside the
existing reconciliation issue evidence JSON and remain non-authoritative until a human
selects or edits a value through the normal review service.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from typing import Mapping

from sqlalchemy import select

from litoral_trace.db.models import ReconciliationIssue, UsLaceyFieldCandidate, UsLaceyOperation
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.lacey_engine.ai_providers import (
    AIProviderConfig,
    PROVIDER_OPENAI,
    _openai_output_text,
    _post_json,
)
from litoral_trace.lacey_engine.ai_routing import AITask, AITierConfig
from litoral_trace.lacey_engine.ai_shadow import AIShadowError
from litoral_trace.us_lacey.db import get_us_lacey_db_session


AI_REVIEW_OFF = "OFF"
AI_REVIEW_SHADOW = "SHADOW"


@dataclass(frozen=True, slots=True)
class AIReviewConfig:
    mode: str
    max_issues_per_operation: int

    @classmethod
    def from_env(cls) -> "AIReviewConfig":
        mode = os.getenv("US_LACEY_AI_REVIEW_MODE", "off").strip().upper()
        if mode not in {AI_REVIEW_OFF, AI_REVIEW_SHADOW}:
            mode = AI_REVIEW_OFF
        try:
            maximum = max(1, min(int(os.getenv("US_LACEY_AI_REVIEW_MAX_ISSUES", "8")), 25))
        except ValueError:
            maximum = 8
        return cls(mode=mode, max_issues_per_operation=maximum)


@dataclass(frozen=True, slots=True)
class ReviewCandidate:
    candidate_id: int
    value: str
    source_page: int | None
    source_locator: str | None
    extraction_confidence: float


@dataclass(frozen=True, slots=True)
class AIReviewRecommendation:
    action: str
    candidate_id: int | None
    confidence: float
    reason_code: str
    model: str
    candidate_set_sha256: str


_DECISION_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "action": {"type": "string", "enum": ["SELECT", "NEEDS_HUMAN"]},
        "candidate_id": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "reason_code": {
            "type": "string",
            "enum": [
                "SOURCE_AUTHORITY",
                "CROSS_DOCUMENT_SUPPORT",
                "CONTEXT_CONSISTENCY",
                "CONFLICT_UNRESOLVED",
                "INSUFFICIENT_EVIDENCE",
            ],
        },
    },
    "required": ["action", "candidate_id", "confidence", "reason_code"],
}

_PROMPT = """You are a bounded evidence reconciler for U.S. Lacey Act declaration preparation.
You are NOT allowed to create, infer, calculate, rewrite, or propose a new regulatory fact.
You may only select one candidate_id from the exact candidate options supplied below.
If the evidence is insufficient, contradictory without a defensible documentary basis, or
would require outside knowledge, choose NEEDS_HUMAN with candidate_id null.
Never infer country of harvest from country of origin, addresses, ports, routing or exporter
location. Never substitute shipment gross/net weight for plant-material quantity. Never
invent an HTS, entry number, manufacturer ID, scientific name or quantity.
Prefer direct labelled documentary evidence and independent cross-document agreement.
Return only the requested structured JSON.
"""


def _candidate_set_hash(field_name: str, candidates: tuple[ReviewCandidate, ...]) -> str:
    payload = [
        {
            "id": candidate.candidate_id,
            "value": candidate.value,
            "page": candidate.source_page,
            "locator": candidate.source_locator,
        }
        for candidate in candidates
    ]
    encoded = json.dumps(
        {"field_name": field_name, "candidates": payload},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _task_for_issue(issue: ReconciliationIssue) -> AITask:
    """Reserve Sol for genuine blocking contradictions; use Terra otherwise."""
    if issue.rule_code == "US_LACEY_FIELD_CONFLICT" or issue.severity == "BLOCKING":
        return AITask.ADJUDICATE
    return AITask.RECONCILE


def _payload_text(*, field_name: str, issue: ReconciliationIssue, candidates: tuple[ReviewCandidate, ...]) -> str:
    options = [
        {
            "candidate_id": item.candidate_id,
            "value": item.value,
            "source_page": item.source_page,
            # Locators are bounded because generic extractors can persist long table
            # coordinates/text. The model receives no original file bytes in this stage.
            "source_locator": (item.source_locator or "")[:1000],
            "extraction_confidence": round(item.extraction_confidence, 4),
        }
        for item in candidates
    ]
    context = {
        "field_name": field_name,
        "issue_rule": issue.rule_code,
        "issue_severity": issue.severity,
        "left_value": issue.left_value,
        "right_value": issue.right_value,
        "candidates": options,
    }
    return _PROMPT + "\nEvidence-backed options:\n" + json.dumps(context, ensure_ascii=False, sort_keys=True)


def _call_openai_decision(
    *,
    provider_config: AIProviderConfig,
    model: str,
    field_name: str,
    issue: ReconciliationIssue,
    candidates: tuple[ReviewCandidate, ...],
) -> AIReviewRecommendation:
    candidate_hash = _candidate_set_hash(field_name, candidates)
    response = _post_json(
        url=provider_config.base_url,
        timeout=provider_config.timeout_seconds,
        headers={"Authorization": f"Bearer {provider_config.api_key}"},
        payload={
            "model": model,
            "store": False,
            "input": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": _payload_text(
                                field_name=field_name,
                                issue=issue,
                                candidates=candidates,
                            ),
                        }
                    ],
                }
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "lacey_candidate_recommendation_v1",
                    "strict": True,
                    "schema": _DECISION_SCHEMA,
                }
            },
        },
    )
    try:
        payload = json.loads(_openai_output_text(response))
    except json.JSONDecodeError as exc:
        raise AIShadowError("AI review recommendation returned invalid JSON.") from exc
    if not isinstance(payload, Mapping):
        raise AIShadowError("AI review recommendation returned an invalid object.")

    action = str(payload.get("action") or "")
    reason_code = str(payload.get("reason_code") or "")
    try:
        confidence = float(payload.get("confidence") or 0.0)
    except (TypeError, ValueError) as exc:
        raise AIShadowError("AI review confidence is invalid.") from exc
    candidate_id = payload.get("candidate_id")
    if candidate_id is not None:
        try:
            candidate_id = int(candidate_id)
        except (TypeError, ValueError) as exc:
            raise AIShadowError("AI review candidate ID is invalid.") from exc

    allowed_ids = {candidate.candidate_id for candidate in candidates}
    if action == "SELECT":
        if candidate_id not in allowed_ids:
            raise AIShadowError("AI review attempted to select an unknown candidate.")
    elif action == "NEEDS_HUMAN":
        candidate_id = None
    else:
        raise AIShadowError("AI review action is invalid.")
    if not 0.0 <= confidence <= 1.0:
        raise AIShadowError("AI review confidence is outside the supported range.")
    if reason_code not in {
        "SOURCE_AUTHORITY",
        "CROSS_DOCUMENT_SUPPORT",
        "CONTEXT_CONSISTENCY",
        "CONFLICT_UNRESOLVED",
        "INSUFFICIENT_EVIDENCE",
    }:
        raise AIShadowError("AI review reason code is invalid.")
    return AIReviewRecommendation(
        action=action,
        candidate_id=candidate_id,
        confidence=confidence,
        reason_code=reason_code,
        model=model,
        candidate_set_sha256=candidate_hash,
    )


def _review_candidates(rows: list[UsLaceyFieldCandidate]) -> tuple[ReviewCandidate, ...]:
    """Deduplicate exact candidate values while retaining strongest source record."""
    strongest: dict[str, UsLaceyFieldCandidate] = {}
    for row in rows:
        if row.validation_status != "VALID" or not (row.normalized_value or row.original_value):
            continue
        value = str(row.normalized_value or row.original_value).strip()
        key = value.casefold()
        incumbent = strongest.get(key)
        if incumbent is None or float(row.confidence) > float(incumbent.confidence):
            strongest[key] = row
    return tuple(
        ReviewCandidate(
            candidate_id=row.id,
            value=str(row.normalized_value or row.original_value).strip(),
            source_page=row.source_page,
            source_locator=row.source_locator,
            extraction_confidence=float(row.confidence),
        )
        for row in strongest.values()
    )


def recommend_open_reconciliation_issues(*, organization_id: int, operation_id: int) -> int:
    """Persist non-authoritative Terra/Sol recommendations for open evidence conflicts."""
    review_config = AIReviewConfig.from_env()
    if review_config.mode != AI_REVIEW_SHADOW:
        return 0
    provider_config = AIProviderConfig.from_env()
    if (
        provider_config.mode != "SHADOW"
        or provider_config.provider != PROVIDER_OPENAI
        or not provider_config.allow_external
        or not provider_config.api_key
    ):
        return 0

    org_id = int(organization_id)
    session = get_us_lacey_db_session()
    persisted = 0
    try:
        set_tenant_db_context(session, org_id)
        operation = session.scalar(
            select(UsLaceyOperation).where(
                UsLaceyOperation.organization_id == org_id,
                UsLaceyOperation.id == int(operation_id),
            )
        )
        if operation is None:
            return 0
        issues = session.scalars(
            select(ReconciliationIssue)
            .where(
                ReconciliationIssue.organization_id == org_id,
                ReconciliationIssue.operation_reference == f"us_lacey:{operation.public_id}",
                ReconciliationIssue.status == "OPEN",
                ReconciliationIssue.us_lacey_operation_field_id.is_not(None),
            )
            .order_by(ReconciliationIssue.id.asc())
            .limit(review_config.max_issues_per_operation)
        ).all()
        tiers = AITierConfig.from_env()
        for issue in issues:
            field_id = int(issue.us_lacey_operation_field_id or 0)
            rows = session.scalars(
                select(UsLaceyFieldCandidate)
                .where(
                    UsLaceyFieldCandidate.organization_id == org_id,
                    UsLaceyFieldCandidate.operation_id == operation.id,
                    UsLaceyFieldCandidate.operation_field_id == field_id,
                    UsLaceyFieldCandidate.decision == "PENDING",
                )
                .order_by(UsLaceyFieldCandidate.id.asc())
            ).all()
            candidates = _review_candidates(list(rows))
            if len(candidates) < 2:
                continue
            task = _task_for_issue(issue)
            model = tiers.model_for(task)
            if not model:
                continue
            current_evidence = dict(issue.evidence_json or {})
            prior = current_evidence.get("ai_recommendation")
            candidate_hash = _candidate_set_hash(str(issue.field_name or ""), candidates)
            if (
                isinstance(prior, Mapping)
                and prior.get("candidate_set_sha256") == candidate_hash
                and prior.get("model") == model
            ):
                continue
            recommendation = _call_openai_decision(
                provider_config=provider_config,
                model=model,
                field_name=str(issue.field_name or ""),
                issue=issue,
                candidates=candidates,
            )
            current_evidence["ai_recommendation"] = {
                "schema_version": "lacey_ai_candidate_recommendation_v1",
                "action": recommendation.action,
                "candidate_id": recommendation.candidate_id,
                "confidence": recommendation.confidence,
                "reason_code": recommendation.reason_code,
                "model": recommendation.model,
                "candidate_set_sha256": recommendation.candidate_set_sha256,
                "authoritative": False,
            }
            issue.evidence_json = current_evidence
            persisted += 1
        session.commit()
        return persisted
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
