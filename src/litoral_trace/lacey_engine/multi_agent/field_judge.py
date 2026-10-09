"""Fail-safe semantic Judge contracts for the Lacey specialist pipeline.

The Judge is intentionally unable to create or rewrite business values. It may only
classify candidate identities that already exist after deterministic evidence
verification and line binding.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import logging
import os
import time
from typing import Mapping, Protocol

from ..ai_providers import AIProviderConfig, _post_json
from ..ai_shadow import AIShadowError
from ..gemini_provider import gemini_output_text, gemini_usage
from .authority import document_authority
from .contracts import CandidateEnvelope


logger = logging.getLogger(__name__)

FIELD_JUDGE_VERSION = "lacey_field_judge_v1"
_FIELD_JUDGE_MODE_ENV = "LT_AI_FIELD_JUDGE_MODE"


class FieldJudgeMode(str, Enum):
    OFF = "off"
    SHADOW = "shadow"
    ENFORCE = "enforce"


class FieldJudgeDecision(str, Enum):
    ACCEPT = "ACCEPT"
    REJECT = "REJECT"
    NEEDS_REVIEW = "NEEDS_REVIEW"


class FieldJudgeReason(str, Enum):
    EXACT_FIELD_CONTEXT = "EXACT_FIELD_CONTEXT"
    SEMANTIC_FIELD_MATCH = "SEMANTIC_FIELD_MATCH"
    AUTHORITATIVE_SOURCE = "AUTHORITATIVE_SOURCE"
    FIELD_MISMATCH = "FIELD_MISMATCH"
    SCOPE_MISMATCH = "SCOPE_MISMATCH"
    AMBIGUOUS_CONTEXT = "AMBIGUOUS_CONTEXT"
    INSUFFICIENT_CONTEXT = "INSUFFICIENT_CONTEXT"


@dataclass(frozen=True, slots=True)
class FieldJudgeDecisionRecord:
    candidate_id: str
    field_key: str
    line_item_key: str | None
    decision: FieldJudgeDecision
    reason: FieldJudgeReason


@dataclass(frozen=True, slots=True)
class FieldJudgeEvaluation:
    version: str
    mode: FieldJudgeMode
    provider: str
    model: str
    latency_ms: int | None
    input_tokens: int | None
    output_tokens: int | None
    total_tokens: int | None
    decisions: tuple[FieldJudgeDecisionRecord, ...]
    safe_error: str | None = None


class FieldJudgeProvider(Protocol):
    name: str
    model: str

    def judge_structured(
        self,
        *,
        prompt: str,
        schema: dict[str, object],
    ) -> tuple[Mapping[str, object], int | None, int | None, int | None, int | None]: ...


def field_judge_mode(environ: Mapping[str, str] | None = None) -> FieldJudgeMode:
    """Read the closed Judge mode, failing safely to ``off``."""

    source = os.environ if environ is None else environ
    raw = str(source.get(_FIELD_JUDGE_MODE_ENV, "off") or "off").strip().lower()
    try:
        return FieldJudgeMode(raw)
    except ValueError:
        logger.warning(
            "Invalid %s=%r; defaulting to off.",
            _FIELD_JUDGE_MODE_ENV,
            raw,
        )
        return FieldJudgeMode.OFF


def candidate_identity(envelope: CandidateEnvelope) -> str:
    """Return a deterministic identity for immutable candidate semantics.

    ``agent_run_id`` is deliberately excluded because it is execution metadata and
    would make the same evidence receive a different identity on every retry.
    """

    candidate = envelope.candidate
    canonical = {
        "document_id": str(envelope.document_id),
        "document_type": envelope.document_type.value,
        "specialist": envelope.specialist.value,
        "field_key": candidate.field_key,
        "line_item_key": envelope.line_item_key,
        "normalized_value": candidate.normalized_value,
        "evidence_class": candidate.evidence_class.value,
        "page": int(candidate.page),
        "source_text": candidate.source_text,
    }
    payload = json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return "fj1_" + hashlib.sha256(payload).hexdigest()


def _needs_review(
    envelope: CandidateEnvelope,
    *,
    reason: FieldJudgeReason,
) -> FieldJudgeDecisionRecord:
    return FieldJudgeDecisionRecord(
        candidate_id=candidate_identity(envelope),
        field_key=envelope.candidate.field_key,
        line_item_key=envelope.line_item_key,
        decision=FieldJudgeDecision.NEEDS_REVIEW,
        reason=reason,
    )


def validate_field_judge_decisions(
    candidates: tuple[CandidateEnvelope, ...],
    decisions: tuple[FieldJudgeDecisionRecord, ...],
) -> tuple[FieldJudgeDecisionRecord, ...]:
    """Validate provider decisions against the exact candidate set.

    Every existing candidate receives exactly one output record. Unknown IDs are
    ignored, duplicate/missing decisions fail closed, scope echoes must match, and
    unverified evidence can never be accepted.
    """

    by_id: dict[str, list[FieldJudgeDecisionRecord]] = {}
    for record in decisions:
        by_id.setdefault(str(record.candidate_id), []).append(record)

    validated: list[FieldJudgeDecisionRecord] = []
    for envelope in candidates:
        candidate_id = candidate_identity(envelope)
        records = by_id.get(candidate_id, [])
        if len(records) != 1:
            validated.append(
                _needs_review(envelope, reason=FieldJudgeReason.INSUFFICIENT_CONTEXT)
            )
            continue

        record = records[0]
        try:
            decision = FieldJudgeDecision(record.decision)
            reason = FieldJudgeReason(record.reason)
        except (TypeError, ValueError):
            validated.append(
                _needs_review(envelope, reason=FieldJudgeReason.INSUFFICIENT_CONTEXT)
            )
            continue

        field_matches = str(record.field_key) == envelope.candidate.field_key
        line_matches = record.line_item_key == envelope.line_item_key
        if not field_matches or not line_matches:
            validated.append(
                _needs_review(envelope, reason=FieldJudgeReason.SCOPE_MISMATCH)
            )
            continue

        if decision is FieldJudgeDecision.ACCEPT and not envelope.candidate.evidence_verified:
            validated.append(
                _needs_review(envelope, reason=FieldJudgeReason.INSUFFICIENT_CONTEXT)
            )
            continue

        validated.append(
            FieldJudgeDecisionRecord(
                candidate_id=candidate_id,
                field_key=envelope.candidate.field_key,
                line_item_key=envelope.line_item_key,
                decision=decision,
                reason=reason,
            )
        )

    return tuple(validated)


def field_judge_output_schema(
    candidates: tuple[CandidateEnvelope, ...],
) -> dict[str, object]:
    """Return a closed schema that exposes no writable business-value field."""

    candidate_ids = [candidate_identity(candidate) for candidate in candidates]
    field_keys = sorted({candidate.candidate.field_key for candidate in candidates})
    line_item_keys = sorted(
        {
            candidate.line_item_key
            for candidate in candidates
            if candidate.line_item_key is not None
        }
    )
    has_null_line = any(candidate.line_item_key is None for candidate in candidates)

    if line_item_keys and has_null_line:
        line_item_schema: dict[str, object] = {
            "anyOf": [
                {"type": "string", "enum": line_item_keys},
                {"type": "null"},
            ]
        }
    elif line_item_keys:
        line_item_schema = {"type": "string", "enum": line_item_keys}
    else:
        line_item_schema = {"type": "null"}

    item_schema: dict[str, object] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "candidate_id": {"type": "string", "enum": candidate_ids},
            "field_key": {"type": "string", "enum": field_keys},
            "line_item_key": line_item_schema,
            "decision": {
                "type": "string",
                "enum": [decision.value for decision in FieldJudgeDecision],
            },
            "reason": {
                "type": "string",
                "enum": [reason.value for reason in FieldJudgeReason],
            },
        },
        "required": [
            "candidate_id",
            "field_key",
            "line_item_key",
            "decision",
            "reason",
        ],
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "decisions": {
                "type": "array",
                "items": item_schema,
                "minItems": len(candidates),
                "maxItems": len(candidates),
            }
        },
        "required": ["decisions"],
    }


def build_field_judge_prompt(candidates: tuple[CandidateEnvelope, ...]) -> str:
    """Build bounded semantic context from already-existing candidates only."""

    context = [
        {
            "candidate_id": candidate_identity(envelope),
            "field_key": envelope.candidate.field_key,
            "line_item_key": envelope.line_item_key,
            "normalized_value": envelope.candidate.normalized_value,
            "source_text": envelope.candidate.source_text,
            "document_type": envelope.document_type.value,
            "specialist": envelope.specialist.value,
            "page": envelope.candidate.page,
            "evidence_verified": envelope.candidate.evidence_verified,
            "document_authority": document_authority(
                envelope.candidate.field_key,
                envelope.document_type,
            ),
        }
        for envelope in candidates
    ]
    return (
        "You are a customs evidence judge. Classify ONLY the supplied candidate IDs. "
        "You cannot create, correct, normalize, synthesize, infer, or return a new business "
        "value. Never rewrite normalized_value or source_text. For every supplied candidate "
        "return exactly one decision: ACCEPT, REJECT, or NEEDS_REVIEW, plus one allowed reason. "
        "ACCEPT only when the existing candidate is semantically supported by its exact source "
        "context and scope. If context is ambiguous or insufficient, return NEEDS_REVIEW.\n\n"
        "CANDIDATE CONTEXT:\n"
        + json.dumps(context, ensure_ascii=False, sort_keys=True)
    )


def _parse_field_judge_decisions(
    raw: Mapping[str, object],
) -> tuple[FieldJudgeDecisionRecord, ...]:
    raw_decisions = raw.get("decisions")
    if not isinstance(raw_decisions, list):
        return ()

    parsed: list[FieldJudgeDecisionRecord] = []
    for item in raw_decisions:
        if not isinstance(item, Mapping):
            continue
        try:
            parsed.append(
                FieldJudgeDecisionRecord(
                    candidate_id=str(item["candidate_id"]),
                    field_key=str(item["field_key"]),
                    line_item_key=(
                        None
                        if item.get("line_item_key") is None
                        else str(item.get("line_item_key"))
                    ),
                    decision=FieldJudgeDecision(str(item["decision"])),
                    reason=FieldJudgeReason(str(item["reason"])),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return tuple(parsed)


class GeminiFieldJudgeProvider:
    """Gemini structured-output adapter for the bounded semantic Judge."""

    name = "gemini"

    def __init__(self, config: AIProviderConfig) -> None:
        if not config.allow_external:
            raise AIShadowError("External AI provider is disabled by policy.")
        if not config.api_key:
            raise AIShadowError(
                "Gemini requires US_LACEY_GEMINI_API_KEY or US_LACEY_AI_API_KEY."
            )
        self.config = config
        self.model = config.model

    def judge_structured(
        self,
        *,
        prompt: str,
        schema: dict[str, object],
    ) -> tuple[Mapping[str, object], int | None, int | None, int | None, int | None]:
        payload: dict[str, object] = {
            "model": self.model,
            "store": False,
            "input": [{"type": "text", "text": prompt}],
            "generation_config": {"thinking_level": "low"},
            "response_format": {
                "type": "text",
                "mime_type": "application/json",
                "schema": schema,
            },
        }
        started = time.monotonic()
        response = _post_json(
            url=self.config.base_url,
            payload=payload,
            timeout=self.config.timeout_seconds,
            headers={"x-goog-api-key": self.config.api_key},
        )
        latency_ms = int((time.monotonic() - started) * 1000)
        input_tokens, output_tokens, total_tokens = gemini_usage(response)
        try:
            parsed = json.loads(gemini_output_text(response))
        except json.JSONDecodeError as exc:
            raise AIShadowError("Gemini Field Judge structured output is invalid JSON.") from exc
        if not isinstance(parsed, dict):
            raise AIShadowError("Gemini Field Judge output must be a JSON object.")
        return parsed, latency_ms, input_tokens, output_tokens, total_tokens


def evaluate_field_judge(
    candidates: tuple[CandidateEnvelope, ...],
    *,
    provider: FieldJudgeProvider,
    mode: FieldJudgeMode,
) -> FieldJudgeEvaluation:
    """Evaluate candidates and deterministically fail closed on provider/runtime errors."""

    effective_mode = FieldJudgeMode(mode)
    if not candidates:
        return FieldJudgeEvaluation(
            version=FIELD_JUDGE_VERSION,
            mode=effective_mode,
            provider=provider.name,
            model=provider.model,
            latency_ms=0,
            input_tokens=None,
            output_tokens=None,
            total_tokens=None,
            decisions=(),
        )

    try:
        raw, latency_ms, input_tokens, output_tokens, total_tokens = provider.judge_structured(
            prompt=build_field_judge_prompt(candidates),
            schema=field_judge_output_schema(candidates),
        )
        decisions = validate_field_judge_decisions(
            candidates,
            _parse_field_judge_decisions(raw),
        )
        return FieldJudgeEvaluation(
            version=FIELD_JUDGE_VERSION,
            mode=effective_mode,
            provider=provider.name,
            model=provider.model,
            latency_ms=latency_ms,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
            decisions=decisions,
        )
    except Exception as exc:  # Safety boundary: Judge failure must not escape shadow execution.
        logger.warning("Field Judge failed safely: %s", exc)
        return FieldJudgeEvaluation(
            version=FIELD_JUDGE_VERSION,
            mode=effective_mode,
            provider=getattr(provider, "name", "unknown"),
            model=getattr(provider, "model", "unknown"),
            latency_ms=None,
            input_tokens=None,
            output_tokens=None,
            total_tokens=None,
            decisions=validate_field_judge_decisions(candidates, ()),
            safe_error=str(exc),
        )
