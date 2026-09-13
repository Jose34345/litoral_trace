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
from typing import Mapping

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
