"""Restricted AI resolver for true Phase 4 fusion conflicts.

The resolver is deliberately incapable of returning a corrected business value.  The
model may only select one opaque candidate ID that already exists in the conflict pool
or return NEEDS_REVIEW.  A deterministic Python barrier re-validates that invariant
independently of provider-side structured output enforcement.
"""
from __future__ import annotations

import json
from typing import Literal, Mapping, Protocol

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from ..ai_providers import AIProviderConfig, _post_json
from ..ai_shadow import AIShadowError
from ..gemini_provider import gemini_output_text
from .authority import candidate_identity
from .fusion import FusionConflict


REASON_CODES: tuple[str, ...] = (
    "HIGHER_AUTHORITY_SOURCE",
    "STRONGER_CONTEXT_MATCH",
    "CONSISTENT_WITH_LINE_ITEM",
    "CORROBORATED_PROVENANCE",
    "CLEARER_SOURCE_EVIDENCE",
)


class AIResolverDecision(BaseModel):
    """Closed Phase 5 decision contract: choose an existing ID or request review."""

    model_config = ConfigDict(extra="forbid")

    field_key: str
    line_item_key: str | None
    candidate_ids: list[str] = Field(min_length=1)
    decision: Literal["SELECT", "NEEDS_REVIEW"]
    selected_candidate_id: str | None = Field(
        None,
        description=(
            "MUST be exactly one of the provided candidate IDs. Null if NEEDS_REVIEW."
        ),
    )
    reason_code: str | None = Field(
        None,
        description=(
            "Short code explaining the choice, e.g., 'HIGHER_AUTHORITY_SOURCE'. "
            "Null if NEEDS_REVIEW."
        ),
    )

    @model_validator(mode="after")
    def validate_decision_shape(self) -> "AIResolverDecision":
        if self.decision == "SELECT" and self.selected_candidate_id is None:
            raise ValueError("SELECT requires selected_candidate_id")
        if self.decision == "NEEDS_REVIEW":
            if self.selected_candidate_id is not None:
                raise ValueError("NEEDS_REVIEW requires selected_candidate_id=null")
            if self.reason_code is not None:
                raise ValueError("NEEDS_REVIEW requires reason_code=null")
        return self


class AIResolverProvider(Protocol):
    """Provider boundary used by the resolver and unit-test stubs."""

    def resolve_structured(
        self,
        *,
        prompt: str,
        schema: dict[str, object],
    ) -> Mapping[str, object]: ...


class GeminiAIResolverProvider:
    """Gemini text-only structured resolver using the existing shared transport."""

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

    def resolve_structured(
        self,
        *,
        prompt: str,
        schema: dict[str, object],
    ) -> Mapping[str, object]:
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
        response = _post_json(
            url=self.config.base_url,
            payload=payload,
            timeout=self.config.timeout_seconds,
            headers={"x-goog-api-key": self.config.api_key},
        )
        try:
            parsed = json.loads(gemini_output_text(response))
        except json.JSONDecodeError as exc:
            raise AIShadowError("Gemini resolver structured output is invalid JSON.") from exc
        if not isinstance(parsed, dict):
            raise AIShadowError("Gemini resolver output must be a JSON object.")
        return parsed


def resolve_conflict(
    conflict: FusionConflict,
    *,
    provider: AIResolverProvider,
) -> AIResolverDecision:
    """Resolve one true conflict without ever accepting an invented business value."""
    if not conflict.requires_ai_resolution:
        raise ValueError("AI resolver accepts only conflicts requiring AI resolution.")

    candidate_ids = [candidate_identity(candidate) for candidate in conflict.candidates]
    prompt = build_resolver_prompt(conflict, candidate_ids=candidate_ids)
    schema = resolver_output_schema(conflict, candidate_ids=candidate_ids)
    raw = provider.resolve_structured(prompt=prompt, schema=schema)

    try:
        decision = AIResolverDecision.model_validate(raw)
    except (ValidationError, TypeError, ValueError):
        return _needs_review(conflict, candidate_ids)

    # Deterministic post-AI barrier.  These checks do not trust provider-side schema
    # enforcement and intentionally canonicalize any suspicious response to review.
    if decision.field_key != conflict.key.field_key:
        return _needs_review(conflict, candidate_ids)
    if decision.line_item_key != conflict.key.line_item_key:
        return _needs_review(conflict, candidate_ids)
    if len(decision.candidate_ids) != len(candidate_ids):
        return _needs_review(conflict, candidate_ids)
    if set(decision.candidate_ids) != set(candidate_ids):
        return _needs_review(conflict, candidate_ids)
    if decision.reason_code is not None and decision.reason_code not in REASON_CODES:
        return _needs_review(conflict, candidate_ids)

    if decision.decision == "NEEDS_REVIEW":
        return _needs_review(conflict, candidate_ids)

    if decision.selected_candidate_id not in candidate_ids:
        # Required fail-safe: hallucinated IDs silently degrade to human review.
        return _needs_review(conflict, candidate_ids)

    return AIResolverDecision(
        field_key=conflict.key.field_key,
        line_item_key=conflict.key.line_item_key,
        candidate_ids=candidate_ids,
        decision="SELECT",
        selected_candidate_id=decision.selected_candidate_id,
        reason_code=decision.reason_code,
    )


def build_resolver_prompt(
    conflict: FusionConflict,
    *,
    candidate_ids: list[str] | None = None,
) -> str:
    """Build the bounded customs-auditor prompt from existing conflict evidence only."""
    ids = candidate_ids or [candidate_identity(candidate) for candidate in conflict.candidates]
    candidates = [
        {
            "candidate_id": candidate_id,
            "value": candidate.candidate.value,
            "source_text": candidate.candidate.source_text,
            "document_type": candidate.document_type.value,
        }
        for candidate, candidate_id in zip(conflict.candidates, ids, strict=True)
    ]
    context = {
        "field_key": conflict.key.field_key,
        "line_item_key": conflict.key.line_item_key,
        "candidates": candidates,
    }
    return (
        "You are an expert customs auditor resolving a data conflict. "
        "Analyze the provenance and context of these candidates. "
        "You CANNOT invent a new value. You MUST select the most accurate candidate ID "
        "from the provided list, or return NEEDS_REVIEW if it requires human judgment.\n\n"
        "Never output a corrected, synthesized, normalized, inferred, or replacement business "
        "value. The only selectable objects are the candidate IDs supplied below. "
        "Use only one of these reason codes when SELECT is appropriate: "
        + ", ".join(REASON_CODES)
        + ". For NEEDS_REVIEW, selected_candidate_id and reason_code MUST both be null.\n\n"
        "CONFLICT CONTEXT:\n"
        + json.dumps(context, ensure_ascii=False, sort_keys=True)
    )


def resolver_output_schema(
    conflict: FusionConflict,
    *,
    candidate_ids: list[str] | None = None,
) -> dict[str, object]:
    """Return a per-conflict schema whose string outputs are all closed enumerations."""
    ids = candidate_ids or [candidate_identity(candidate) for candidate in conflict.candidates]
    schema = AIResolverDecision.model_json_schema()
    properties = schema["properties"]

    properties["field_key"] = {
        "type": "string",
        "enum": [conflict.key.field_key],
    }
    if conflict.key.line_item_key is None:
        properties["line_item_key"] = {"type": "null"}
    else:
        properties["line_item_key"] = {
            "type": "string",
            "enum": [conflict.key.line_item_key],
        }
    properties["candidate_ids"] = {
        "type": "array",
        "items": {"type": "string", "enum": ids},
        "minItems": len(ids),
        "maxItems": len(ids),
    }
    properties["decision"] = {
        "type": "string",
        "enum": ["SELECT", "NEEDS_REVIEW"],
    }
    properties["selected_candidate_id"] = {
        "anyOf": [
            {"type": "string", "enum": ids},
            {"type": "null"},
        ],
        "description": (
            "MUST be exactly one of the provided candidate IDs. Null if NEEDS_REVIEW."
        ),
    }
    properties["reason_code"] = {
        "anyOf": [
            {"type": "string", "enum": list(REASON_CODES)},
            {"type": "null"},
        ],
        "description": "Closed reason code. Null if NEEDS_REVIEW.",
    }
    schema["additionalProperties"] = False
    return schema


def _needs_review(
    conflict: FusionConflict,
    candidate_ids: list[str],
) -> AIResolverDecision:
    return AIResolverDecision(
        field_key=conflict.key.field_key,
        line_item_key=conflict.key.line_item_key,
        candidate_ids=list(candidate_ids),
        decision="NEEDS_REVIEW",
        selected_candidate_id=None,
        reason_code=None,
    )
