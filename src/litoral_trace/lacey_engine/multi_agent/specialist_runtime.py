"""Shared execution boundary for deterministic Lacey extraction specialists.

Specialists own field scope and domain prompting, not transport.  The provider performs
structured extraction through the existing AI-shadow conversion path; this runtime then
enforces the specialist contract again before wrapping existing ``AICandidate`` objects.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from uuid import uuid4

from ..ai_shadow import AIExtractionResult
from .contracts import (
    AgentTask,
    CandidateEnvelope,
    RoutedDocument,
    SpecialistResult,
    SpecialistRole,
)


@dataclass(frozen=True, slots=True)
class SpecialistInputDocument:
    routed: RoutedDocument
    filename: str
    content: bytes


class ScopedAIExtractionProvider(Protocol):
    name: str
    model: str

    def extract_scoped(
        self,
        *,
        filename: str,
        content: bytes,
        pages: tuple[int, ...],
        allowed_fields: frozenset[str],
        prompt: str,
    ) -> AIExtractionResult: ...


def _sum_reported(values: list[int | None]) -> int | None:
    reported = [value for value in values if value is not None]
    return sum(reported) if reported else None


class BaseSpecialistExtractor:
    """Base class that guarantees a specialist cannot leak fields outside its scope."""

    role: SpecialistRole
    allowed_fields: frozenset[str]
    prompt: str

    def __init__(self, provider: ScopedAIExtractionProvider) -> None:
        self.provider = provider

    def task_for(self, documents: tuple[RoutedDocument, ...]) -> AgentTask:
        return AgentTask(
            role=self.role,
            documents=documents,
            allowed_fields=self.allowed_fields,
        )

    def extract(self, documents: tuple[SpecialistInputDocument, ...]) -> SpecialistResult:
        agent_run_id = uuid4()
        envelopes: list[CandidateEnvelope] = []
        warnings: list[str] = []
        latency_ms = 0
        input_tokens: list[int | None] = []
        output_tokens: list[int | None] = []
        total_tokens: list[int | None] = []

        for source in documents:
            result = self.provider.extract_scoped(
                filename=source.filename,
                content=source.content,
                pages=source.routed.pages,
                allowed_fields=self.allowed_fields,
                prompt=self.prompt,
            )
            latency_ms += result.latency_ms or 0
            input_tokens.append(result.input_tokens)
            output_tokens.append(result.output_tokens)
            total_tokens.append(result.total_tokens)

            for candidate in result.candidates:
                if candidate.field_key not in self.allowed_fields:
                    warnings.append(
                        f"OUT_OF_SCOPE_FIELD:{candidate.field_key}:{source.routed.document_type.value}"
                    )
                    continue
                if candidate.page not in source.routed.pages:
                    warnings.append(
                        f"OUT_OF_SCOPE_PAGE:{candidate.page}:{source.routed.document_type.value}"
                    )
                    continue
                envelopes.append(
                    CandidateEnvelope(
                        candidate=candidate,
                        document_id=source.routed.document_id,
                        document_type=source.routed.document_type,
                        specialist=self.role,
                        agent_run_id=agent_run_id,
                        # Phase 3 owns line binding.  Do not guess identity here.
                        line_item_key=None,
                        source_span_id=None,
                    )
                )

        return SpecialistResult(
            role=self.role,
            candidates=tuple(envelopes),
            provider=self.provider.name,
            model=self.provider.model,
            latency_ms=latency_ms,
            warnings=tuple(warnings),
            input_tokens=_sum_reported(input_tokens),
            output_tokens=_sum_reported(output_tokens),
            total_tokens=_sum_reported(total_tokens),
        )
