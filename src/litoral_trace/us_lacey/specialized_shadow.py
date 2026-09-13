"""Operation-scoped specialized AI extraction for the U.S. Lacey shadow rollout.

This module is intentionally non-authoritative. It routes the current operation source
set to domain specialists, verifies every returned evidence span against Engine 2 before
line binding/fusion, applies the bounded Field Judge when explicitly enabled, and returns
comparison data for isolated persistence.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
import hashlib
import time
from uuid import UUID

from litoral_trace.lacey_engine.ai_providers import AIProviderConfig
from litoral_trace.lacey_engine.ai_shadow import (
    AIExtractionResult,
    AIShadowError,
    verify_ai_evidence,
)
from litoral_trace.lacey_engine.domain import DocumentResolution
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    MultiAgentExtractionResult,
    SpecialistRole,
)
from litoral_trace.lacey_engine.multi_agent.field_judge import (
    FIELD_JUDGE_VERSION,
    FieldJudgeDecision,
    FieldJudgeMode,
    FieldJudgeProvider,
    GeminiFieldJudgeProvider,
    evaluate_field_judge,
    field_judge_mode as configured_field_judge_mode,
)
from litoral_trace.lacey_engine.multi_agent.gemini_specialist_adapter import (
    GeminiSpecialistProvider,
)
from litoral_trace.lacey_engine.multi_agent.orchestrator import orchestrate_specialists
from litoral_trace.lacey_engine.multi_agent.router import build_routing_plan, route_document
from litoral_trace.lacey_engine.multi_agent.specialist_runtime import (
    ScopedAIExtractionProvider,
    SpecialistInputDocument,
)
from litoral_trace.lacey_engine.multi_agent.specialists import (
    BotanicalExtractor,
    CommercialLineExtractor,
    CustomsIdentityExtractor,
    LogisticsExtractor,
)
from litoral_trace.us_lacey.specialized_projection import (
    SPECIALIZED_PROJECTION_VERSION,
    SpecializedProjectionMode,
    specialized_projection_mode as configured_specialized_projection_mode,
)


SPECIALIZED_SHADOW_SCHEMA_VERSION = "lacey_multi_agent_shadow_v1"


@dataclass(frozen=True, slots=True)
class SpecializedShadowDocument:
    document_id: UUID
    operation_document_id: int
    assurance_document_id: int
    source_sha256: str
    role_hint: str | None
    filename: str
    content: bytes
    engine2_resolution: DocumentResolution


@dataclass(frozen=True, slots=True)
class SpecializedShadowRun:
    result: MultiAgentExtractionResult
    provider: str
    model: str
    latency_ms: int
    input_tokens: int | None
    output_tokens: int | None
    total_tokens: int | None


def _effective_judge_mode(mode: FieldJudgeMode | str | None) -> FieldJudgeMode:
    if mode is None:
        return configured_field_judge_mode()
    return FieldJudgeMode(mode)


def _effective_projection_mode(
    mode: SpecializedProjectionMode | str | None,
) -> SpecializedProjectionMode:
    if mode is None:
        return configured_specialized_projection_mode()
    return SpecializedProjectionMode(mode)


def specialized_engine_version(
    *,
    provider: str,
    model: str,
    source_set_fingerprint: str | None = None,
    judge_mode: FieldJudgeMode | str | None = None,
    projection_mode: SpecializedProjectionMode | str | None = None,
) -> str:
    """Return an immutable identity for one specialized execution contract.

    Specialized fusion is operation-scoped, so the source-set fingerprint participates
    in the persisted engine identity when available. Judge and projection versions plus
    their effective modes are also part of the identity so differently gated executions
    cannot be reused as the same immutable run.
    """
    effective_judge_mode = _effective_judge_mode(judge_mode)
    effective_projection_mode = _effective_projection_mode(projection_mode)
    identity = (
        f"v1|{provider}|{model}|schema={SPECIALIZED_SHADOW_SCHEMA_VERSION}|"
        "evidence=engine2-exact|"
        f"judge={FIELD_JUDGE_VERSION}:{effective_judge_mode.value}|"
        f"projection={SPECIALIZED_PROJECTION_VERSION}:{effective_projection_mode.value}"
    )
    if source_set_fingerprint:
        identity += f"|source_set={source_set_fingerprint}"
    return f"multi-agent-v1:{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:32]}"


def _sum_reported(values: list[int | None]) -> int | None:
    reported = [value for value in values if value is not None]
    return sum(reported) if reported else None


def _page_texts(resolution: DocumentResolution) -> dict[int, str]:
    page_count = max(0, int(resolution.layout.page_count))
    texts: dict[int, str] = {}
    for page in range(1, page_count + 1):
        texts[page] = "\n".join(
            block.text for block in resolution.layout.blocks if block.page == page
        )
    return texts


def _verify_candidates(
    candidates: tuple[CandidateEnvelope, ...],
    *,
    resolutions: dict[UUID, DocumentResolution],
) -> tuple[CandidateEnvelope, ...]:
    verified: list[CandidateEnvelope] = []
    for envelope in candidates:
        resolution = resolutions.get(envelope.document_id)
        if resolution is None:
            raise AIShadowError("Specialized candidate has no Engine 2 evidence context.")
        singleton = AIExtractionResult(
            provider=envelope.candidate.provider,
            model=envelope.candidate.model,
            schema_version=SPECIALIZED_SHADOW_SCHEMA_VERSION,
            candidates=(envelope.candidate,),
            page_count=resolution.layout.page_count,
            latency_ms=None,
        )
        checked = verify_ai_evidence(engine2=resolution, ai=singleton)
        verified.append(replace(envelope, candidate=checked.candidates[0]))
    return tuple(verified)


def _serialize_field_judge(run: SpecializedShadowRun) -> dict[str, object] | None:
    evaluation = run.result.field_judge
    if evaluation is None:
        return None

    decisions = tuple(evaluation.decisions)
    accepted_count = sum(
        decision.decision is FieldJudgeDecision.ACCEPT for decision in decisions
    )
    rejected_count = sum(
        decision.decision is FieldJudgeDecision.REJECT for decision in decisions
    )
    needs_review_count = sum(
        decision.decision is FieldJudgeDecision.NEEDS_REVIEW for decision in decisions
    )
    return {
        "version": evaluation.version,
        "mode": evaluation.mode.value,
        "provider": evaluation.provider,
        "model": evaluation.model,
        "telemetry_scope": "operation",
        "latency_ms": evaluation.latency_ms,
        "input_tokens": evaluation.input_tokens,
        "output_tokens": evaluation.output_tokens,
        "total_tokens": evaluation.total_tokens,
        "accepted_count": accepted_count,
        "rejected_count": rejected_count,
        "needs_review_count": needs_review_count,
        "safe_error": evaluation.safe_error,
        "decisions": [
            {
                "candidate_id": decision.candidate_id,
                "field_key": decision.field_key,
                "line_item_key": decision.line_item_key,
                "decision": decision.decision.value,
                "reason": decision.reason.value,
            }
            for decision in decisions
        ],
    }


def serialize_specialized_document_run(
    *,
    run: SpecializedShadowRun,
    document_id: UUID,
    source_set_fingerprint: str,
) -> dict[str, object]:
    """Serialize only fused candidates sourced from one document.

    Latency/tokens and Judge telemetry are operation-scoped totals and are explicitly
    tagged as such so a consumer cannot accidentally sum duplicated per-document
    snapshots as actual spend.
    """
    envelopes = tuple(
        envelope
        for envelope in run.result.fused_candidates
        if envelope.document_id == document_id
    )
    return {
        "schema_version": SPECIALIZED_SHADOW_SCHEMA_VERSION,
        "architecture": "specialized",
        "provider": run.provider,
        "model": run.model,
        "operation_status": run.result.operation.value,
        "source_set_fingerprint": source_set_fingerprint,
        "telemetry_scope": "operation",
        "latency_ms": run.latency_ms,
        "input_tokens": run.input_tokens,
        "output_tokens": run.output_tokens,
        "total_tokens": run.total_tokens,
        "candidate_count": len(envelopes),
        "operation_candidate_count": len(run.result.fused_candidates),
        "field_judge": _serialize_field_judge(run),
        "candidates": [
            {
                "field_key": envelope.candidate.field_key,
                "value": envelope.candidate.value,
                "normalized_value": envelope.candidate.normalized_value,
                "evidence_class": envelope.candidate.evidence_class.value,
                "page": envelope.candidate.page,
                "source_text": envelope.candidate.source_text,
                "confidence": envelope.candidate.confidence,
                "bbox": (
                    [
                        envelope.candidate.bbox.x0,
                        envelope.candidate.bbox.top,
                        envelope.candidate.bbox.x1,
                        envelope.candidate.bbox.bottom,
                    ]
                    if envelope.candidate.bbox
                    else None
                ),
                "reason": envelope.candidate.reason,
                "evidence_verified": envelope.candidate.evidence_verified,
                "document_type": envelope.document_type.value,
                "specialist": envelope.specialist.value,
                "line_item_key": envelope.line_item_key,
            }
            for envelope in envelopes
        ],
        "failed_specialists": [
            failure.role.value for failure in run.result.partial_failures
        ],
        "specialist_statuses": [
            {
                "role": status.role.value,
                "status": status.status.value,
                "failed": status.error is not None,
            }
            for status in run.result.specialist_statuses
        ],
    }


def run_specialized_shadow_operation(
    *,
    documents: tuple[SpecializedShadowDocument, ...],
    provider: ScopedAIExtractionProvider | None = None,
    config: AIProviderConfig | None = None,
    concurrency: int | None = None,
    judge_mode: FieldJudgeMode | str | None = None,
    judge_provider: FieldJudgeProvider | None = None,
) -> SpecializedShadowRun:
    """Run one current source set through bounded specialists without UI authority."""
    if not documents:
        raise AIShadowError("Specialized shadow requires at least one source document.")

    effective_mode = _effective_judge_mode(judge_mode)
    configured = config
    scoped_provider = provider
    if scoped_provider is None:
        configured = configured or AIProviderConfig.from_env()
        if configured.provider != "gemini":
            raise AIShadowError("Specialized shadow currently requires the Gemini provider.")
        scoped_provider = GeminiSpecialistProvider(configured)

    candidate_judge = None
    if effective_mode is not FieldJudgeMode.OFF:
        runtime_judge = judge_provider
        construction_error: Exception | None = None
        if runtime_judge is None:
            try:
                configured = configured or AIProviderConfig.from_env()
                runtime_judge = GeminiFieldJudgeProvider(configured)
            except Exception as exc:  # The orchestrator converts this to fail-safe review.
                construction_error = exc

        if runtime_judge is not None:
            candidate_judge = lambda candidates: evaluate_field_judge(
                candidates,
                provider=runtime_judge,
                mode=effective_mode,
            )
        elif construction_error is not None:
            def unavailable_judge(candidates):
                del candidates
                raise construction_error

            candidate_judge = unavailable_judge

    routed_documents = []
    inputs: list[SpecialistInputDocument] = []
    resolutions: dict[UUID, DocumentResolution] = {}
    for document in documents:
        page_texts = _page_texts(document.engine2_resolution)
        routed = route_document(
            document_id=document.document_id,
            page_texts=page_texts,
            filename=document.filename,
        )
        resolutions[document.document_id] = document.engine2_resolution
        for routed_document in routed:
            routed_documents.append(routed_document)
            inputs.append(
                SpecialistInputDocument(
                    routed=routed_document,
                    filename=document.filename,
                    content=document.content,
                )
            )

    routing_plan = build_routing_plan(tuple(routed_documents))
    extractors = {
        SpecialistRole.CUSTOMS_IDENTITY: CustomsIdentityExtractor(scoped_provider),
        SpecialistRole.LOGISTICS: LogisticsExtractor(scoped_provider),
        SpecialistRole.COMMERCIAL_LINES: CommercialLineExtractor(scoped_provider),
        SpecialistRole.BOTANICAL: BotanicalExtractor(scoped_provider),
    }
    started = time.monotonic()
    result = asyncio.run(
        orchestrate_specialists(
            routing_plan=routing_plan,
            documents=tuple(inputs),
            extractors=extractors,
            concurrency=concurrency,
            candidate_verifier=lambda candidates: _verify_candidates(
                candidates,
                resolutions=resolutions,
            ),
            field_judge_mode=effective_mode,
            candidate_judge=candidate_judge,
        )
    )
    elapsed = int((time.monotonic() - started) * 1000)

    return SpecializedShadowRun(
        result=result,
        provider=scoped_provider.name,
        model=scoped_provider.model,
        latency_ms=elapsed,
        input_tokens=_sum_reported(
            [specialist.input_tokens for specialist in result.specialist_results]
        ),
        output_tokens=_sum_reported(
            [specialist.output_tokens for specialist in result.specialist_results]
        ),
        total_tokens=_sum_reported(
            [specialist.total_tokens for specialist in result.specialist_results]
        ),
    )
