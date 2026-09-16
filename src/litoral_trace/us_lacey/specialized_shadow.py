"""Operation-scoped specialized AI extraction for the U.S. Lacey shadow rollout.

Specialized model output is non-authoritative. Version 2 adds a tenant-scoped,
content-addressed computational cache before any external specialist/Judge call. Cached
runs rehydrate evidence only; current-operation projection and audit remain independent.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
import hashlib
import logging
import time
from typing import Mapping
from uuid import UUID

from litoral_trace.lacey_engine.ai_providers import AIProviderConfig
from litoral_trace.lacey_engine.ai_shadow import (
    AICandidate,
    AIExtractionResult,
    AIShadowError,
    verify_ai_evidence,
)
from litoral_trace.lacey_engine.domain import BoundingBox, DocumentResolution, EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType as SpecializedDocumentType,
    MultiAgentExtractionResult,
    OperationStatus,
    SpecialistRole,
)
from litoral_trace.lacey_engine.multi_agent.field_judge import (
    FIELD_JUDGE_VERSION,
    FieldJudgeDecision,
    FieldJudgeDecisionRecord,
    FieldJudgeEvaluation,
    FieldJudgeMode,
    FieldJudgeProvider,
    FieldJudgeReason,
    GeminiFieldJudgeProvider,
    evaluate_field_judge,
    field_judge_mode as configured_field_judge_mode,
)
from litoral_trace.lacey_engine.multi_agent.fusion import FusionConflict, FusionKey
from litoral_trace.lacey_engine.multi_agent.gemini_specialist_adapter import GeminiSpecialistProvider
from litoral_trace.lacey_engine.multi_agent.orchestrator import orchestrate_specialists
from litoral_trace.lacey_engine.multi_agent.router import RoutingPlan, build_routing_plan, route_document
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
from litoral_trace.us_lacey.specialized_inference_cache import (
    cache_organization_id,
    find_cached_specialized_payloads,
    specialized_computation_fingerprint,
)
from litoral_trace.us_lacey.specialized_projection import (
    SPECIALIZED_PROJECTION_VERSION,
    SpecializedProjectionMode,
    specialized_projection_mode as configured_specialized_projection_mode,
)


LOGGER = logging.getLogger(__name__)
SPECIALIZED_SHADOW_SCHEMA_VERSION = "lacey_multi_agent_shadow_v2"


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
    computation_fingerprint: str | None = None
    cache_documents: tuple[Mapping[str, object], ...] = ()
    cache_hit: bool = False


def _effective_judge_mode(mode: FieldJudgeMode | str | None) -> FieldJudgeMode:
    if mode is None:
        return configured_field_judge_mode()
    return FieldJudgeMode(mode)


def _effective_projection_mode(mode: SpecializedProjectionMode | str | None) -> SpecializedProjectionMode:
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
    effective_judge_mode = _effective_judge_mode(judge_mode)
    effective_projection_mode = _effective_projection_mode(projection_mode)
    identity = (
        f"v2|{provider}|{model}|schema={SPECIALIZED_SHADOW_SCHEMA_VERSION}|"
        "evidence=engine2-exact|"
        f"judge={FIELD_JUDGE_VERSION}:{effective_judge_mode.value}|"
        f"projection={SPECIALIZED_PROJECTION_VERSION}:{effective_projection_mode.value}"
    )
    if source_set_fingerprint:
        identity += f"|source_set={source_set_fingerprint}"
    return f"multi-agent-v2:{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:32]}"


def _sum_reported(values: list[int | None]) -> int | None:
    reported = [value for value in values if value is not None]
    return sum(reported) if reported else None


def _page_texts(resolution: DocumentResolution) -> dict[int, str]:
    page_count = max(0, int(resolution.layout.page_count))
    return {
        page: "\n".join(block.text for block in resolution.layout.blocks if block.page == page)
        for page in range(1, page_count + 1)
    }


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
        "accepted_count": sum(item.decision is FieldJudgeDecision.ACCEPT for item in decisions),
        "rejected_count": sum(item.decision is FieldJudgeDecision.REJECT for item in decisions),
        "needs_review_count": sum(item.decision is FieldJudgeDecision.NEEDS_REVIEW for item in decisions),
        "safe_error": evaluation.safe_error,
        "decisions": [
            {
                "candidate_id": item.candidate_id,
                "field_key": item.field_key,
                "line_item_key": item.line_item_key,
                "decision": item.decision.value,
                "reason": item.reason.value,
            }
            for item in decisions
        ],
    }


def _cache_document_for(run: SpecializedShadowRun, document_id: UUID) -> Mapping[str, object]:
    for item in run.cache_documents:
        if str(item.get("document_id") or "") == str(document_id):
            return item
    return {"document_id": str(document_id)}


def serialize_specialized_document_run(
    *,
    run: SpecializedShadowRun,
    document_id: UUID,
    source_set_fingerprint: str,
) -> dict[str, object]:
    envelopes = tuple(item for item in run.result.fused_candidates if item.document_id == document_id)
    return {
        "schema_version": SPECIALIZED_SHADOW_SCHEMA_VERSION,
        "architecture": "specialized",
        "provider": run.provider,
        "model": run.model,
        "operation_status": run.result.operation.value,
        "source_set_fingerprint": source_set_fingerprint,
        "computation_fingerprint": run.computation_fingerprint,
        "cache_hit": run.cache_hit,
        "cache_document": dict(_cache_document_for(run, document_id)),
        "telemetry_scope": "operation",
        "latency_ms": run.latency_ms,
        "input_tokens": run.input_tokens,
        "output_tokens": run.output_tokens,
        "total_tokens": run.total_tokens,
        "candidate_count": len(envelopes),
        "operation_candidate_count": len(run.result.fused_candidates),
        "field_judge": _serialize_field_judge(run),
        "fusion_conflicts": [
            {
                "field_key": conflict.key.field_key,
                "line_item_key": conflict.key.line_item_key,
                "unbound_identity": conflict.key.unbound_identity,
                "requires_ai_resolution": conflict.requires_ai_resolution,
            }
            for conflict in run.result.fusion_conflicts
        ],
        "candidates": [
            {
                "field_key": envelope.candidate.field_key,
                "value": envelope.candidate.value,
                "normalized_value": envelope.candidate.normalized_value,
                "evidence_class": envelope.candidate.evidence_class.value,
                "page": envelope.candidate.page,
                "source_text": envelope.candidate.source_text,
                "confidence": envelope.candidate.confidence,
                "bbox": ([envelope.candidate.bbox.x0, envelope.candidate.bbox.top, envelope.candidate.bbox.x1, envelope.candidate.bbox.bottom] if envelope.candidate.bbox else None),
                "reason": envelope.candidate.reason,
                "evidence_verified": envelope.candidate.evidence_verified,
                "document_type": envelope.document_type.value,
                "specialist": envelope.specialist.value,
                "line_item_key": envelope.line_item_key,
            }
            for envelope in envelopes
        ],
        "failed_specialists": [failure.role.value for failure in run.result.partial_failures],
        "specialist_statuses": [
            {"role": status.role.value, "status": status.status.value, "failed": status.error is not None}
            for status in run.result.specialist_statuses
        ],
    }


def _cached_field_judge(payload: Mapping[str, object]) -> FieldJudgeEvaluation | None:
    raw = payload.get("field_judge")
    if not isinstance(raw, Mapping):
        return None
    decisions_raw = raw.get("decisions")
    decisions: list[FieldJudgeDecisionRecord] = []
    if isinstance(decisions_raw, list):
        for item in decisions_raw:
            if not isinstance(item, Mapping):
                continue
            try:
                decisions.append(
                    FieldJudgeDecisionRecord(
                        candidate_id=str(item["candidate_id"]),
                        field_key=str(item["field_key"]),
                        line_item_key=None if item.get("line_item_key") is None else str(item.get("line_item_key")),
                        decision=FieldJudgeDecision(str(item["decision"])),
                        reason=FieldJudgeReason(str(item["reason"])),
                    )
                )
            except (KeyError, TypeError, ValueError):
                return None
    try:
        return FieldJudgeEvaluation(
            version=str(raw.get("version") or FIELD_JUDGE_VERSION),
            mode=FieldJudgeMode(str(raw.get("mode") or "off")),
            provider=str(raw.get("provider") or "cached"),
            model=str(raw.get("model") or "cached"),
            latency_ms=None,
            input_tokens=None,
            output_tokens=None,
            total_tokens=None,
            decisions=tuple(decisions),
            safe_error=None if raw.get("safe_error") is None else str(raw.get("safe_error")),
        )
    except ValueError:
        return None


def _cached_candidate(item: Mapping[str, object], *, document_id: UUID, provider: str, model: str) -> CandidateEnvelope | None:
    try:
        bbox_raw = item.get("bbox")
        bbox = None
        if isinstance(bbox_raw, list) and len(bbox_raw) == 4:
            bbox = BoundingBox(*(float(value) for value in bbox_raw))
        candidate = AICandidate(
            field_key=str(item["field_key"]),
            value=str(item["value"]),
            normalized_value=str(item["normalized_value"]),
            evidence_class=EvidenceClass(str(item["evidence_class"])),
            page=int(item["page"]),
            source_text=str(item["source_text"]),
            confidence=float(item["confidence"]),
            provider=provider,
            model=model,
            bbox=bbox,
            reason=None if item.get("reason") is None else str(item.get("reason")),
            evidence_verified=bool(item.get("evidence_verified")),
        )
        return CandidateEnvelope(
            candidate=candidate,
            document_id=document_id,
            document_type=SpecializedDocumentType(str(item["document_type"])),
            specialist=SpecialistRole(str(item["specialist"])),
            agent_run_id=UUID(int=0),
            line_item_key=None if item.get("line_item_key") is None else str(item.get("line_item_key")),
            source_span_id=None,
        )
    except (KeyError, TypeError, ValueError):
        return None


def _rehydrate_cached_run(
    payloads: tuple[Mapping[str, object], ...],
    *,
    computation_fingerprint: str,
) -> SpecializedShadowRun | None:
    if not payloads:
        return None
    first = payloads[0]
    provider = str(first.get("provider") or "")
    model = str(first.get("model") or "")
    if not provider or not model:
        return None
    candidates: list[CandidateEnvelope] = []
    cache_documents: list[Mapping[str, object]] = []
    for payload in payloads:
        if str(payload.get("provider") or "") != provider or str(payload.get("model") or "") != model:
            return None
        cache_document = payload.get("cache_document")
        if not isinstance(cache_document, Mapping):
            return None
        try:
            document_id = UUID(str(cache_document["document_id"]))
        except (KeyError, TypeError, ValueError):
            return None
        cache_documents.append(cache_document)
        raw_candidates = payload.get("candidates")
        if not isinstance(raw_candidates, list):
            return None
        for raw_candidate in raw_candidates:
            if not isinstance(raw_candidate, Mapping):
                return None
            envelope = _cached_candidate(raw_candidate, document_id=document_id, provider=provider, model=model)
            if envelope is None:
                return None
            candidates.append(envelope)

    conflicts: list[FusionConflict] = []
    raw_conflicts = first.get("fusion_conflicts")
    if isinstance(raw_conflicts, list):
        for item in raw_conflicts:
            if not isinstance(item, Mapping):
                return None
            conflicts.append(
                FusionConflict(
                    key=FusionKey(
                        str(item.get("field_key") or ""),
                        None if item.get("line_item_key") is None else str(item.get("line_item_key")),
                        None if item.get("unbound_identity") is None else str(item.get("unbound_identity")),
                    ),
                    candidates=(),
                    normalized_values=(),
                    requires_ai_resolution=bool(item.get("requires_ai_resolution")),
                )
            )
    judge = _cached_field_judge(first)
    try:
        operation = OperationStatus(str(first.get("operation_status") or "COMPLETED"))
    except ValueError:
        return None
    result = MultiAgentExtractionResult(
        routing_plan=RoutingPlan(()),
        specialist_results=(),
        fused_candidates=tuple(candidates),
        partial_failures=(),
        operation=operation,
        specialist_statuses=(),
        field_judge=judge,
        fusion_conflicts=tuple(conflicts),
    )
    return SpecializedShadowRun(
        result=result,
        provider=provider,
        model=model,
        latency_ms=0,
        input_tokens=0,
        output_tokens=0,
        total_tokens=0,
        computation_fingerprint=computation_fingerprint,
        cache_documents=tuple(cache_documents),
        cache_hit=True,
    )


def _cache_descriptors(documents: tuple[SpecializedShadowDocument, ...]) -> tuple[Mapping[str, object], ...]:
    return tuple(
        {
            "document_id": str(document.document_id),
            "sha256": document.source_sha256,
            "role_hint": document.role_hint or "",
            "filename": document.filename,
        }
        for document in documents
    )


def _try_cache(
    *,
    documents: tuple[SpecializedShadowDocument, ...],
    config: AIProviderConfig,
    judge_mode: FieldJudgeMode,
) -> tuple[str | None, SpecializedShadowRun | None]:
    try:
        organization_id = cache_organization_id(tuple(documents))
        if organization_id is None:
            return None, None
        descriptors = _cache_descriptors(documents)
        engine_versions = ",".join(sorted({document.engine2_resolution.engine_version for document in documents}))
        computation_fingerprint = specialized_computation_fingerprint(
            organization_id=organization_id,
            documents=descriptors,
            engine_version=engine_versions,
            provider=config.provider,
            model=config.model,
            max_pages=config.max_pages,
            judge_mode=judge_mode.value,
            projection_mode=_effective_projection_mode(None).value,
            specialized_schema_version=SPECIALIZED_SHADOW_SCHEMA_VERSION,
            field_judge_version=FIELD_JUDGE_VERSION,
            projection_version=SPECIALIZED_PROJECTION_VERSION,
        )
        payloads = find_cached_specialized_payloads(
            organization_id=organization_id,
            documents=tuple(documents),
            computation_fingerprint=computation_fingerprint,
            schema_version=SPECIALIZED_SHADOW_SCHEMA_VERSION,
        )
        if payloads is None:
            return computation_fingerprint, None
        cached = _rehydrate_cached_run(payloads, computation_fingerprint=computation_fingerprint)
        if cached is not None:
            LOGGER.info("Lacey specialized inference cache hit", extra={"organization_id": organization_id, "computation_fingerprint": computation_fingerprint})
        return computation_fingerprint, cached
    except Exception:
        LOGGER.exception("Lacey specialized inference cache lookup failed safely")
        return None, None


def run_specialized_shadow_operation(
    *,
    documents: tuple[SpecializedShadowDocument, ...],
    provider: ScopedAIExtractionProvider | None = None,
    config: AIProviderConfig | None = None,
    concurrency: int | None = None,
    judge_mode: FieldJudgeMode | str | None = None,
    judge_provider: FieldJudgeProvider | None = None,
) -> SpecializedShadowRun:
    if not documents:
        raise AIShadowError("Specialized shadow requires at least one source document.")

    effective_mode = _effective_judge_mode(judge_mode)
    configured = config
    scoped_provider = provider
    computation_fingerprint: str | None = None
    if scoped_provider is None:
        configured = configured or AIProviderConfig.from_env()
        if configured.provider != "gemini":
            raise AIShadowError("Specialized shadow currently requires the Gemini provider.")
        computation_fingerprint, cached = _try_cache(
            documents=documents,
            config=configured,
            judge_mode=effective_mode,
        )
        if cached is not None:
            return cached
        scoped_provider = GeminiSpecialistProvider(configured)

    candidate_judge = None
    if effective_mode is not FieldJudgeMode.OFF:
        runtime_judge = judge_provider
        construction_error: Exception | None = None
        if runtime_judge is None:
            try:
                configured = configured or AIProviderConfig.from_env()
                runtime_judge = GeminiFieldJudgeProvider(configured)
            except Exception as exc:
                construction_error = exc
        if runtime_judge is not None:
            candidate_judge = lambda candidates: evaluate_field_judge(candidates, provider=runtime_judge, mode=effective_mode)
        elif construction_error is not None:
            def unavailable_judge(candidates):
                del candidates
                raise construction_error
            candidate_judge = unavailable_judge

    routed_documents = []
    inputs: list[SpecialistInputDocument] = []
    resolutions: dict[UUID, DocumentResolution] = {}
    for document in documents:
        routed = route_document(
            document_id=document.document_id,
            page_texts=_page_texts(document.engine2_resolution),
            filename=document.filename,
        )
        resolutions[document.document_id] = document.engine2_resolution
        for routed_document in routed:
            routed_documents.append(routed_document)
            inputs.append(SpecialistInputDocument(routed=routed_document, filename=document.filename, content=document.content))

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
            candidate_verifier=lambda candidates: _verify_candidates(candidates, resolutions=resolutions),
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
        input_tokens=_sum_reported([item.input_tokens for item in result.specialist_results]),
        output_tokens=_sum_reported([item.output_tokens for item in result.specialist_results]),
        total_tokens=_sum_reported([item.total_tokens for item in result.specialist_results]),
        computation_fingerprint=computation_fingerprint,
        cache_documents=_cache_descriptors(documents),
        cache_hit=False,
    )
