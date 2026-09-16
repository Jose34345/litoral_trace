from __future__ import annotations

import json
from uuid import NAMESPACE_URL, uuid5

from litoral_trace.lacey_engine.ai_shadow import (
    AICandidate,
    AIExtractionResult,
    AI_SHADOW_SCHEMA_VERSION,
)
from litoral_trace.lacey_engine.domain import (
    DocumentResolution,
    DocumentType as EngineDocumentType,
    EvidenceClass,
    LayoutBlock,
    ParsedLayout,
)
from litoral_trace.lacey_engine.multi_agent.contracts import OperationStatus, SpecialistRole
from litoral_trace.lacey_engine.multi_agent.field_judge import (
    FIELD_JUDGE_VERSION,
    FieldJudgeDecision,
    FieldJudgeMode,
)
from litoral_trace.us_lacey.specialized_shadow import (
    SPECIALIZED_SHADOW_SCHEMA_VERSION,
    SpecializedShadowDocument,
    run_specialized_shadow_operation,
    serialize_specialized_document_run,
    specialized_engine_version,
)


class FakeSpecialistProvider:
    name = "gemini"
    model = "fixture-specialist-model"

    def extract_scoped(
        self,
        *,
        filename: str,
        content: bytes,
        pages: tuple[int, ...],
        allowed_fields: frozenset[str],
        prompt: str,
    ) -> AIExtractionResult:
        candidates = ()
        if "hts_code" in allowed_fields:
            candidates = (
                AICandidate(
                    field_key="hts_code",
                    value="4419.90.9000",
                    normalized_value="4419.90.9000",
                    evidence_class=EvidenceClass.EXPLICIT,
                    page=1,
                    source_text="SKU-1 HTS 4419.90.9000",
                    confidence=0.96,
                    provider=self.name,
                    model=self.model,
                    evidence_verified=False,
                ),
            )
        return AIExtractionResult(
            provider=self.name,
            model=self.model,
            schema_version=AI_SHADOW_SCHEMA_VERSION,
            candidates=candidates,
            page_count=len(pages),
            latency_ms=23,
            input_tokens=100,
            output_tokens=12,
            total_tokens=112,
        )


class FakeJudgeProvider:
    name = "gemini"
    model = "fixture-judge-model"

    def __init__(
        self,
        *,
        decision: FieldJudgeDecision = FieldJudgeDecision.ACCEPT,
        fail: bool = False,
    ) -> None:
        self.decision = decision
        self.fail = fail
        self.calls = 0

    def judge_structured(self, *, prompt: str, schema: dict[str, object]):
        self.calls += 1
        if self.fail:
            raise RuntimeError("simulated judge outage")
        context = json.loads(prompt.split("CANDIDATE CONTEXT:\n", 1)[1])
        return (
            {
                "decisions": [
                    {
                        "candidate_id": item["candidate_id"],
                        "field_key": item["field_key"],
                        "line_item_key": item["line_item_key"],
                        "decision": self.decision.value,
                        "reason": "EXACT_FIELD_CONTEXT",
                    }
                    for item in context
                ]
            },
            17,
            30,
            5,
            35,
        )


def _resolution() -> DocumentResolution:
    block = LayoutBlock(
        block_id="page-1-row-1",
        page=1,
        bbox=None,
        text="COMMERCIAL INVOICE\nSKU-1 HTS 4419.90.9000",
        block_type="text",
    )
    return DocumentResolution(
        filename="invoice.pdf",
        engine_version="fixture-engine2",
        document_type=EngineDocumentType.COMMERCIAL_INVOICE,
        type_confidence=0.99,
        layout=ParsedLayout(blocks=(block,), page_count=1),
        sections=(),
        fields={},
    )


def _document() -> SpecializedShadowDocument:
    return SpecializedShadowDocument(
        document_id=uuid5(NAMESPACE_URL, "shadow-invoice"),
        operation_document_id=10,
        assurance_document_id=20,
        source_sha256="a" * 64,
        role_hint="COMMERCIAL_INVOICE",
        filename="invoice.pdf",
        content=b"%PDF-fixture",
        engine2_resolution=_resolution(),
    )


def test_specialized_shadow_has_distinct_non_authoritative_schema() -> None:
    assert SPECIALIZED_SHADOW_SCHEMA_VERSION == "lacey_multi_agent_shadow_v2"
    assert SPECIALIZED_SHADOW_SCHEMA_VERSION != AI_SHADOW_SCHEMA_VERSION


def test_specialized_shadow_verifies_evidence_before_fusion_and_aggregates_usage() -> None:
    run = run_specialized_shadow_operation(
        documents=(_document(),),
        provider=FakeSpecialistProvider(),
        concurrency=1,
    )

    assert run.result.operation is OperationStatus.COMPLETED
    assert {item.role for item in run.result.specialist_results} == {
        SpecialistRole.COMMERCIAL_LINES,
        SpecialistRole.CUSTOMS_IDENTITY,
    }
    assert len(run.result.fused_candidates) == 1
    fused = run.result.fused_candidates[0]
    assert fused.candidate.field_key == "hts_code"
    assert fused.candidate.evidence_verified is True
    assert run.provider == "gemini"
    assert run.model == "fixture-specialist-model"
    assert run.input_tokens == 200
    assert run.output_tokens == 24
    assert run.total_tokens == 224
    assert run.latency_ms >= 0


def test_specialized_shadow_keeps_unmatched_evidence_unverified() -> None:
    class WrongSourceProvider(FakeSpecialistProvider):
        def extract_scoped(self, **kwargs) -> AIExtractionResult:
            result = super().extract_scoped(**kwargs)
            if not result.candidates:
                return result
            candidate = result.candidates[0]
            wrong = AICandidate(
                field_key=candidate.field_key,
                value=candidate.value,
                normalized_value=candidate.normalized_value,
                evidence_class=candidate.evidence_class,
                page=candidate.page,
                source_text="THIS TEXT DOES NOT EXIST IN THE DOCUMENT",
                confidence=candidate.confidence,
                provider=candidate.provider,
                model=candidate.model,
                evidence_verified=False,
            )
            return AIExtractionResult(
                provider=result.provider,
                model=result.model,
                schema_version=result.schema_version,
                candidates=(wrong,),
                page_count=result.page_count,
                latency_ms=result.latency_ms,
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                total_tokens=result.total_tokens,
            )

    run = run_specialized_shadow_operation(
        documents=(_document(),),
        provider=WrongSourceProvider(),
        concurrency=1,
    )

    assert run.result.fused_candidates[0].candidate.evidence_verified is False


def test_field_judge_off_never_calls_provider_and_preserves_specialized_fusion() -> None:
    judge = FakeJudgeProvider(decision=FieldJudgeDecision.REJECT)

    run = run_specialized_shadow_operation(
        documents=(_document(),),
        provider=FakeSpecialistProvider(),
        judge_mode=FieldJudgeMode.OFF,
        judge_provider=judge,
        concurrency=1,
    )

    assert judge.calls == 0
    assert len(run.result.fused_candidates) == 1
    assert run.result.field_judge is None


def test_field_judge_shadow_records_reject_without_changing_fusion() -> None:
    judge = FakeJudgeProvider(decision=FieldJudgeDecision.REJECT)

    run = run_specialized_shadow_operation(
        documents=(_document(),),
        provider=FakeSpecialistProvider(),
        judge_mode=FieldJudgeMode.SHADOW,
        judge_provider=judge,
        concurrency=1,
    )

    assert judge.calls == 1
    assert len(run.result.fused_candidates) == 1
    assert run.result.field_judge is not None
    assert run.result.field_judge.mode is FieldJudgeMode.SHADOW
    assert run.result.field_judge.decisions[0].decision is FieldJudgeDecision.REJECT
    assert run.result.field_judge.latency_ms == 17
    assert run.result.field_judge.total_tokens == 35


def test_field_judge_enforce_only_accept_candidates_reach_fusion() -> None:
    rejected = run_specialized_shadow_operation(
        documents=(_document(),),
        provider=FakeSpecialistProvider(),
        judge_mode=FieldJudgeMode.ENFORCE,
        judge_provider=FakeJudgeProvider(decision=FieldJudgeDecision.REJECT),
        concurrency=1,
    )
    accepted = run_specialized_shadow_operation(
        documents=(_document(),),
        provider=FakeSpecialistProvider(),
        judge_mode=FieldJudgeMode.ENFORCE,
        judge_provider=FakeJudgeProvider(decision=FieldJudgeDecision.ACCEPT),
        concurrency=1,
    )

    assert rejected.result.fused_candidates == ()
    assert len(accepted.result.fused_candidates) == 1


def test_field_judge_failure_in_shadow_is_safe_and_keeps_fusion() -> None:
    judge = FakeJudgeProvider(fail=True)

    run = run_specialized_shadow_operation(
        documents=(_document(),),
        provider=FakeSpecialistProvider(),
        judge_mode=FieldJudgeMode.SHADOW,
        judge_provider=judge,
        concurrency=1,
    )

    assert len(run.result.fused_candidates) == 1
    assert run.result.field_judge is not None
    assert run.result.field_judge.safe_error == "simulated judge outage"
    assert run.result.field_judge.decisions[0].decision is FieldJudgeDecision.NEEDS_REVIEW


def test_specialized_serialization_persists_bounded_field_judge_telemetry() -> None:
    run = run_specialized_shadow_operation(
        documents=(_document(),),
        provider=FakeSpecialistProvider(),
        judge_mode=FieldJudgeMode.SHADOW,
        judge_provider=FakeJudgeProvider(decision=FieldJudgeDecision.ACCEPT),
        concurrency=1,
    )

    payload = serialize_specialized_document_run(
        run=run,
        document_id=_document().document_id,
        source_set_fingerprint="source-set-fixture",
    )

    judge_payload = payload["field_judge"]
    assert judge_payload["version"] == FIELD_JUDGE_VERSION
    assert judge_payload["mode"] == "shadow"
    assert judge_payload["provider"] == "gemini"
    assert judge_payload["model"] == "fixture-judge-model"
    assert judge_payload["latency_ms"] == 17
    assert judge_payload["input_tokens"] == 30
    assert judge_payload["output_tokens"] == 5
    assert judge_payload["total_tokens"] == 35
    assert judge_payload["accepted_count"] == 1
    assert judge_payload["rejected_count"] == 0
    assert judge_payload["needs_review_count"] == 0
    assert judge_payload["decisions"] == [
        {
            "candidate_id": run.result.field_judge.decisions[0].candidate_id,
            "field_key": "hts_code",
            "line_item_key": run.result.field_judge.decisions[0].line_item_key,
            "decision": "ACCEPT",
            "reason": "EXACT_FIELD_CONTEXT",
        }
    ]
    assert "value" not in judge_payload["decisions"][0]
    assert "normalized_value" not in judge_payload["decisions"][0]


def test_specialized_engine_identity_changes_with_effective_judge_mode() -> None:
    common = {
        "provider": "gemini",
        "model": "fixture-specialist-model",
        "source_set_fingerprint": "source-set-fixture",
    }

    off = specialized_engine_version(**common, judge_mode=FieldJudgeMode.OFF)
    shadow = specialized_engine_version(**common, judge_mode=FieldJudgeMode.SHADOW)
    enforce = specialized_engine_version(**common, judge_mode=FieldJudgeMode.ENFORCE)

    assert len({off, shadow, enforce}) == 3