from __future__ import annotations

import asyncio
from uuid import UUID

from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    RoutedDocument,
    SpecialistResult,
    SpecialistRole,
)
from litoral_trace.lacey_engine.multi_agent.field_judge import (
    FieldJudgeDecision,
    FieldJudgeDecisionRecord,
    FieldJudgeEvaluation,
    FieldJudgeMode,
    FieldJudgeReason,
    candidate_identity,
)
from litoral_trace.lacey_engine.multi_agent.orchestrator import orchestrate_specialists
from litoral_trace.lacey_engine.multi_agent.router import RoutingAssignment, RoutingPlan
from litoral_trace.lacey_engine.multi_agent.specialist_runtime import SpecialistInputDocument


class OneCandidateExtractor:
    role = SpecialistRole.LOGISTICS

    def extract(self, documents: tuple[SpecialistInputDocument, ...]) -> SpecialistResult:
        routed = documents[0].routed
        envelope = CandidateEnvelope(
            candidate=AICandidate(
                field_key="bill_of_lading",
                value="OOLU1234567",
                normalized_value="OOLU1234567",
                evidence_class=EvidenceClass.EXPLICIT,
                page=1,
                source_text="B/L OOLU1234567",
                confidence=0.99,
                provider="fixture",
                model="fixture",
                evidence_verified=True,
            ),
            document_id=routed.document_id,
            document_type=routed.document_type,
            specialist=self.role,
            agent_run_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            line_item_key=None,
            source_span_id=None,
        )
        return SpecialistResult(
            role=self.role,
            candidates=(envelope,),
            provider="fixture",
            model="fixture",
            latency_ms=1,
            warnings=(),
        )


def _packet():
    routed = RoutedDocument(
        document_id=UUID("11111111-1111-1111-1111-111111111111"),
        document_type=DocumentType.BILL_OF_LADING,
        pages=(1,),
        confidence=0.99,
        signals=("fixture",),
    )
    return (
        RoutingPlan((RoutingAssignment(document=routed, specialists=(SpecialistRole.LOGISTICS,)),)),
        (SpecialistInputDocument(routed=routed, filename="bol.pdf", content=b"fixture"),),
    )


def _reject(candidates: tuple[CandidateEnvelope, ...]) -> FieldJudgeEvaluation:
    assert len(candidates) == 1
    envelope = candidates[0]
    return FieldJudgeEvaluation(
        version="lacey_field_judge_v1",
        mode=FieldJudgeMode.ENFORCE,
        provider="fixture",
        model="fixture",
        latency_ms=1,
        input_tokens=None,
        output_tokens=None,
        total_tokens=None,
        decisions=(
            FieldJudgeDecisionRecord(
                candidate_id=candidate_identity(envelope),
                field_key=envelope.candidate.field_key,
                line_item_key=envelope.line_item_key,
                decision=FieldJudgeDecision.REJECT,
                reason=FieldJudgeReason.FIELD_MISMATCH,
            ),
        ),
    )


def test_off_mode_does_not_call_judge() -> None:
    routing_plan, documents = _packet()
    calls = []

    def judge(candidates):
        calls.append(candidates)
        return _reject(candidates)

    result = asyncio.run(
        orchestrate_specialists(
            routing_plan=routing_plan,
            documents=documents,
            extractors={SpecialistRole.LOGISTICS: OneCandidateExtractor()},
            concurrency=1,
            field_judge_mode=FieldJudgeMode.OFF,
            candidate_judge=judge,
        )
    )

    assert calls == []
    assert len(result.fused_candidates) == 1
    assert result.field_judge is None


def test_shadow_mode_records_reject_but_does_not_change_fusion() -> None:
    routing_plan, documents = _packet()

    result = asyncio.run(
        orchestrate_specialists(
            routing_plan=routing_plan,
            documents=documents,
            extractors={SpecialistRole.LOGISTICS: OneCandidateExtractor()},
            concurrency=1,
            field_judge_mode=FieldJudgeMode.SHADOW,
            candidate_judge=_reject,
        )
    )

    assert result.field_judge is not None
    assert result.field_judge.decisions[0].decision is FieldJudgeDecision.REJECT
    assert len(result.fused_candidates) == 1


def test_enforce_mode_withholds_reject_from_safe_fusion() -> None:
    routing_plan, documents = _packet()

    result = asyncio.run(
        orchestrate_specialists(
            routing_plan=routing_plan,
            documents=documents,
            extractors={SpecialistRole.LOGISTICS: OneCandidateExtractor()},
            concurrency=1,
            field_judge_mode=FieldJudgeMode.ENFORCE,
            candidate_judge=_reject,
        )
    )

    assert result.field_judge is not None
    assert result.field_judge.decisions[0].decision is FieldJudgeDecision.REJECT
    assert result.fused_candidates == ()
