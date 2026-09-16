from __future__ import annotations

import asyncio
from dataclasses import replace
import threading
import time
from uuid import NAMESPACE_URL, uuid5

from litoral_trace.lacey_engine.ai_shadow import AICandidate, AIShadowError
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    OperationStatus,
    RoutedDocument,
    SpecialistExecutionState,
    SpecialistRole,
    SpecialistResult,
)
from litoral_trace.lacey_engine.multi_agent.orchestrator import (
    orchestrate_specialists,
    run_specialist,
    specialist_concurrency_limit,
)
from litoral_trace.lacey_engine.multi_agent.router import RoutingAssignment, RoutingPlan
from litoral_trace.lacey_engine.multi_agent.specialist_runtime import SpecialistInputDocument


class SuccessfulExtractor:
    def __init__(self, role: SpecialistRole) -> None:
        self.role = role
        self.thread_ids: list[int] = []

    def extract(self, documents: tuple[SpecialistInputDocument, ...]) -> SpecialistResult:
        self.thread_ids.append(threading.get_ident())
        return SpecialistResult(
            role=self.role,
            candidates=(),
            provider="fixture",
            model="fixture",
            latency_ms=1,
            warnings=(),
        )


class FailingExtractor:
    def __init__(self, role: SpecialistRole) -> None:
        self.role = role
        self.calls = 0

    def extract(self, documents: tuple[SpecialistInputDocument, ...]) -> SpecialistResult:
        self.calls += 1
        raise AIShadowError("simulated botanical provider exhaustion")


class SlowExtractor(SuccessfulExtractor):
    def __init__(
        self,
        role: SpecialistRole,
        *,
        lock: threading.Lock,
        active: list[int],
        max_active: list[int],
    ) -> None:
        super().__init__(role)
        self.lock = lock
        self.active = active
        self.max_active = max_active

    def extract(self, documents: tuple[SpecialistInputDocument, ...]) -> SpecialistResult:
        self.thread_ids.append(threading.get_ident())
        with self.lock:
            self.active[0] += 1
            self.max_active[0] = max(self.max_active[0], self.active[0])
        try:
            time.sleep(0.08)
            return SpecialistResult(
                role=self.role,
                candidates=(),
                provider="fixture",
                model="fixture",
                latency_ms=80,
                warnings=(),
            )
        finally:
            with self.lock:
                self.active[0] -= 1


class CandidateExtractor(SuccessfulExtractor):
    def extract(self, documents: tuple[SpecialistInputDocument, ...]) -> SpecialistResult:
        source = documents[0]
        candidate = AICandidate(
            field_key="hts_code",
            value="4419.90.9000",
            normalized_value="4419.90.9000",
            evidence_class=EvidenceClass.EXPLICIT,
            page=1,
            source_text="SKU-1 HTS 4419.90.9000",
            confidence=0.95,
            provider="fixture",
            model="fixture",
            evidence_verified=False,
        )
        envelope = CandidateEnvelope(
            candidate=candidate,
            document_id=source.routed.document_id,
            document_type=source.routed.document_type,
            specialist=self.role,
            agent_run_id=uuid5(NAMESPACE_URL, "phase6-verifier-run"),
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


def _source(role: SpecialistRole, index: int) -> tuple[RoutingAssignment, SpecialistInputDocument]:
    document_type = {
        SpecialistRole.CUSTOMS_IDENTITY: DocumentType.ENTRY_WORKSHEET,
        SpecialistRole.LOGISTICS: DocumentType.BILL_OF_LADING,
        SpecialistRole.COMMERCIAL_LINES: DocumentType.COMMERCIAL_INVOICE,
        SpecialistRole.BOTANICAL: DocumentType.BOTANICAL_DECLARATION,
    }[role]
    routed = RoutedDocument(
        document_id=uuid5(NAMESPACE_URL, f"phase6-{role.value}-{index}"),
        document_type=document_type,
        pages=(1,),
        confidence=0.99,
        signals=("fixture",),
    )
    return (
        RoutingAssignment(document=routed, specialists=(role,)),
        SpecialistInputDocument(
            routed=routed,
            filename=f"{role.value.lower()}.pdf",
            content=b"fixture",
        ),
    )


def _packet():
    roles = (
        SpecialistRole.CUSTOMS_IDENTITY,
        SpecialistRole.LOGISTICS,
        SpecialistRole.COMMERCIAL_LINES,
        SpecialistRole.BOTANICAL,
    )
    pairs = [_source(role, index) for index, role in enumerate(roles, start=1)]
    return RoutingPlan(tuple(pair[0] for pair in pairs)), tuple(pair[1] for pair in pairs)


def test_specialist_concurrency_defaults_to_two_and_never_exceeds_three(monkeypatch):
    monkeypatch.delenv("LT_AI_SPECIALIST_CONCURRENCY", raising=False)
    assert specialist_concurrency_limit() == 2

    monkeypatch.setenv("LT_AI_SPECIALIST_CONCURRENCY", "99")
    assert specialist_concurrency_limit() == 3

    monkeypatch.setenv("LT_AI_SPECIALIST_CONCURRENCY", "0")
    assert specialist_concurrency_limit() == 1

    monkeypatch.setenv("LT_AI_SPECIALIST_CONCURRENCY", "not-an-int")
    assert specialist_concurrency_limit() == 2


def test_run_specialist_executes_sync_extract_off_main_thread():
    extractor = SuccessfulExtractor(SpecialistRole.CUSTOMS_IDENTITY)
    _, source = _source(SpecialistRole.CUSTOMS_IDENTITY, 1)
    main_thread = threading.get_ident()

    async def scenario():
        return await run_specialist(
            extractor,
            (source,),
            semaphore=asyncio.Semaphore(1),
        )

    result = asyncio.run(scenario())

    assert isinstance(result, SpecialistResult)
    assert extractor.thread_ids
    assert extractor.thread_ids[0] != main_thread


def test_run_specialist_converts_ai_shadow_error_to_failure():
    extractor = FailingExtractor(SpecialistRole.BOTANICAL)
    _, source = _source(SpecialistRole.BOTANICAL, 1)

    async def scenario():
        return await run_specialist(
            extractor,
            (source,),
            semaphore=asyncio.Semaphore(1),
        )

    result = asyncio.run(scenario())

    assert result.role is SpecialistRole.BOTANICAL
    assert result.error == "simulated botanical provider exhaustion"
    assert extractor.calls == 1


def test_chaos_botanical_failure_yields_partial_without_killing_other_specialists():
    routing_plan, documents = _packet()
    extractors = {
        SpecialistRole.CUSTOMS_IDENTITY: SuccessfulExtractor(SpecialistRole.CUSTOMS_IDENTITY),
        SpecialistRole.LOGISTICS: SuccessfulExtractor(SpecialistRole.LOGISTICS),
        SpecialistRole.COMMERCIAL_LINES: SuccessfulExtractor(SpecialistRole.COMMERCIAL_LINES),
        SpecialistRole.BOTANICAL: FailingExtractor(SpecialistRole.BOTANICAL),
    }

    result = asyncio.run(
        orchestrate_specialists(
            routing_plan=routing_plan,
            documents=documents,
            extractors=extractors,
            concurrency=2,
        )
    )

    assert result.operation is OperationStatus.PARTIAL
    assert {item.role for item in result.specialist_results} == {
        SpecialistRole.CUSTOMS_IDENTITY,
        SpecialistRole.LOGISTICS,
        SpecialistRole.COMMERCIAL_LINES,
    }
    assert len(result.partial_failures) == 1
    assert result.partial_failures[0].role is SpecialistRole.BOTANICAL

    statuses = {item.role: item.status for item in result.specialist_statuses}
    assert statuses == {
        SpecialistRole.CUSTOMS_IDENTITY: SpecialistExecutionState.COMPLETED,
        SpecialistRole.LOGISTICS: SpecialistExecutionState.COMPLETED,
        SpecialistRole.COMMERCIAL_LINES: SpecialistExecutionState.COMPLETED,
        SpecialistRole.BOTANICAL: SpecialistExecutionState.FAILED,
    }


def test_orchestrator_respects_explicit_semaphore_limit():
    routing_plan, documents = _packet()
    lock = threading.Lock()
    active = [0]
    max_active = [0]
    extractors = {
        role: SlowExtractor(role, lock=lock, active=active, max_active=max_active)
        for role in SpecialistRole
    }

    result = asyncio.run(
        orchestrate_specialists(
            routing_plan=routing_plan,
            documents=documents,
            extractors=extractors,
            concurrency=2,
        )
    )

    assert result.operation is OperationStatus.COMPLETED
    assert max_active[0] == 2


def test_orchestrator_applies_verifier_before_line_binding_and_fusion():
    assignment, source = _source(SpecialistRole.COMMERCIAL_LINES, 1)
    routing_plan = RoutingPlan((assignment,))
    seen = []

    def verifier(candidates: tuple[CandidateEnvelope, ...]) -> tuple[CandidateEnvelope, ...]:
        assert len(candidates) == 1
        assert candidates[0].candidate.evidence_verified is False
        seen.append(candidates[0].candidate.source_text)
        return (
            replace(
                candidates[0],
                candidate=replace(candidates[0].candidate, evidence_verified=True),
            ),
        )

    result = asyncio.run(
        orchestrate_specialists(
            routing_plan=routing_plan,
            documents=(source,),
            extractors={
                SpecialistRole.COMMERCIAL_LINES: CandidateExtractor(
                    SpecialistRole.COMMERCIAL_LINES
                )
            },
            concurrency=1,
            candidate_verifier=verifier,
        )
    )

    assert seen == ["SKU-1 HTS 4419.90.9000"]
    assert len(result.fused_candidates) == 1
    assert result.fused_candidates[0].candidate.evidence_verified is True
