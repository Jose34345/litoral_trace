from __future__ import annotations

from dataclasses import FrozenInstanceError, fields
from uuid import uuid4

import pytest

from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import (
    AgentTask,
    CandidateEnvelope,
    DocumentType,
    MultiAgentExtractionResult,
    RoutedDocument,
    SpecialistFailure,
    SpecialistResult,
    SpecialistRole,
)


def _candidate() -> AICandidate:
    return AICandidate(
        field_key="hts_code",
        value="9403.60.8081",
        normalized_value="9403608081",
        evidence_class=EvidenceClass.EXPLICIT,
        page=1,
        source_text="HTS 9403.60.8081",
        confidence=0.97,
        provider="gemini",
        model="fixture-model",
        reason="phase-0 fixture",
        evidence_verified=True,
    )


def _routed_document() -> RoutedDocument:
    return RoutedDocument(
        document_id=uuid4(),
        document_type=DocumentType.ENTRY_WORKSHEET,
        pages=(1, 2),
        confidence=0.99,
        signals=("entry number", "HTS"),
    )


def test_document_type_contract_has_exact_phase_zero_values():
    assert tuple(item.value for item in DocumentType) == (
        "COMMERCIAL_INVOICE",
        "ENTRY_WORKSHEET",
        "BILL_OF_LADING",
        "ARRIVAL_NOTICE",
        "BOTANICAL_DECLARATION",
        "SUPPLIER_ORIGIN",
        "PACKING_LIST",
        "UNKNOWN",
    )


def test_specialist_role_contract_has_exact_phase_zero_values():
    assert tuple(item.value for item in SpecialistRole) == (
        "CUSTOMS_IDENTITY",
        "LOGISTICS",
        "COMMERCIAL_LINES",
        "BOTANICAL",
    )


def test_candidate_envelope_wraps_existing_ai_candidate_without_mutating_it():
    candidate = _candidate()
    document_id = uuid4()
    agent_run_id = uuid4()

    envelope = CandidateEnvelope(
        candidate=candidate,
        document_id=document_id,
        document_type=DocumentType.ENTRY_WORKSHEET,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        agent_run_id=agent_run_id,
        line_item_key="LINE-1",
        source_span_id=None,
    )

    assert envelope.candidate is candidate
    assert candidate.field_key == "hts_code"
    assert candidate.value == "9403.60.8081"
    assert [field.name for field in fields(CandidateEnvelope)] == [
        "candidate",
        "document_id",
        "document_type",
        "specialist",
        "agent_run_id",
        "line_item_key",
        "source_span_id",
    ]


def test_contract_dataclasses_are_frozen_and_slotted():
    routed = _routed_document()
    task = AgentTask(
        role=SpecialistRole.CUSTOMS_IDENTITY,
        documents=(routed,),
        allowed_fields=frozenset({"importer_name", "filing_entry_reference"}),
    )

    assert not hasattr(routed, "__dict__")
    assert not hasattr(task, "__dict__")
    assert isinstance(task.documents, tuple)
    assert isinstance(task.allowed_fields, frozenset)

    with pytest.raises(FrozenInstanceError):
        routed.confidence = 0.1  # type: ignore[misc]

    with pytest.raises(FrozenInstanceError):
        task.allowed_fields = frozenset()  # type: ignore[misc]


def test_top_level_result_preserves_partial_failure_without_collapsing_successes():
    routed = _routed_document()
    candidate = _candidate()
    envelope = CandidateEnvelope(
        candidate=candidate,
        document_id=routed.document_id,
        document_type=routed.document_type,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        agent_run_id=uuid4(),
        line_item_key="LINE-1",
        source_span_id=None,
    )
    specialist_result = SpecialistResult(
        role=SpecialistRole.COMMERCIAL_LINES,
        candidates=(envelope,),
        provider="gemini",
        model="fixture-model",
        latency_ms=125,
        warnings=(),
    )
    botanical_failure = SpecialistFailure(
        role=SpecialistRole.BOTANICAL,
        error="fixture provider failure",
    )
    routing_plan = object()

    result = MultiAgentExtractionResult(
        routing_plan=routing_plan,
        specialist_results=(specialist_result,),
        fused_candidates=(envelope,),
        partial_failures=(botanical_failure,),
    )

    assert result.routing_plan is routing_plan
    assert result.specialist_results == (specialist_result,)
    assert result.fused_candidates == (envelope,)
    assert result.partial_failures == (botanical_failure,)
