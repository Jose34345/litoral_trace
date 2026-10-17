from __future__ import annotations

import importlib
import importlib.util
from uuid import NAMESPACE_URL, uuid4, uuid5

from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.authority import candidate_identity
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    SpecialistRole,
)
from litoral_trace.lacey_engine.multi_agent.fusion import fuse_candidates


class StubResolverProvider:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload
        self.calls: list[dict[str, object]] = []

    def resolve_structured(self, *, prompt: str, schema: dict[str, object]) -> dict[str, object]:
        self.calls.append({"prompt": prompt, "schema": schema})
        return self.payload


def _resolver_module():
    module_name = "litoral_trace.lacey_engine.multi_agent.resolver"
    assert importlib.util.find_spec(module_name) is not None, "Phase 5 resolver module is missing"
    return importlib.import_module(module_name)


def _envelope(
    value: str,
    *,
    document_type: DocumentType,
    confidence: float,
    document_seed: str,
) -> CandidateEnvelope:
    candidate = AICandidate(
        field_key="entered_value",
        value=value,
        normalized_value=value,
        evidence_class=EvidenceClass.EXPLICIT,
        page=1,
        source_text=f"SKU ACT-TRAY-18 entered value {value} USD supporting row",
        confidence=confidence,
        provider="fixture",
        model="fixture",
        evidence_verified=True,
    )
    return CandidateEnvelope(
        candidate=candidate,
        document_id=uuid5(NAMESPACE_URL, document_seed),
        document_type=document_type,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        agent_run_id=uuid4(),
        line_item_key="SKU:ACT-TRAY-18",
        source_span_id=None,
    )


def _real_conflict():
    invoice = _envelope(
        "18900.00",
        document_type=DocumentType.COMMERCIAL_INVOICE,
        confidence=0.80,
        document_seed="invoice",
    )
    entry = _envelope(
        "19000.00",
        document_type=DocumentType.ENTRY_WORKSHEET,
        confidence=0.70,
        document_seed="entry",
    )
    conflict = fuse_candidates((invoice, entry)).conflicts[0]
    assert conflict.requires_ai_resolution is True
    return conflict


def test_valid_candidate_id_resolves_conflict_without_free_value_output():
    resolver = _resolver_module()
    conflict = _real_conflict()
    candidate_ids = [candidate_identity(candidate) for candidate in conflict.candidates]
    selected_id = candidate_ids[-1]
    provider = StubResolverProvider(
        {
            "field_key": conflict.key.field_key,
            "line_item_key": conflict.key.line_item_key,
            "candidate_ids": candidate_ids,
            "decision": "SELECT",
            "selected_candidate_id": selected_id,
            "reason_code": "STRONGER_CONTEXT_MATCH",
        }
    )

    result = resolver.resolve_conflict(conflict, provider=provider)

    assert result.decision == "SELECT"
    assert result.selected_candidate_id == selected_id
    assert result.candidate_ids == candidate_ids
    assert len(provider.calls) == 1

    call = provider.calls[0]
    prompt = str(call["prompt"])
    assert "You CANNOT invent a new value" in prompt
    assert "NEEDS_REVIEW" in prompt
    for candidate, candidate_id in zip(conflict.candidates, candidate_ids, strict=True):
        assert candidate_id in prompt
        assert candidate.candidate.value in prompt
        assert candidate.candidate.source_text in prompt
        assert candidate.document_type.value in prompt

    schema = call["schema"]
    properties = schema["properties"]
    assert set(properties) == {
        "field_key",
        "line_item_key",
        "candidate_ids",
        "decision",
        "selected_candidate_id",
        "reason_code",
    }
    assert "value" not in properties
    assert "new_value" not in properties


def test_hallucinated_candidate_id_fails_safe_to_needs_review():
    resolver = _resolver_module()
    conflict = _real_conflict()
    candidate_ids = [candidate_identity(candidate) for candidate in conflict.candidates]
    provider = StubResolverProvider(
        {
            "field_key": conflict.key.field_key,
            "line_item_key": conflict.key.line_item_key,
            "candidate_ids": candidate_ids,
            "decision": "SELECT",
            "selected_candidate_id": "cand_hallucinated_not_in_pool",
            "reason_code": "STRONGER_CONTEXT_MATCH",
        }
    )

    result = resolver.resolve_conflict(conflict, provider=provider)

    assert result.decision == "NEEDS_REVIEW"
    assert result.selected_candidate_id is None
    assert result.reason_code is None
    assert result.candidate_ids == candidate_ids


def test_extra_free_value_field_is_rejected_and_fails_safe():
    resolver = _resolver_module()
    conflict = _real_conflict()
    candidate_ids = [candidate_identity(candidate) for candidate in conflict.candidates]
    provider = StubResolverProvider(
        {
            "field_key": conflict.key.field_key,
            "line_item_key": conflict.key.line_item_key,
            "candidate_ids": candidate_ids,
            "decision": "SELECT",
            "selected_candidate_id": candidate_ids[0],
            "reason_code": "STRONGER_CONTEXT_MATCH",
            "new_value": "99999.99",
        }
    )

    result = resolver.resolve_conflict(conflict, provider=provider)

    assert result.decision == "NEEDS_REVIEW"
    assert result.selected_candidate_id is None
    assert result.reason_code is None
