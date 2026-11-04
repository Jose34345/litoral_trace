from __future__ import annotations

import asyncio
from uuid import NAMESPACE_URL, uuid5

from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    RoutedDocument,
    SpecialistRole,
    SpecialistResult,
)
from litoral_trace.lacey_engine.multi_agent.orchestrator import orchestrate_specialists
from litoral_trace.lacey_engine.multi_agent.router import RoutingAssignment, RoutingPlan
from litoral_trace.lacey_engine.multi_agent.specialist_runtime import SpecialistInputDocument


class _ConflictingBolExtractor:
    role = SpecialistRole.LOGISTICS

    def extract(self, documents: tuple[SpecialistInputDocument, ...]) -> SpecialistResult:
        values = ("MAEU274342495", "OOLU1234567890")
        candidates = []
        for source, value in zip(documents, values, strict=True):
            candidate = AICandidate(
                field_key="bill_of_lading",
                value=value,
                normalized_value=value,
                evidence_class=EvidenceClass.EXPLICIT,
                page=1,
                source_text=f"Master B/L: {value}",
                confidence=0.99,
                provider="fixture",
                model="fixture",
                evidence_verified=True,
            )
            candidates.append(
                CandidateEnvelope(
                    candidate=candidate,
                    document_id=source.routed.document_id,
                    document_type=DocumentType.BILL_OF_LADING,
                    specialist=self.role,
                    agent_run_id=uuid5(NAMESPACE_URL, f"conflict-run-{value}"),
                    line_item_key=None,
                    source_span_id=None,
                )
            )
        return SpecialistResult(
            role=self.role,
            candidates=tuple(candidates),
            provider="fixture",
            model="fixture",
            latency_ms=1,
            warnings=(),
        )


def _source(index: int) -> tuple[RoutingAssignment, SpecialistInputDocument]:
    routed = RoutedDocument(
        document_id=uuid5(NAMESPACE_URL, f"conflicting-bol-document-{index}"),
        document_type=DocumentType.BILL_OF_LADING,
        pages=(1,),
        confidence=0.99,
        signals=("fixture",),
    )
    return (
        RoutingAssignment(document=routed, specialists=(SpecialistRole.LOGISTICS,)),
        SpecialistInputDocument(
            routed=routed,
            filename=f"bill-{index}.pdf",
            content=b"fixture",
        ),
    )


def test_orchestrator_exposes_fusion_conflicts_to_downstream_safety_gates() -> None:
    first_assignment, first_document = _source(1)
    second_assignment, second_document = _source(2)
    result = asyncio.run(
        orchestrate_specialists(
            routing_plan=RoutingPlan((first_assignment, second_assignment)),
            documents=(first_document, second_document),
            extractors={SpecialistRole.LOGISTICS: _ConflictingBolExtractor()},
            concurrency=1,
        )
    )

    assert len(result.fused_candidates) == 1
    assert len(result.fusion_conflicts) == 1
    conflict = result.fusion_conflicts[0]
    assert conflict.key.field_key == "bill_of_lading"
    assert conflict.key.line_item_key is None
    assert set(conflict.normalized_values) == {"MAEU274342495", "OOLU1234567890"}
    assert conflict.requires_ai_resolution is True
