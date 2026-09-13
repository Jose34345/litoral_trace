from __future__ import annotations

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
from litoral_trace.us_lacey.specialized_shadow import (
    SPECIALIZED_SHADOW_SCHEMA_VERSION,
    SpecializedShadowDocument,
    run_specialized_shadow_operation,
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
    assert SPECIALIZED_SHADOW_SCHEMA_VERSION == "lacey_multi_agent_shadow_v1"
    assert SPECIALIZED_SHADOW_SCHEMA_VERSION != AI_SHADOW_SCHEMA_VERSION


def test_specialized_shadow_verifies_evidence_before_fusion_and_aggregates_usage() -> None:
    run = run_specialized_shadow_operation(
        documents=(_document(),),
        provider=FakeSpecialistProvider(),
        concurrency=1,
    )

    assert run.result.operation is OperationStatus.COMPLETED
    assert {item.role for item in run.result.specialist_results} == {
        SpecialistRole.COMMERCIAL_LINES
    }
    assert len(run.result.fused_candidates) == 1
    fused = run.result.fused_candidates[0]
    assert fused.candidate.field_key == "hts_code"
    assert fused.candidate.evidence_verified is True
    assert run.provider == "gemini"
    assert run.model == "fixture-specialist-model"
    assert run.input_tokens == 100
    assert run.output_tokens == 12
    assert run.total_tokens == 112
    assert run.latency_ms >= 0


def test_specialized_shadow_keeps_unmatched_evidence_unverified() -> None:
    class WrongSourceProvider(FakeSpecialistProvider):
        def extract_scoped(self, **kwargs) -> AIExtractionResult:
            result = super().extract_scoped(**kwargs)
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
