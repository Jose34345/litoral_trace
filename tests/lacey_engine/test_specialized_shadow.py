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
from litoral_trace.lacey_engine.multi_agent.authority import (
    candidate_identity as admission_candidate_identity,
)
from litoral_trace.lacey_engine.multi_agent.candidate_admission import (
    CANDIDATE_ADMISSION_VERSION,
    CandidateAdmissionDecision,
    CandidateAdmissionReason,
)
from litoral_trace.lacey_engine.multi_agent.contracts import OperationStatus, SpecialistRole
from litoral_trace.lacey_engine.multi_agent.field_judge import (
    FIELD_JUDGE_VERSION,
    FieldJudgeDecision,
    FieldJudgeMode,
)
from litoral_trace.us_lacey.specialized_taxonomy import SPECIALIZED_TAXONOMY_VERSION
from litoral_trace.us_lacey.specialized_shadow import (
    SPECIALIZED_SHADOW_SCHEMA_VERSION,
    SpecializedShadowDocument,
    _rehydrate_cached_run,
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
    assert SPECIALIZED_SHADOW_SCHEMA_VERSION == "lacey_multi_agent_shadow_v7"
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
    assert run.result.candidate_admission is not None
    assert run.result.candidate_admission.admitted_count == 1
    assert run.result.candidate_admission.blocked_count == 0
    assert run.provider == "gemini"
    assert run.model == "fixture-specialist-model"
    assert run.input_tokens == 200
    assert run.output_tokens == 24
    assert run.total_tokens == 224
    assert run.latency_ms >= 0


def test_specialized_shadow_blocks_unmatched_evidence_before_fusion() -> None:
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

    assert run.result.fused_candidates == ()
    assert run.result.candidate_admission is not None
    assert run.result.candidate_admission.admitted_count == 0
    assert run.result.candidate_admission.blocked_count == 1
    record = run.result.candidate_admission.records[0]
    assert record.decision is CandidateAdmissionDecision.BLOCKED
    assert record.reason is CandidateAdmissionReason.EVIDENCE_UNVERIFIED


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

    admission_payload = payload["candidate_admission"]
    assert admission_payload["version"] == CANDIDATE_ADMISSION_VERSION
    assert admission_payload["admitted_count"] == 1
    assert admission_payload["blocked_count"] == 0
    assert admission_payload["records"][0]["decision"] == "ADMITTED"
    assert admission_payload["records"][0]["reason"] == "VERIFIED_SUPPORTED"
    assert "value" not in admission_payload["records"][0]
    assert "normalized_value" not in admission_payload["records"][0]


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
    assert off.startswith("multi-agent-v7:")

class EmptySpecialistProvider:
    name = "gemini"
    model = "fixture-empty-specialist-model"

    def extract_scoped(
        self,
        *,
        filename: str,
        content: bytes,
        pages: tuple[int, ...],
        allowed_fields: frozenset[str],
        prompt: str,
    ) -> AIExtractionResult:
        return AIExtractionResult(
            provider=self.name,
            model=self.model,
            schema_version=AI_SHADOW_SCHEMA_VERSION,
            candidates=(),
            page_count=len(pages),
            latency_ms=1,
            input_tokens=1,
            output_tokens=0,
            total_tokens=1,
        )


def _mixed_resolution() -> DocumentResolution:
    blocks = (
        LayoutBlock(
            block_id="page-1-row-1",
            page=1,
            bbox=None,
            text="COMMERCIAL INVOICE\nInvoice No. INV-1\nCommercial Line Items\nEntered Value",
            block_type="text",
        ),
        LayoutBlock(
            block_id="page-2-row-1",
            page=2,
            bbox=None,
            text="PACKING LIST\nPackage Detail\nCartons Pieces\nNet Wt. Gross Wt.",
            block_type="text",
        ),
    )
    return DocumentResolution(
        filename="mixed-packet.pdf",
        engine_version="fixture-engine2",
        document_type=EngineDocumentType.COMMERCIAL_INVOICE,
        type_confidence=0.99,
        layout=ParsedLayout(blocks=blocks, page_count=2),
        sections=(),
        fields={},
    )


def _mixed_document() -> SpecializedShadowDocument:
    return SpecializedShadowDocument(
        document_id=uuid5(NAMESPACE_URL, "shadow-mixed-packet"),
        operation_document_id=11,
        assurance_document_id=21,
        source_sha256="b" * 64,
        role_hint="SUPPORTING_DOCUMENT",
        filename="mixed-packet.pdf",
        content=b"%PDF-mixed-fixture",
        engine2_resolution=_mixed_resolution(),
    )


def test_specialized_serialization_persists_document_routing_without_candidates() -> None:
    document = _mixed_document()
    run = run_specialized_shadow_operation(
        documents=(document,),
        provider=EmptySpecialistProvider(),
        concurrency=1,
    )

    payload = serialize_specialized_document_run(
        run=run,
        document_id=document.document_id,
        source_set_fingerprint="source-set-routing-fixture",
    )

    assert payload["candidate_count"] == 0
    assert payload["candidate_admission"] == {
        "version": CANDIDATE_ADMISSION_VERSION,
        "admitted_count": 0,
        "blocked_count": 0,
        "records": [],
    }
    assert payload["routing_plan"] == [
        {
            "document_type": "COMMERCIAL_INVOICE",
            "pages": [1],
            "confidence": run.result.routing_plan.documents[0].confidence,
            "signals": list(run.result.routing_plan.documents[0].signals),
            "specialists": ["COMMERCIAL_LINES", "CUSTOMS_IDENTITY"],
        },
        {
            "document_type": "PACKING_LIST",
            "pages": [2],
            "confidence": run.result.routing_plan.documents[1].confidence,
            "signals": list(run.result.routing_plan.documents[1].signals),
            "specialists": ["COMMERCIAL_LINES"],
        },
    ]
    assert payload["cache_document"]["document_id"] == str(document.document_id)


def test_specialized_cache_rehydrates_document_routing_plan() -> None:
    document = _mixed_document()
    run = run_specialized_shadow_operation(
        documents=(document,),
        provider=EmptySpecialistProvider(),
        concurrency=1,
    )
    payload = serialize_specialized_document_run(
        run=run,
        document_id=document.document_id,
        source_set_fingerprint="source-set-routing-fixture",
    )

    cached = _rehydrate_cached_run(
        (payload,),
        documents=(document,),
        computation_fingerprint="routing-cache-fixture",
    )

    assert cached is not None
    assert cached.result.candidate_admission is not None
    assert cached.result.candidate_admission.admitted_count == 0
    assert cached.result.candidate_admission.blocked_count == 0
    assert [
        (item.document_type.value, item.pages)
        for item in cached.result.routing_plan.documents
    ] == [
        ("COMMERCIAL_INVOICE", (1,)),
        ("PACKING_LIST", (2,)),
    ]
    assert [
        tuple(role.value for role in cached.result.routing_plan.specialists_for(item))
        for item in cached.result.routing_plan.documents
    ] == [
        ("COMMERCIAL_LINES", "CUSTOMS_IDENTITY"),
        ("COMMERCIAL_LINES",),
    ]



def test_specialized_cache_rebinds_admission_candidate_identity_to_current_document() -> None:
    original = _document()
    run = run_specialized_shadow_operation(
        documents=(original,),
        provider=FakeSpecialistProvider(),
        concurrency=1,
    )
    payload = serialize_specialized_document_run(
        run=run,
        document_id=original.document_id,
        source_set_fingerprint="source-set-admission-rebind-fixture",
    )
    old_candidate_id = payload["candidate_admission"]["records"][0]["candidate_id"]

    rebound_document = SpecializedShadowDocument(
        document_id=uuid5(NAMESPACE_URL, "shadow-invoice-rebound"),
        operation_document_id=110,
        assurance_document_id=120,
        source_sha256=original.source_sha256,
        role_hint=original.role_hint,
        filename=original.filename,
        content=original.content,
        engine2_resolution=original.engine2_resolution,
    )
    cached = _rehydrate_cached_run(
        (payload,),
        documents=(rebound_document,),
        computation_fingerprint="admission-rebind-cache-fixture",
    )

    assert cached is not None
    assert len(cached.result.fused_candidates) == 1
    assert cached.result.candidate_admission is not None
    record = cached.result.candidate_admission.records[0]
    rebound_candidate = cached.result.fused_candidates[0]
    assert record.document_id == rebound_document.document_id
    assert record.candidate_id == admission_candidate_identity(rebound_candidate)
    assert record.candidate_id != old_candidate_id


def test_specialized_cache_fails_closed_when_admission_record_cannot_be_rebound() -> None:
    original = _document()
    run = run_specialized_shadow_operation(
        documents=(original,),
        provider=FakeSpecialistProvider(),
        concurrency=1,
    )
    payload = serialize_specialized_document_run(
        run=run,
        document_id=original.document_id,
        source_set_fingerprint="source-set-admission-unmatched-fixture",
    )
    payload["candidates"] = []
    payload["candidate_count"] = 0
    payload["operation_candidate_count"] = 0
    payload["candidate_admission"]["admitted_count"] = 0
    payload["candidate_admission"]["blocked_count"] = 1
    payload["candidate_admission"]["records"][0]["decision"] = "BLOCKED"
    payload["candidate_admission"]["records"][0]["reason"] = "INVALID_VALUE"

    rebound_document = SpecializedShadowDocument(
        document_id=uuid5(NAMESPACE_URL, "shadow-invoice-unmatched-rebound"),
        operation_document_id=210,
        assurance_document_id=220,
        source_sha256=original.source_sha256,
        role_hint=original.role_hint,
        filename=original.filename,
        content=original.content,
        engine2_resolution=original.engine2_resolution,
    )

    assert (
        _rehydrate_cached_run(
            (payload,),
            documents=(rebound_document,),
            computation_fingerprint="admission-unmatched-cache-fixture",
        )
        is None
    )


def test_specialized_cache_rebinds_document_local_conflict_scope() -> None:
    original = _mixed_document()
    run = run_specialized_shadow_operation(
        documents=(original,),
        provider=EmptySpecialistProvider(),
        concurrency=1,
    )
    payload = serialize_specialized_document_run(
        run=run,
        document_id=original.document_id,
        source_set_fingerprint="source-set-local-conflict-rebind",
    )
    payload["fusion_conflicts"] = [
        {
            "field_key": "hts_code",
            "line_item_key": "LINE:1",
            "unbound_identity": str(original.document_id),
            "requires_ai_resolution": True,
        }
    ]

    rebound_document = SpecializedShadowDocument(
        document_id=uuid5(NAMESPACE_URL, "shadow-mixed-packet-rebound"),
        operation_document_id=111,
        assurance_document_id=121,
        source_sha256=original.source_sha256,
        role_hint=original.role_hint,
        filename=original.filename,
        content=original.content,
        engine2_resolution=original.engine2_resolution,
    )
    cached = _rehydrate_cached_run(
        (payload,),
        documents=(rebound_document,),
        computation_fingerprint="local-conflict-rebind-cache",
    )

    assert cached is not None
    assert len(cached.result.fusion_conflicts) == 1
    key = cached.result.fusion_conflicts[0].key
    assert key.line_item_key == "LINE:1"
    assert key.unbound_identity == str(rebound_document.document_id)


class MislabelledBolProvider:
    name = "gemini"
    model = "fixture-mislabelled-bol-model"

    def extract_scoped(self, *, filename, content, pages, allowed_fields, prompt):
        candidates = ()
        if "bill_of_lading" in allowed_fields:
            candidates = (
                AICandidate(
                    field_key="bill_of_lading",
                    value="OOLU1234567890",
                    normalized_value="OOLU1234567890",
                    evidence_class=EvidenceClass.EXPLICIT,
                    page=1,
                    source_text="Vessel: OOLU1234567890",
                    confidence=0.99,
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
            latency_ms=1,
        )


def _mislabelled_bol_document() -> SpecializedShadowDocument:
    block = LayoutBlock(
        block_id="bol-page-1",
        page=1,
        bbox=None,
        text="OCEAN BILL OF LADING\nVessel: OOLU1234567890",
        block_type="text",
    )
    resolution = DocumentResolution(
        filename="bill-of-lading.pdf",
        engine_version="fixture-engine2",
        document_type=EngineDocumentType.BILL_OF_LADING,
        type_confidence=0.99,
        layout=ParsedLayout(blocks=(block,), page_count=1),
        sections=(),
        fields={},
    )
    return SpecializedShadowDocument(
        document_id=uuid5(NAMESPACE_URL, "shadow-mislabelled-bol"),
        operation_document_id=12,
        assurance_document_id=22,
        source_sha256="c" * 64,
        role_hint="BILL_OF_LADING",
        filename="bill-of-lading.pdf",
        content=b"%PDF-bol-fixture",
        engine2_resolution=resolution,
    )


def test_specialized_admission_blocks_semantic_role_mismatch_before_fusion() -> None:
    run = run_specialized_shadow_operation(
        documents=(_mislabelled_bol_document(),),
        provider=MislabelledBolProvider(),
        concurrency=1,
    )

    assert run.result.fused_candidates == ()
    assert run.result.candidate_admission is not None
    assert run.result.candidate_admission.blocked_count == 1
    assert (
        run.result.candidate_admission.records[0].reason
        is CandidateAdmissionReason.SEMANTIC_ROLE_MISMATCH
    )



class BotanicalTaxonomyProvider:
    name = "gemini"
    model = "fixture-botanical-taxonomy-model"

    def extract_scoped(self, *, filename, content, pages, allowed_fields, prompt):
        candidates = ()
        if {"genus", "species"} & set(allowed_fields):
            candidates = tuple(
                candidate
                for candidate in (
                    AICandidate(
                        field_key="genus",
                        value="Hevea",
                        normalized_value="Hevea",
                        evidence_class=EvidenceClass.EXPLICIT,
                        page=1,
                        source_text="SKU RUBBER-1 Genus: Hevea Species: brasiliensis",
                        confidence=0.99,
                        provider=self.name,
                        model=self.model,
                        evidence_verified=False,
                    ),
                    AICandidate(
                        field_key="species",
                        value="brasiliensis",
                        normalized_value="brasiliensis",
                        evidence_class=EvidenceClass.EXPLICIT,
                        page=1,
                        source_text="SKU RUBBER-1 Genus: Hevea Species: brasiliensis",
                        confidence=0.99,
                        provider=self.name,
                        model=self.model,
                        evidence_verified=False,
                    ),
                )
                if candidate.field_key in allowed_fields
            )
        return AIExtractionResult(
            provider=self.name,
            model=self.model,
            schema_version=AI_SHADOW_SCHEMA_VERSION,
            candidates=candidates,
            page_count=len(pages),
            latency_ms=1,
        )


def _botanical_taxonomy_document() -> SpecializedShadowDocument:
    block = LayoutBlock(
        block_id="botanical-page-1",
        page=1,
        bbox=None,
        text=(
            "BOTANICAL DECLARATION\n"
            "SKU RUBBER-1 Genus: Hevea Species: brasiliensis"
        ),
        block_type="text",
    )
    resolution = DocumentResolution(
        filename="botanical.pdf",
        engine_version="fixture-engine2",
        document_type=EngineDocumentType.SPECIES_DECLARATION,
        type_confidence=0.99,
        layout=ParsedLayout(blocks=(block,), page_count=1),
        sections=(),
        fields={},
    )
    return SpecializedShadowDocument(
        document_id=uuid5(NAMESPACE_URL, "shadow-botanical-taxonomy"),
        operation_document_id=13,
        assurance_document_id=23,
        source_sha256="d" * 64,
        role_hint="BOTANICAL_DECLARATION",
        filename="botanical.pdf",
        content=b"%PDF-botanical-fixture",
        engine2_resolution=resolution,
    )


def test_specialized_serialization_persists_noncanonical_taxonomy_for_pdf_candidates() -> None:
    document = _botanical_taxonomy_document()
    run = run_specialized_shadow_operation(
        documents=(document,),
        provider=BotanicalTaxonomyProvider(),
        concurrency=1,
    )

    payload = serialize_specialized_document_run(
        run=run,
        document_id=document.document_id,
        source_set_fingerprint="source-set-taxonomy-fixture",
    )

    by_field = {item["field_key"]: item for item in payload["candidates"]}
    assert by_field["genus"]["value"] == "Hevea"
    assert by_field["genus"]["taxonomy"]["version"] == SPECIALIZED_TAXONOMY_VERSION
    assert by_field["genus"]["taxonomy"]["status"] == "REVIEW_REQUIRED"

    assert by_field["species"]["value"] == "brasiliensis"
    assert by_field["species"]["normalized_value"] == "brasiliensis"
    assert by_field["species"]["taxonomy"]["query"] == "Hevea brasiliensis"
    assert by_field["species"]["taxonomy"]["query_source"] == "LINE_GENUS_CONTEXT"
    assert by_field["species"]["taxonomy"]["status"] == "RESOLVED"
    assert (
        by_field["species"]["taxonomy"]["candidates"][0]["scientific_name"]
        == "Hevea brasiliensis"
    )
