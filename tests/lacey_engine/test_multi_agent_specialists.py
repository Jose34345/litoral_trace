from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from litoral_trace.lacey_engine.ai_shadow import AICandidate, AIExtractionResult, AI_SHADOW_SCHEMA_VERSION
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import DocumentType, RoutedDocument, SpecialistRole
from litoral_trace.lacey_engine.multi_agent.specialist_runtime import SpecialistInputDocument
from litoral_trace.lacey_engine.multi_agent.specialists import (
    BotanicalExtractor,
    CommercialLineExtractor,
    CustomsIdentityExtractor,
    LogisticsExtractor,
)


class FakeScopedProvider:
    name = "fake"
    model = "fake-model"

    def __init__(self, candidates: tuple[AICandidate, ...]) -> None:
        self.candidates = candidates
        self.calls: list[dict[str, object]] = []

    def extract_scoped(self, **kwargs) -> AIExtractionResult:
        self.calls.append(kwargs)
        return AIExtractionResult(
            provider=self.name,
            model=self.model,
            schema_version=AI_SHADOW_SCHEMA_VERSION,
            candidates=self.candidates,
            page_count=len(kwargs["pages"]),
            latency_ms=17,
        )


def _candidate(field_key: str, value: str, *, page: int = 1) -> AICandidate:
    return AICandidate(
        field_key=field_key,
        value=value,
        normalized_value=value,
        evidence_class=EvidenceClass.EXPLICIT,
        page=page,
        source_text=f"ROW-1 SKU-1 {field_key} {value}",
        confidence=0.9,
        provider="fake",
        model="fake-model",
        evidence_verified=False,
    )


def _document(document_type: DocumentType = DocumentType.COMMERCIAL_INVOICE) -> SpecialistInputDocument:
    routed = RoutedDocument(
        document_id=uuid5(NAMESPACE_URL, document_type.value),
        document_type=document_type,
        pages=(1,),
        confidence=0.98,
        signals=("fixture",),
    )
    return SpecialistInputDocument(routed=routed, filename="fixture.pdf", content=b"%PDF-fixture")


def test_specialist_allowed_fields_are_closed_and_domain_specific():
    assert CustomsIdentityExtractor.allowed_fields == frozenset(
        {
            "importer_name",
            "importer_address",
            "consignee_name",
            "consignee_address",
            "filing_entry_reference",
            "manufacturer_id",
        }
    )
    assert LogisticsExtractor.allowed_fields == frozenset(
        {"bill_of_lading", "container_number", "estimated_arrival_date"}
    )
    assert CommercialLineExtractor.allowed_fields == frozenset(
        {"description", "article_component", "hts_code", "entered_value"}
    )
    assert BotanicalExtractor.allowed_fields == frozenset(
        {"genus", "species", "country_of_harvest", "plant_quantity", "metric_unit"}
    )


def test_runtime_drops_provider_field_outside_specialist_scope_and_warns():
    provider = FakeScopedProvider(
        (
            _candidate("hts_code", "4419.90.9000"),
            _candidate("genus", "Acacia"),
        )
    )
    specialist = CommercialLineExtractor(provider)

    result = specialist.extract((_document(),))

    assert result.role is SpecialistRole.COMMERCIAL_LINES
    assert [item.candidate.field_key for item in result.candidates] == ["hts_code"]
    assert result.candidates[0].line_item_key is None
    assert result.candidates[0].source_span_id is None
    assert result.warnings == ("OUT_OF_SCOPE_FIELD:genus:COMMERCIAL_INVOICE",)
    assert provider.calls[0]["allowed_fields"] == CommercialLineExtractor.allowed_fields
    assert "row" in str(provider.calls[0]["prompt"]).casefold()


def test_runtime_drops_candidate_from_page_outside_routed_page_set():
    provider = FakeScopedProvider((_candidate("container_number", "TLLU4827315", page=2),))
    specialist = LogisticsExtractor(provider)

    result = specialist.extract((_document(DocumentType.BILL_OF_LADING),))

    assert result.candidates == ()
    assert result.warnings == ("OUT_OF_SCOPE_PAGE:2:BILL_OF_LADING",)


def test_task_for_preserves_closed_allowed_fields():
    provider = FakeScopedProvider(())
    specialist = BotanicalExtractor(provider)
    routed = _document(DocumentType.BOTANICAL_DECLARATION).routed

    task = specialist.task_for((routed,))

    assert task.role is SpecialistRole.BOTANICAL
    assert task.documents == (routed,)
    assert task.allowed_fields is BotanicalExtractor.allowed_fields


def test_agent_run_id_is_shared_inside_one_specialist_run():
    provider = FakeScopedProvider(
        (
            _candidate("importer_name", "Northstar Kitchen Imports LLC"),
            _candidate("manufacturer_id", "VNMKHOM123HCM"),
        )
    )
    specialist = CustomsIdentityExtractor(provider)

    result = specialist.extract((_document(DocumentType.ENTRY_WORKSHEET),))

    assert len(result.candidates) == 2
    assert len({item.agent_run_id for item in result.candidates}) == 1
    assert all(item.specialist is SpecialistRole.CUSTOMS_IDENTITY for item in result.candidates)
    assert result.latency_ms == 17
