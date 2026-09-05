from __future__ import annotations

import json
from io import BytesIO

import pytest

from litoral_trace.lacey_engine.ai_providers import (
    AIProviderConfig,
    MistralOcrProvider,
    QwenOllamaProvider,
    build_ai_provider,
)
from litoral_trace.lacey_engine.ai_shadow import (
    AI_SHADOW_SCHEMA_VERSION,
    AIShadowError,
    ReconciliationStatus,
    candidate_from_payload,
    evaluate_golden,
    extraction_result_from_payload,
    reconcile_engine2_with_ai,
)
from litoral_trace.lacey_engine.domain import (
    AdmittedCandidate,
    DocumentResolution,
    DocumentType,
    EvidenceClass,
    FieldStatus,
    LayoutBlock,
    LayoutStructureType,
    ParsedLayout,
    Provenance,
    RawCandidate,
    ResolvedField,
)


def _matched(field_key: str, value: str, block: LayoutBlock) -> ResolvedField:
    raw = RawCandidate(
        field_key=field_key,
        raw_text=value,
        normalized_value=value,
        source_block=block,
        evidence_class=EvidenceClass.EXPLICIT,
        extractor_name="test",
        extractor_version="1",
        label=field_key,
    )
    provenance = Provenance(
        filename="fixture.pdf",
        page=block.page,
        bbox=block.bbox,
        block_id=block.block_id,
        source_text=block.text,
        extractor_name="test",
        extractor_version="1",
        evidence_class=EvidenceClass.EXPLICIT,
    )
    admitted = AdmittedCandidate(raw=raw, provenance=provenance, score=100.0, document_type=DocumentType.WEB_PRINT_MANIFEST)
    return ResolvedField(field_key, FieldStatus.MATCHED, value, admitted, (admitted,))


def _engine2() -> DocumentResolution:
    container_block = LayoutBlock(
        block_id="p1-container",
        page=1,
        bbox=None,
        text="Container Number MSKU9228574",
        block_type="TEXT_LINE",
        structure_type=LayoutStructureType.KEY_VALUE_TABLE,
        key_text="Container Number",
        value_text="MSKU9228574",
    )
    port_block = LayoutBlock(
        block_id="p1-port",
        page=1,
        bbox=None,
        text="Foreign Port of Lading PORT CHALMERS NEW ZEALAND",
        block_type="TEXT_LINE",
    )
    description_block = LayoutBlock(
        block_id="p2-description",
        page=2,
        bbox=None,
        text="Cargo Description 1 SINGLE PACKS OF PINUS RADIATA TIMBER",
        block_type="TEXT_LINE",
    )
    return DocumentResolution(
        filename="fixture.pdf",
        engine_version="lacey-engine-2.0.0",
        document_type=DocumentType.WEB_PRINT_MANIFEST,
        type_confidence=0.99,
        layout=ParsedLayout((container_block, port_block, description_block), 2),
        sections=(),
        fields={
            "container_number": _matched("container_number", "MSKU9228574", container_block),
            "description": _matched("description", "SINGLE PACKS OF PINUS RADIATA TIMBER", description_block),
            "country_of_harvest": ResolvedField("country_of_harvest", FieldStatus.MISSING, None, None, ()),
        },
    )


def _candidate(field_key: str, value: str, source_text: str, *, page: int = 1, evidence: str = "EXPLICIT", confidence: float = 0.95):
    return {
        "field_key": field_key,
        "value": value,
        "evidence_class": evidence,
        "page": page,
        "source_text": source_text,
        "confidence": confidence,
        "bbox": None,
        "reason": None,
    }


def test_reconciliation_agrees_and_rejects_inferred_regulatory_value():
    engine2 = _engine2()
    ai = extraction_result_from_payload(
        payload={"candidates": [
            _candidate("container_number", "MSKU9228574", "Container Number MSKU9228574"),
            _candidate("country_of_harvest", "New Zealand", "Foreign Port of Lading PORT CHALMERS NEW ZEALAND", evidence="INFERRED"),
        ]},
        provider="qwen_ollama",
        model="qwen2.5vl:7b",
    )
    comparison = reconcile_engine2_with_ai(engine2=engine2, ai=ai)
    assert comparison.field("container_number").status is ReconciliationStatus.AGREEMENT
    assert comparison.field("country_of_harvest").status is ReconciliationStatus.AI_REJECTED
    assert comparison.inferred_candidate_rate == 0.5
    assert comparison.verified_evidence_rate == 1.0


def test_unverified_source_text_never_becomes_ai_only():
    engine2 = _engine2()
    ai = extraction_result_from_payload(
        payload={"candidates": [_candidate("manufacturer_id", "ABC123", "Manufacturer ID ABC123")]},
        provider="qwen_ollama",
        model="qwen2.5vl:7b",
    )
    comparison = reconcile_engine2_with_ai(engine2=engine2, ai=ai)
    row = comparison.field("manufacturer_id")
    assert row.status is ReconciliationStatus.AI_REJECTED
    assert row.ai_value is None
    assert row.rejected_ai_candidate_count == 1


def test_golden_metrics_measure_precision_without_treating_inference_as_autofill():
    engine2 = _engine2()
    ai = extraction_result_from_payload(
        payload={"candidates": [
            _candidate("container_number", "MSKU9228574", "Container Number MSKU9228574"),
            _candidate("description", "WRONG DESCRIPTION", "Cargo Description 1 SINGLE PACKS OF PINUS RADIATA TIMBER", page=2),
            _candidate("country_of_harvest", "New Zealand", "Foreign Port of Lading PORT CHALMERS NEW ZEALAND", evidence="INFERRED"),
        ]},
        provider="mistral_ocr",
        model="mistral-ocr-latest",
    )
    metrics = evaluate_golden(
        engine2=engine2,
        ai=ai,
        expected={
            "container_number": "MSKU9228574",
            "description": "SINGLE PACKS OF PINUS RADIATA TIMBER",
            "country_of_harvest": None,
        },
    )
    assert metrics.engine2_precision == 1.0
    assert metrics.engine2_recall == 1.0
    assert metrics.ai_precision == 0.5
    assert metrics.ai_recall == 0.5
    assert metrics.ai_false_candidate_rate == 0.5
    assert metrics.ai_inferred_candidate_rate == pytest.approx(1 / 3)
    assert metrics.reconciliation_conflicts == 1


def test_candidate_contract_rejects_unknown_fields_and_requires_evidence():
    with pytest.raises(AIShadowError):
        candidate_from_payload(
            payload=_candidate("not_a_lacey_field", "x", "source"), provider="qwen_ollama", model="qwen"
        )
    bad = _candidate("container_number", "MSKU9228574", "")
    with pytest.raises(AIShadowError):
        candidate_from_payload(payload=bad, provider="qwen_ollama", model="qwen")


def test_provider_factory_switches_models_by_config_without_code_change():
    qwen_config = AIProviderConfig("SHADOW", "qwen_ollama", "qwen2.5vl:7b", "http://localhost:11434/api/chat", None, 30, 8, False)
    assert isinstance(build_ai_provider(qwen_config), QwenOllamaProvider)
    mistral_config = AIProviderConfig("SHADOW", "mistral_ocr", "mistral-ocr-latest", "https://api.mistral.ai/v1/ocr", "test-key", 30, 8, True)
    assert isinstance(build_ai_provider(mistral_config), MistralOcrProvider)
    assert build_ai_provider(AIProviderConfig("OFF", "qwen_ollama", "qwen", "http://localhost", None, 30, 8, False)) is None


def test_mistral_requires_explicit_external_data_egress_opt_in():
    config = AIProviderConfig("SHADOW", "mistral_ocr", "mistral-ocr-latest", "https://api.mistral.ai/v1/ocr", "test-key", 30, 8, False)
    with pytest.raises(AIShadowError, match="disabled by policy"):
        MistralOcrProvider(config)


def test_qwen_adapter_parses_structured_output_without_network(monkeypatch):
    from litoral_trace.lacey_engine import ai_providers

    config = AIProviderConfig("SHADOW", "qwen_ollama", "qwen2.5vl:7b", "http://localhost:11434/api/chat", None, 30, 8, False)
    monkeypatch.setattr(ai_providers, "_document_images", lambda filename, content, max_pages: [b"image"])
    monkeypatch.setattr(
        ai_providers,
        "_post_json",
        lambda **kwargs: {"message": {"content": json.dumps({"candidates": [_candidate("container_number", "MSKU9228574", "Container Number MSKU9228574")]})}},
    )
    result = QwenOllamaProvider(config).extract(filename="fixture.pdf", content=b"pdf")
    assert result.schema_version == AI_SHADOW_SCHEMA_VERSION
    assert result.page_count == 1
    assert result.candidates[0].field_key == "container_number"
    assert result.candidates[0].page == 1


def test_mistral_adapter_uses_document_annotation_schema_without_network(monkeypatch):
    from litoral_trace.lacey_engine import ai_providers

    captured = {}
    def fake_post_json(**kwargs):
        captured.update(kwargs)
        return {
            "document_annotation": {"candidates": [_candidate("container_number", "MSKU9228574", "Container Number MSKU9228574")]},
            "pages": [{"index": 0}],
        }
    monkeypatch.setattr(ai_providers, "_post_json", fake_post_json)
    config = AIProviderConfig("SHADOW", "mistral_ocr", "mistral-ocr-latest", "https://api.mistral.ai/v1/ocr", "test-key", 30, 8, True)
    from pypdf import PdfWriter
    stream = BytesIO()
    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    writer.write(stream)
    result = MistralOcrProvider(config).extract(filename="fixture.pdf", content=stream.getvalue())
    payload = captured["payload"]
    assert payload["document_annotation_format"]["type"] == "json_schema"
    assert payload["document"]["document_url"].startswith("data:application/pdf;base64,")
    assert result.candidates[0].value == "MSKU9228574"
