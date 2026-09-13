from __future__ import annotations

import json

from litoral_trace.lacey_engine.ai_providers import AIProviderConfig
from litoral_trace.lacey_engine.multi_agent.gemini_specialist_adapter import (
    GeminiSpecialistProvider,
    _scoped_schema,
)


def _config() -> AIProviderConfig:
    return AIProviderConfig(
        mode="SHADOW",
        provider="gemini",
        model="gemini-test",
        base_url="https://example.invalid/v1beta/interactions",
        api_key="test-key",
        timeout_seconds=30.0,
        max_pages=8,
        allow_external=True,
    )


def test_scoped_schema_closes_field_key_enum():
    schema = _scoped_schema(frozenset({"hts_code", "entered_value"}))

    item = schema["properties"]["candidates"]["items"]
    assert item["properties"]["field_key"]["enum"] == ["entered_value", "hts_code"]
    assert "complete source row" in item["properties"]["source_text"]["description"]


def test_adapter_reuses_shared_transport_and_ai_shadow_conversion(monkeypatch):
    import litoral_trace.lacey_engine.multi_agent.gemini_specialist_adapter as module

    captured_payloads: list[dict[str, object]] = []

    monkeypatch.setattr(
        module,
        "_document_images",
        lambda filename, content, max_pages: [b"page-1", b"page-2", b"page-3"][:max_pages],
    )

    def fake_post_json(*, url, payload, timeout, headers):
        captured_payloads.append(payload)
        page_number = 2
        return {
            "output_text": json.dumps(
                {
                    "candidates": [
                        {
                            "field_key": "description",
                            "value": "PAL",
                            "evidence_class": "EXPLICIT",
                            "page": page_number,
                            "source_text": "PAL",
                            "confidence": 0.99,
                            "bbox": None,
                            "reason": "packaging noise",
                        },
                        {
                            "field_key": "hts_code",
                            "value": "4419.90.9000",
                            "evidence_class": "EXPLICIT",
                            "page": page_number,
                            "source_text": "1 ACT-TRAY-18 Acacia tray 4419.90.9000 420 18,900.00",
                            "confidence": 0.96,
                            "bbox": None,
                            "reason": "commercial row",
                        },
                    ]
                }
            )
        }

    monkeypatch.setattr(module, "_post_json", fake_post_json)

    provider = GeminiSpecialistProvider(_config())
    result = provider.extract_scoped(
        filename="fixture.pdf",
        content=b"%PDF-fixture",
        pages=(2,),
        allowed_fields=frozenset({"description", "hts_code", "entered_value"}),
        prompt="Extract commercial rows only.",
    )

    # The existing deterministic garbage filter still runs through
    # extraction_result_from_payload and removes the PAL description.
    assert [(candidate.field_key, candidate.value, candidate.page) for candidate in result.candidates] == [
        ("hts_code", "4419.90.9000", 2)
    ]
    assert result.page_count == 1
    assert len(captured_payloads) == 1

    payload = captured_payloads[0]
    prompt_text = payload["input"][0]["text"]
    assert "Extract commercial rows only." in prompt_text
    assert "CLOSED FIELD CONTRACT" in prompt_text
    enum = payload["response_format"]["schema"]["properties"]["candidates"]["items"]["properties"]["field_key"]["enum"]
    assert enum == ["description", "entered_value", "hts_code"]
