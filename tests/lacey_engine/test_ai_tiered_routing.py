from __future__ import annotations

import json

import pytest

from litoral_trace.lacey_engine import ai_providers
from litoral_trace.lacey_engine.ai_providers import (
    AIProviderConfig,
    OpenAIResponsesProvider,
    build_ai_provider,
)
from litoral_trace.lacey_engine.ai_routing import (
    AITask,
    AITierConfig,
    can_offer_bulk_confirmation,
    next_ai_task,
)
from litoral_trace.lacey_engine.ai_shadow import ReconciliationStatus


def _candidate():
    return {
        "field_key": "container_number",
        "value": "MSKU9228574",
        "evidence_class": "EXPLICIT",
        "page": 1,
        "source_text": "Container Number MSKU9228574",
        "confidence": 0.98,
        "bbox": None,
        "reason": None,
    }


def test_default_tiers_keep_luna_for_volume_and_sol_for_true_ambiguity():
    config = AITierConfig()
    assert config.model_for(AITask.EXTRACT) == "gpt-5.6-luna"
    assert config.model_for(AITask.RECONCILE) == "gpt-5.6-terra"
    assert config.model_for(AITask.ADJUDICATE) == "gpt-5.6-sol"
    assert config.model_for(AITask.NONE) is None


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (ReconciliationStatus.AGREEMENT, AITask.NONE),
        (ReconciliationStatus.AI_ONLY, AITask.RECONCILE),
        (ReconciliationStatus.ENGINE2_ONLY, AITask.RECONCILE),
        (ReconciliationStatus.CONFLICT, AITask.ADJUDICATE),
        (ReconciliationStatus.AI_AMBIGUOUS, AITask.ADJUDICATE),
        (ReconciliationStatus.ENGINE2_CONFLICT, AITask.ADJUDICATE),
        (ReconciliationStatus.BOTH_MISSING, AITask.NONE),
        (ReconciliationStatus.AI_REJECTED, AITask.NONE),
    ],
)
def test_routing_spends_more_only_when_evidence_requires_it(status, expected):
    assert next_ai_task(status) is expected


def test_only_independent_evidence_agreement_is_bulk_confirmation_candidate():
    assert can_offer_bulk_confirmation(ReconciliationStatus.AGREEMENT) is True
    assert can_offer_bulk_confirmation(ReconciliationStatus.AI_ONLY) is False
    assert can_offer_bulk_confirmation(ReconciliationStatus.CONFLICT) is False


def test_openai_provider_is_shadow_only_and_requires_explicit_external_egress():
    blocked = AIProviderConfig(
        "SHADOW",
        "openai",
        "gpt-5.6-luna",
        "https://api.openai.com/v1/responses",
        "test-key",
        30,
        8,
        False,
    )
    with pytest.raises(Exception, match="disabled by policy"):
        OpenAIResponsesProvider(blocked)

    enabled = AIProviderConfig(
        "SHADOW",
        "openai",
        "gpt-5.6-luna",
        "https://api.openai.com/v1/responses",
        "test-key",
        30,
        8,
        True,
    )
    assert isinstance(build_ai_provider(enabled), OpenAIResponsesProvider)


def test_openai_responses_adapter_uses_vision_and_strict_structured_output_without_network(monkeypatch):
    captured = {}

    monkeypatch.setattr(
        ai_providers,
        "_document_images",
        lambda filename, content, max_pages: [b"image-bytes"],
    )

    def fake_post_json(**kwargs):
        captured.update(kwargs)
        return {
            "output": [
                {
                    "type": "message",
                    "content": [
                        {
                            "type": "output_text",
                            "text": json.dumps({"candidates": [_candidate()]}),
                        }
                    ],
                }
            ]
        }

    monkeypatch.setattr(ai_providers, "_post_json", fake_post_json)
    config = AIProviderConfig(
        "SHADOW",
        "openai",
        "gpt-5.6-luna",
        "https://api.openai.com/v1/responses",
        "test-key",
        30,
        8,
        True,
    )
    result = OpenAIResponsesProvider(config).extract(filename="fixture.pdf", content=b"pdf")

    assert result.provider == "openai"
    assert result.model == "gpt-5.6-luna"
    assert result.page_count == 1
    assert result.candidates[0].value == "MSKU9228574"
    payload = captured["payload"]
    assert payload["model"] == "gpt-5.6-luna"
    assert payload["text"]["format"]["type"] == "json_schema"
    assert payload["text"]["format"]["strict"] is True
    assert payload["input"][0]["content"][1]["type"] == "input_image"
    assert payload["input"][0]["content"][1]["image_url"].startswith("data:image/png;base64,")
    assert captured["headers"]["Authorization"] == "Bearer test-key"
