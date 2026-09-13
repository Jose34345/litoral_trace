from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from litoral_trace.lacey_engine import ai_providers
from litoral_trace.lacey_engine.ai_providers import AIProviderConfig, build_ai_provider
from litoral_trace.lacey_engine.ai_routing import AITask, AITierConfig
from litoral_trace.lacey_engine.ai_shadow import AIShadowError
from litoral_trace.lacey_engine.gemini_provider import GeminiInteractionsProvider
from litoral_trace.us_lacey import ai_review
from litoral_trace.us_lacey.ai_review import ReviewCandidate, _call_gemini_decision


def _candidate_payload():
    return {
        "field_key": "container_number",
        "value": "MSKU9228574",
        "evidence_class": "EXPLICIT",
        "page": 99,
        "source_text": "Container Number MSKU9228574",
        "confidence": 0.98,
        "bbox": None,
        "reason": None,
    }


def _gemini_response(payload: dict) -> dict:
    return {
        "id": "int_test",
        "status": "completed",
        "steps": [
            {
                "type": "model_output",
                "status": "done",
                "content": [{"type": "text", "text": json.dumps(payload)}],
            }
        ],
    }


def _config(model: str = "gemini-3.5-flash-lite") -> AIProviderConfig:
    return AIProviderConfig(
        "SHADOW",
        "gemini",
        model,
        "https://generativelanguage.googleapis.com/v1beta/interactions",
        "test-gemini-key",
        30,
        8,
        True,
    )


def test_gemini_provider_requires_explicit_external_egress_and_key():
    blocked = AIProviderConfig(
        "SHADOW",
        "gemini",
        "gemini-3.5-flash-lite",
        "https://generativelanguage.googleapis.com/v1beta/interactions",
        "test-key",
        30,
        8,
        False,
    )
    with pytest.raises(AIShadowError, match="disabled by policy"):
        GeminiInteractionsProvider(blocked)

    missing_key = AIProviderConfig(
        "SHADOW",
        "gemini",
        "gemini-3.5-flash-lite",
        "https://generativelanguage.googleapis.com/v1beta/interactions",
        None,
        30,
        8,
        True,
    )
    with pytest.raises(AIShadowError, match="GEMINI_API_KEY"):
        GeminiInteractionsProvider(missing_key)


def test_factory_builds_gemini_provider():
    assert isinstance(build_ai_provider(_config()), GeminiInteractionsProvider)


def test_gemini_extraction_is_stateless_structured_and_page_bound(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        ai_providers,
        "_document_images",
        lambda filename, content, max_pages: [b"unused"],
    )
    # The Gemini adapter imported the helper at module load; patch it there too.
    from litoral_trace.lacey_engine import gemini_provider

    monkeypatch.setattr(
        gemini_provider,
        "_document_images",
        lambda filename, content, max_pages: [b"image-bytes"],
    )

    def fake_post_json(**kwargs):
        captured.update(kwargs)
        return _gemini_response({"candidates": [_candidate_payload()]})

    monkeypatch.setattr(gemini_provider, "_post_json", fake_post_json)
    result = GeminiInteractionsProvider(_config()).extract(
        filename="fixture.pdf", content=b"pdf"
    )

    assert result.provider == "gemini"
    assert result.model == "gemini-3.5-flash-lite"
    assert result.page_count == 1
    assert result.candidates[0].page == 1
    assert result.candidates[0].value == "MSKU9228574"
    payload = captured["payload"]
    assert payload["store"] is False
    assert payload["generation_config"]["thinking_level"] == "low"
    assert payload["response_format"]["mime_type"] == "application/json"
    assert payload["response_format"]["schema"]["required"] == ["candidates"]
    assert payload["input"][1]["type"] == "image"
    assert payload["input"][1]["mime_type"] == "image/png"
    assert captured["headers"]["x-goog-api-key"] == "test-gemini-key"


def test_provider_aware_tier_defaults_use_stable_gemini_models(monkeypatch):
    monkeypatch.setenv("US_LACEY_AI_PROVIDER", "gemini")
    monkeypatch.delenv("US_LACEY_AI_EXTRACT_MODEL", raising=False)
    monkeypatch.delenv("US_LACEY_AI_RECONCILE_MODEL", raising=False)
    monkeypatch.delenv("US_LACEY_AI_ADJUDICATE_MODEL", raising=False)
    tiers = AITierConfig.from_env()
    assert tiers.model_for(AITask.EXTRACT) == "gemini-3.5-flash-lite"
    assert tiers.model_for(AITask.RECONCILE) == "gemini-3.8-flash"
    assert tiers.model_for(AITask.ADJUDICATE) == "gemini-3.8-flash"


def test_gemini_review_can_only_select_supplied_candidate_and_uses_high_thinking(monkeypatch):
    captured = {}

    def fake_post_json(**kwargs):
        captured.update(kwargs)
        return _gemini_response(
            {
                "action": "SELECT",
                "candidate_id": 11,
                "confidence": 0.9,
                "reason_code": "CROSS_DOCUMENT_SUPPORT",
            }
        )

    monkeypatch.setattr(ai_review, "_post_json", fake_post_json)
    issue = SimpleNamespace(
        rule_code="US_LACEY_FIELD_CONFLICT",
        severity="BLOCKING",
        left_value="Chile",
        right_value="Peru",
    )
    candidates = (
        ReviewCandidate(10, "Chile", 1, "page:1", 0.9),
        ReviewCandidate(11, "Peru", 2, "page:2", 0.92),
    )
    result = _call_gemini_decision(
        provider_config=_config("gemini-3.8-flash"),
        model="gemini-3.8-flash",
        field_name="country_of_harvest",
        issue=issue,
        candidates=candidates,
        thinking_level="high",
    )
    assert result.action == "SELECT"
    assert result.candidate_id == 11
    payload = captured["payload"]
    assert payload["store"] is False
    assert payload["generation_config"]["thinking_level"] == "high"
    assert payload["response_format"]["schema"]["properties"]["action"]["enum"] == [
        "SELECT",
        "NEEDS_HUMAN",
    ]
    assert captured["headers"]["x-goog-api-key"] == "test-gemini-key"


def test_gemini_review_rejects_unknown_candidate(monkeypatch):
    monkeypatch.setattr(
        ai_review,
        "_post_json",
        lambda **kwargs: _gemini_response(
            {
                "action": "SELECT",
                "candidate_id": 999,
                "confidence": 0.99,
                "reason_code": "SOURCE_AUTHORITY",
            }
        ),
    )
    issue = SimpleNamespace(
        rule_code="US_LACEY_FIELD_CONFLICT",
        severity="BLOCKING",
        left_value="Chile",
        right_value="Peru",
    )
    with pytest.raises(AIShadowError, match="unknown candidate"):
        _call_gemini_decision(
            provider_config=_config("gemini-3.8-flash"),
            model="gemini-3.8-flash",
            field_name="country_of_harvest",
            issue=issue,
            candidates=(
                ReviewCandidate(10, "Chile", 1, "page:1", 0.9),
                ReviewCandidate(11, "Peru", 2, "page:2", 0.92),
            ),
            thinking_level="high",
        )
