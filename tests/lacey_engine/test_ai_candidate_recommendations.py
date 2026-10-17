from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from litoral_trace.lacey_engine.ai_providers import AIProviderConfig
from litoral_trace.lacey_engine.ai_shadow import AIShadowError
from litoral_trace.us_lacey import ai_review
from litoral_trace.us_lacey.ai_review import ReviewCandidate, _call_openai_decision, _task_for_issue
from litoral_trace.lacey_engine.ai_routing import AITask


def _candidate(candidate_id: int, value: str) -> ReviewCandidate:
    return ReviewCandidate(candidate_id, value, 1, f"page:1;value:{value}", 0.9)


def _response(payload: dict) -> dict:
    return {
        "output": [
            {
                "type": "message",
                "content": [{"type": "output_text", "text": json.dumps(payload)}],
            }
        ]
    }


def _config() -> AIProviderConfig:
    return AIProviderConfig(
        "SHADOW",
        "openai",
        "gpt-5.6-luna",
        "https://api.openai.com/v1/responses",
        "test-key",
        30,
        8,
        True,
    )


def test_blocking_field_conflict_routes_to_sol_adjudication():
    issue = SimpleNamespace(rule_code="US_LACEY_FIELD_CONFLICT", severity="BLOCKING")
    assert _task_for_issue(issue) is AITask.ADJUDICATE
    issue = SimpleNamespace(rule_code="OTHER_REVIEW", severity="WARNING")
    assert _task_for_issue(issue) is AITask.RECONCILE


def test_ai_can_only_select_one_of_supplied_candidate_ids(monkeypatch):
    captured = {}

    def fake_post_json(**kwargs):
        captured.update(kwargs)
        return _response(
            {
                "action": "SELECT",
                "candidate_id": 11,
                "confidence": 0.88,
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
    result = _call_openai_decision(
        provider_config=_config(),
        model="gpt-5.6-sol",
        field_name="country_of_harvest",
        issue=issue,
        candidates=(_candidate(10, "Chile"), _candidate(11, "Peru")),
    )
    assert result.action == "SELECT"
    assert result.candidate_id == 11
    assert result.model == "gpt-5.6-sol"
    assert captured["payload"]["store"] is False
    prompt = captured["payload"]["input"][0]["content"][0]["text"]
    assert "may only select one candidate_id" in prompt
    assert "Never infer country of harvest" in prompt


def test_unknown_candidate_selection_is_rejected_even_with_valid_json(monkeypatch):
    monkeypatch.setattr(
        ai_review,
        "_post_json",
        lambda **_kwargs: _response(
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
        _call_openai_decision(
            provider_config=_config(),
            model="gpt-5.6-sol",
            field_name="country_of_harvest",
            issue=issue,
            candidates=(_candidate(10, "Chile"), _candidate(11, "Peru")),
        )


def test_needs_human_forces_null_candidate(monkeypatch):
    monkeypatch.setattr(
        ai_review,
        "_post_json",
        lambda **_kwargs: _response(
            {
                "action": "NEEDS_HUMAN",
                "candidate_id": 10,
                "confidence": 0.3,
                "reason_code": "CONFLICT_UNRESOLVED",
            }
        ),
    )
    issue = SimpleNamespace(
        rule_code="US_LACEY_FIELD_CONFLICT",
        severity="BLOCKING",
        left_value="Chile",
        right_value="Peru",
    )
    result = _call_openai_decision(
        provider_config=_config(),
        model="gpt-5.6-sol",
        field_name="country_of_harvest",
        issue=issue,
        candidates=(_candidate(10, "Chile"), _candidate(11, "Peru")),
    )
    assert result.action == "NEEDS_HUMAN"
    assert result.candidate_id is None
