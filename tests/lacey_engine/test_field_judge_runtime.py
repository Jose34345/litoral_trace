from __future__ import annotations

import json
from uuid import UUID

from litoral_trace.lacey_engine.ai_providers import AIProviderConfig
from litoral_trace.lacey_engine.ai_shadow import AICandidate, AIShadowError
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    SpecialistRole,
)
from litoral_trace.lacey_engine.multi_agent.field_judge import (
    FieldJudgeDecision,
    FieldJudgeMode,
    GeminiFieldJudgeProvider,
    candidate_identity,
    evaluate_field_judge,
    field_judge_output_schema,
)


def _candidate() -> CandidateEnvelope:
    return CandidateEnvelope(
        candidate=AICandidate(
            field_key="bill_of_lading",
            value="OOLU1234567",
            normalized_value="OOLU1234567",
            evidence_class=EvidenceClass.EXPLICIT,
            page=1,
            source_text="B/L No. OOLU1234567",
            confidence=0.99,
            provider="fixture",
            model="fixture",
            evidence_verified=True,
        ),
        document_id=UUID("11111111-1111-1111-1111-111111111111"),
        document_type=DocumentType.BILL_OF_LADING,
        specialist=SpecialistRole.LOGISTICS,
        agent_run_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        line_item_key=None,
        source_span_id=None,
    )


class StubProvider:
    name = "stub"
    model = "stub-model"

    def __init__(self, payload=None, *, error: Exception | None = None) -> None:
        self.payload = payload
        self.error = error
        self.calls = 0

    def judge_structured(self, *, prompt: str, schema: dict[str, object]):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.payload, 7, 11, 3, 14


def test_output_schema_has_no_business_value_output_field() -> None:
    envelope = _candidate()
    schema = field_judge_output_schema((envelope,))
    item_properties = schema["properties"]["decisions"]["items"]["properties"]

    assert set(item_properties) == {
        "candidate_id",
        "field_key",
        "line_item_key",
        "decision",
        "reason",
    }
    assert item_properties["candidate_id"]["enum"] == [candidate_identity(envelope)]


def test_evaluate_field_judge_returns_closed_decision_and_provider_telemetry() -> None:
    envelope = _candidate()
    provider = StubProvider(
        {
            "decisions": [
                {
                    "candidate_id": candidate_identity(envelope),
                    "field_key": "bill_of_lading",
                    "line_item_key": None,
                    "decision": "ACCEPT",
                    "reason": "AUTHORITATIVE_SOURCE",
                }
            ]
        }
    )

    result = evaluate_field_judge(
        (envelope,),
        provider=provider,
        mode=FieldJudgeMode.SHADOW,
    )

    assert provider.calls == 1
    assert result.provider == "stub"
    assert result.model == "stub-model"
    assert result.latency_ms == 7
    assert (result.input_tokens, result.output_tokens, result.total_tokens) == (11, 3, 14)
    assert result.decisions[0].decision is FieldJudgeDecision.ACCEPT


def test_provider_failure_degrades_every_candidate_to_needs_review() -> None:
    envelope = _candidate()
    provider = StubProvider(error=AIShadowError("provider unavailable"))

    result = evaluate_field_judge(
        (envelope,),
        provider=provider,
        mode=FieldJudgeMode.ENFORCE,
    )

    assert result.decisions[0].decision is FieldJudgeDecision.NEEDS_REVIEW
    assert result.safe_error == "provider unavailable"


def test_gemini_adapter_parses_structured_output_and_reported_usage(monkeypatch) -> None:
    envelope = _candidate()
    captured: dict[str, object] = {}

    def fake_post_json(*, url, payload, timeout, headers):
        captured["payload"] = payload
        return {
            "usageMetadata": {
                "promptTokenCount": 23,
                "candidatesTokenCount": 5,
                "totalTokenCount": 28,
            }
        }

    monkeypatch.setattr(
        "litoral_trace.lacey_engine.multi_agent.field_judge._post_json",
        fake_post_json,
    )
    monkeypatch.setattr(
        "litoral_trace.lacey_engine.multi_agent.field_judge.gemini_output_text",
        lambda response: json.dumps(
            {
                "decisions": [
                    {
                        "candidate_id": candidate_identity(envelope),
                        "field_key": "bill_of_lading",
                        "line_item_key": None,
                        "decision": "ACCEPT",
                        "reason": "EXACT_FIELD_CONTEXT",
                    }
                ]
            }
        ),
    )
    provider = GeminiFieldJudgeProvider(
        AIProviderConfig(
            mode="SHADOW",
            provider="gemini",
            model="gemini-test",
            base_url="https://example.invalid/interactions",
            api_key="secret",
            timeout_seconds=30,
            max_pages=8,
            allow_external=True,
        )
    )

    result = evaluate_field_judge(
        (envelope,),
        provider=provider,
        mode=FieldJudgeMode.SHADOW,
    )

    assert result.decisions[0].decision is FieldJudgeDecision.ACCEPT
    assert (result.input_tokens, result.output_tokens, result.total_tokens) == (23, 5, 28)
    response_schema = captured["payload"]["response_format"]["schema"]
    decision_properties = response_schema["properties"]["decisions"]["items"]["properties"]
    assert "value" not in decision_properties
