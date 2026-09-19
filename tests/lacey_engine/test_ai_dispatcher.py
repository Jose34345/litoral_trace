from __future__ import annotations

import pytest

from litoral_trace.lacey_engine.ai_providers import AIProviderConfig
from litoral_trace.us_lacey.lacey_engine_service import UsLaceyEngine2Service


def _config() -> AIProviderConfig:
    return AIProviderConfig(
        mode="SHADOW",
        provider="gemini",
        model="fixture-model",
        base_url="https://example.invalid",
        api_key="fixture-key",
        timeout_seconds=30.0,
        max_pages=8,
        allow_external=True,
    )


def _service() -> UsLaceyEngine2Service:
    return UsLaceyEngine2Service(
        session_factory=lambda: None,
        vault_service=object(),
    )


@pytest.mark.parametrize(
    ("architecture", "expected"),
    [
        ("legacy", ["legacy"]),
        ("specialized", ["specialized"]),
        ("shadow", ["legacy", "specialized"]),
    ],
)
def test_ai_dispatcher_selects_requested_architecture(
    monkeypatch,
    architecture: str,
    expected: list[str],
) -> None:
    service = _service()
    calls: list[str] = []
    monkeypatch.setenv("LT_AI_ARCHITECTURE", architecture)
    monkeypatch.setattr(
        service,
        "_run_legacy_ai_operation",
        lambda **_: calls.append("legacy"),
        raising=False,
    )
    monkeypatch.setattr(
        service,
        "_run_specialized_ai_operation",
        lambda **_: calls.append("specialized"),
        raising=False,
    )

    service._dispatch_ai_extractors(
        config=_config(),
        organization_id=11,
        operation_id=13,
        documents=(),
        source_set_fingerprint="f" * 64,
    )

    assert calls == expected


def test_shadow_dispatcher_swallows_specialized_exception_after_legacy_success(
    monkeypatch,
) -> None:
    service = _service()
    calls: list[str] = []
    monkeypatch.setenv("LT_AI_ARCHITECTURE", "shadow")
    monkeypatch.setattr(
        service,
        "_run_legacy_ai_operation",
        lambda **_: calls.append("legacy"),
        raising=False,
    )

    def fail_specialized(**_: object) -> None:
        calls.append("specialized")
        raise RuntimeError("specialized catastrophic failure")

    monkeypatch.setattr(
        service,
        "_run_specialized_ai_operation",
        fail_specialized,
        raising=False,
    )

    service._dispatch_ai_extractors(
        config=_config(),
        organization_id=11,
        operation_id=13,
        documents=(),
        source_set_fingerprint="f" * 64,
    )

    assert calls == ["legacy", "specialized"]
