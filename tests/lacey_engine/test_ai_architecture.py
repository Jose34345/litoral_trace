from __future__ import annotations

import logging

from litoral_trace.lacey_engine.architecture import AIArchitecture, ai_architecture


def test_ai_architecture_defaults_to_legacy() -> None:
    assert ai_architecture({}) is AIArchitecture.LEGACY


def test_ai_architecture_accepts_supported_modes() -> None:
    expected = {
        "legacy": AIArchitecture.LEGACY,
        "specialized": AIArchitecture.SPECIALIZED,
        "shadow": AIArchitecture.SHADOW,
    }
    for raw, mode in expected.items():
        assert ai_architecture({"LT_AI_ARCHITECTURE": raw}) is mode


def test_ai_architecture_normalizes_case_and_whitespace() -> None:
    assert ai_architecture({"LT_AI_ARCHITECTURE": "  SHADOW  "}) is AIArchitecture.SHADOW


def test_ai_architecture_invalid_value_fails_safe_to_legacy(caplog) -> None:
    with caplog.at_level(logging.WARNING):
        result = ai_architecture({"LT_AI_ARCHITECTURE": "future-agent-v9"})

    assert result is AIArchitecture.LEGACY
    assert "LT_AI_ARCHITECTURE" in caplog.text
    assert "legacy" in caplog.text.lower()
