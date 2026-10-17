from __future__ import annotations

import pytest

import litoral_trace.services.translation as translation_module
from litoral_trace.services.translation import DeepLTranslationProvider
from litoral_trace.us_lacey.shadow_evidence_snapshot import configured_translation_provider


class _FakeDeepLResponse:
    def __init__(self, text: str) -> None:
        self.text = text
        self.detected_source_lang = "ES"


class _FakeDeepLTranslator:
    api_keys: list[str] = []
    calls: list[tuple[str, str]] = []

    def __init__(self, api_key: str) -> None:
        self.api_keys.append(api_key)

    def translate_text(self, text: str, *, target_lang: str):
        self.calls.append((text, target_lang))
        return _FakeDeepLResponse(f"deepl:{text}")


def test_deepl_provider_uses_api_key_and_en_us_target(monkeypatch) -> None:
    _FakeDeepLTranslator.api_keys.clear()
    _FakeDeepLTranslator.calls.clear()
    monkeypatch.setenv("LT_DEEPL_API_KEY", "test-key:fx")
    monkeypatch.setattr(translation_module.deepl, "Translator", _FakeDeepLTranslator)

    provider = DeepLTranslationProvider()
    result = provider.translate("Tablas de cortar de madera", "es", "en")

    assert _FakeDeepLTranslator.api_keys == ["test-key:fx"]
    assert _FakeDeepLTranslator.calls == [("Tablas de cortar de madera", "EN-US")]
    assert result.translated_text == "deepl:Tablas de cortar de madera"
    assert result.provider == "DEEPL_API"
    assert result.model_name == "deepl-api"
    assert result.metadata["backend"] == "deepl"
    assert result.metadata["target_lang"] == "EN-US"
    assert result.metadata["detected_source_lang"] == "ES"


def test_deepl_provider_requires_api_key(monkeypatch) -> None:
    monkeypatch.delenv("LT_DEEPL_API_KEY", raising=False)
    with pytest.raises(ValueError, match="LT_DEEPL_API_KEY"):
        DeepLTranslationProvider()


def test_configured_provider_accepts_deepl(monkeypatch) -> None:
    _FakeDeepLTranslator.api_keys.clear()
    monkeypatch.setenv("LT_DEEPL_API_KEY", "factory-key:fx")
    monkeypatch.setenv("LT_LACEY_TRANSLATION_PROVIDER", "deepl")
    monkeypatch.setattr(translation_module.deepl, "Translator", _FakeDeepLTranslator)

    provider = configured_translation_provider()

    assert isinstance(provider, DeepLTranslationProvider)
    assert _FakeDeepLTranslator.api_keys == ["factory-key:fx"]
