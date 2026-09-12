from __future__ import annotations

from litoral_trace.services.translation import OpenSourceTranslationProvider
from litoral_trace.us_lacey.shadow_evidence_snapshot import configured_translation_provider


class _FakeGoogleTranslator:
    init_calls: list[tuple[str, str]] = []
    translated_texts: list[str] = []

    def __init__(self, *, source: str, target: str) -> None:
        self.source = source
        self.target = target
        self.init_calls.append((source, target))

    def translate(self, text: str) -> str:
        self.translated_texts.append(text)
        return f"translated:{text}"


def test_open_source_provider_translates_without_cloud_credentials() -> None:
    _FakeGoogleTranslator.init_calls.clear()
    _FakeGoogleTranslator.translated_texts.clear()
    provider = OpenSourceTranslationProvider(translator_cls=_FakeGoogleTranslator)

    result = provider.translate("Factura comercial de madera", "es", "en")

    assert result.translated_text == "translated:Factura comercial de madera"
    assert result.source_language == "es"
    assert result.target_language == "en"
    assert result.provider == "OPEN_SOURCE_GOOGLE"
    assert result.model_name == "deep-translator-google"
    assert result.model_version == "1"
    assert _FakeGoogleTranslator.init_calls == [("es", "en")]
    assert _FakeGoogleTranslator.translated_texts == ["Factura comercial de madera"]


def test_open_source_provider_maps_generic_chinese_code() -> None:
    _FakeGoogleTranslator.init_calls.clear()
    provider = OpenSourceTranslationProvider(translator_cls=_FakeGoogleTranslator)

    result = provider.translate("中华人民共和国木材出口", "zh", "en")

    assert result.source_language == "zh"
    assert _FakeGoogleTranslator.init_calls == [("zh-CN", "en")]


def test_configured_provider_accepts_open_source(monkeypatch) -> None:
    monkeypatch.setenv("LT_LACEY_TRANSLATION_PROVIDER", "open_source")

    provider = configured_translation_provider()

    assert isinstance(provider, OpenSourceTranslationProvider)


def test_configured_provider_accepts_free_alias(monkeypatch) -> None:
    monkeypatch.setenv("LT_LACEY_TRANSLATION_PROVIDER", "free")

    provider = configured_translation_provider()

    assert isinstance(provider, OpenSourceTranslationProvider)
