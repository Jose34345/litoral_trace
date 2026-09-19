from __future__ import annotations

import logging

import pytest

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


class _GoogleRateLimitError(RuntimeError):
    status_code = 429


class _FailingGoogleTranslator:
    init_calls: list[tuple[str, str]] = []

    def __init__(self, *, source: str, target: str) -> None:
        self.source = source
        self.target = target
        self.init_calls.append((source, target))

    def translate(self, text: str) -> str:
        raise _GoogleRateLimitError("rate limited")


class _FakeMyMemoryTranslator:
    init_calls: list[tuple[str, str, str]] = []
    translated_texts: list[str] = []

    def __init__(self, *, source: str, target: str, email: str) -> None:
        self.source = source
        self.target = target
        self.email = email
        self.init_calls.append((source, target, email))

    def translate(self, text: str) -> str:
        self.translated_texts.append(text)
        return f"fallback:{text}"


class _FailingMyMemoryTranslator:
    def __init__(self, *, source: str, target: str, email: str) -> None:
        self.source = source
        self.target = target
        self.email = email

    def translate(self, text: str) -> str:
        raise RuntimeError("secondary translator unavailable")


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
    assert result.metadata["engine"] == "google"
    assert _FakeGoogleTranslator.init_calls == [("es", "en")]
    assert _FakeGoogleTranslator.translated_texts == ["Factura comercial de madera"]


def test_open_source_provider_falls_back_to_mymemory_when_google_fails(
    caplog,
    monkeypatch,
) -> None:
    monkeypatch.setenv("LT_MYMEMORY_EMAIL", "translation-test@example.com")
    _FailingGoogleTranslator.init_calls.clear()
    _FakeMyMemoryTranslator.init_calls.clear()
    _FakeMyMemoryTranslator.translated_texts.clear()
    provider = OpenSourceTranslationProvider(
        translator_cls=_FailingGoogleTranslator,
        fallback_translator_cls=_FakeMyMemoryTranslator,
    )

    with caplog.at_level(logging.WARNING, logger="litoral_trace.services.translation"):
        result = provider.translate("Tablas de cortar de madera", "es", "en")

    assert result.translated_text == "fallback:Tablas de cortar de madera"
    assert result.source_language == "es"
    assert result.target_language == "en"
    assert result.provider == "OPEN_SOURCE_MYMEMORY"
    assert result.model_name == "deep-translator-mymemory"
    assert result.model_version == "1"
    assert result.metadata == {
        "backend": "deep-translator",
        "engine": "mymemory",
        "fallback_from": "OPEN_SOURCE_GOOGLE",
    }
    assert _FailingGoogleTranslator.init_calls == [("es", "en")]
    assert _FakeMyMemoryTranslator.init_calls == [
        ("es", "en", "translation-test@example.com")
    ]
    assert _FakeMyMemoryTranslator.translated_texts == ["Tablas de cortar de madera"]

    warning = next(
        record
        for record in caplog.records
        if record.getMessage() == "Open-source translation engine failed; trying fallback."
    )
    assert warning.translation_engine == "OPEN_SOURCE_GOOGLE"
    assert warning.translation_fallback_engine == "OPEN_SOURCE_MYMEMORY"
    assert warning.translation_error_type == "_GoogleRateLimitError"
    assert warning.translation_error_code == 429


def test_open_source_provider_uses_default_mymemory_contact_email(monkeypatch) -> None:
    monkeypatch.delenv("LT_MYMEMORY_EMAIL", raising=False)
    _FakeMyMemoryTranslator.init_calls.clear()
    provider = OpenSourceTranslationProvider(
        translator_cls=_FailingGoogleTranslator,
        fallback_translator_cls=_FakeMyMemoryTranslator,
    )

    provider.translate("Madera aserrada", "es", "en")

    assert _FakeMyMemoryTranslator.init_calls == [
        ("es", "en", "soporte@litoraltrace.com")
    ]


def test_open_source_provider_raises_when_all_engines_fail(caplog) -> None:
    provider = OpenSourceTranslationProvider(
        translator_cls=_FailingGoogleTranslator,
        fallback_translator_cls=_FailingMyMemoryTranslator,
    )

    with caplog.at_level(logging.WARNING, logger="litoral_trace.services.translation"):
        with pytest.raises(RuntimeError, match="secondary translator unavailable"):
            provider.translate("Tablas de cortar de madera", "es", "en")

    messages = [record.getMessage() for record in caplog.records]
    assert "Open-source translation engine failed; trying fallback." in messages
    assert "Open-source translation fallback engine failed; no engines remain." in messages


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