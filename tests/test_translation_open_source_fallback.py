from __future__ import annotations

from litoral_trace.services.translation import OpenSourceTranslationProvider


class _FailingPrimaryTranslator:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def translate(self, text: str) -> str:
        raise RuntimeError("primary unavailable")


class _CapturingFallbackTranslator:
    calls: list[dict[str, object]] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def translate(self, text: str) -> str:
        type(self).calls.append({"kwargs": self.kwargs, "text": text})
        return "translated"


def _translate_with_fallback(source: str):
    _CapturingFallbackTranslator.calls.clear()
    provider = OpenSourceTranslationProvider(
        translator_cls=_FailingPrimaryTranslator,
        fallback_translator_cls=_CapturingFallbackTranslator,
    )
    result = provider.translate("source text", source, "en")
    assert result.provider == provider.fallback_provider_name
    assert result.translated_text == "translated"
    assert len(_CapturingFallbackTranslator.calls) == 1
    return _CapturingFallbackTranslator.calls[0]["kwargs"]


def test_mymemory_fallback_uses_locale_codes_for_spanish() -> None:
    kwargs = _translate_with_fallback("es")
    assert kwargs["source"] == "es-ES"
    assert kwargs["target"] == "en-GB"


def test_mymemory_fallback_uses_locale_codes_for_portuguese() -> None:
    kwargs = _translate_with_fallback("pt")
    assert kwargs["source"] == "pt-PT"
    assert kwargs["target"] == "en-GB"


def test_mymemory_fallback_uses_locale_codes_for_chinese() -> None:
    kwargs = _translate_with_fallback("zh")
    assert kwargs["source"] == "zh-CN"
    assert kwargs["target"] == "en-GB"


def test_google_language_keeps_short_codes_and_normalizes_chinese() -> None:
    assert OpenSourceTranslationProvider._google_language("es") == "es"
    assert OpenSourceTranslationProvider._google_language("pt") == "pt"
    assert OpenSourceTranslationProvider._google_language("en") == "en"
    assert OpenSourceTranslationProvider._google_language("zh") == "zh-CN"
