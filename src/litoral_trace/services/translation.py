"""Translation-provider abstraction for multilingual evidence processing.

Translations are interpretations of immutable source spans and must never be
counted as independent evidence.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import logging
import os
from typing import Any, Mapping, Protocol, runtime_checkable

import boto3
import deepl
from deep_translator import GoogleTranslator, MyMemoryTranslator


LOGGER = logging.getLogger(__name__)
_DEFAULT_MYMEMORY_EMAIL = "soporte@litoraltrace.com"


@dataclass(frozen=True, slots=True)
class TranslationResult:
    translated_text: str
    source_language: str
    target_language: str
    provider: str
    model_name: str
    model_version: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


@runtime_checkable
class TranslationProvider(Protocol):
    def translate(
        self,
        text: str,
        source: str,
        target: str = "en",
    ) -> TranslationResult:
        """Translate one immutable source span without altering source evidence."""
        ...


class BaseTranslationProvider:
    """Small concrete base for translation providers with a shared contract."""

    def translate(
        self,
        text: str,
        source: str,
        target: str = "en",
    ) -> TranslationResult:
        raise NotImplementedError


class NoOpEnglishProvider:
    """Deterministic provider for already-English source text."""

    provider_name = "NOOP_ENGLISH"
    model_name = "identity"
    model_version = "1"

    def translate(
        self,
        text: str,
        source: str,
        target: str = "en",
    ) -> TranslationResult:
        source_norm = (source or "").strip().lower()
        target_norm = (target or "").strip().lower()
        if source_norm not in {"en", "eng", "en-us", "en-gb"}:
            raise ValueError("NoOpEnglishProvider only accepts English source text")
        if target_norm not in {"en", "eng", "en-us", "en-gb"}:
            raise ValueError("NoOpEnglishProvider only supports an English target")
        return TranslationResult(
            translated_text=text,
            source_language=source,
            target_language=target,
            provider=self.provider_name,
            model_name=self.model_name,
            model_version=self.model_version,
            metadata={"identity_translation": True},
        )


class OpenSourceTranslationProvider:
    """Free deep-translator adapter with a Google -> MyMemory fallback chain.

    Neither backend uses the paid Google Cloud Translation API. Translator
    classes are injectable so tests remain deterministic and never need outbound
    network access. Backend failures are logged without source text; if every
    engine fails, the final exception is deliberately allowed to propagate.
    """

    provider_name = "OPEN_SOURCE_GOOGLE"
    model_name = "deep-translator-google"
    model_version = "1"
    fallback_provider_name = "OPEN_SOURCE_MYMEMORY"
    fallback_model_name = "deep-translator-mymemory"
    fallback_model_version = "1"

    def __init__(
        self,
        *,
        translator_cls: Any = GoogleTranslator,
        fallback_translator_cls: Any = MyMemoryTranslator,
    ) -> None:
        self._translator_cls = translator_cls
        self._fallback_translator_cls = fallback_translator_cls

    @staticmethod
    def _backend_language(code: str) -> str:
        normalized = (code or "").strip().lower()
        # deep-translator expects its Chinese locale code rather than the shadow
        # detector's intentionally generic ISO-639 ``zh`` value.
        if normalized == "zh":
            return "zh-CN"
        return normalized

    @staticmethod
    def _error_code(error: Exception) -> Any | None:
        """Best-effort status/code extraction without logging error payload text."""
        response = getattr(error, "response", None)
        for candidate in (
            getattr(error, "status_code", None),
            getattr(error, "code", None),
            getattr(response, "status_code", None),
        ):
            if candidate is not None:
                return candidate
        return None

    @staticmethod
    def _mymemory_contact_email() -> str:
        configured = str(os.getenv("LT_MYMEMORY_EMAIL") or "").strip()
        return configured or _DEFAULT_MYMEMORY_EMAIL

    def _translate_with(
        self,
        translator_cls: Any,
        *,
        text: str,
        source: str,
        target: str,
        email: str | None = None,
    ) -> str:
        kwargs: dict[str, Any] = {
            "source": self._backend_language(source),
            "target": self._backend_language(target),
        }
        if email is not None:
            kwargs["email"] = email
        translator = translator_cls(**kwargs)
        translated = str(translator.translate(text) or "").strip()
        if not translated:
            raise RuntimeError("Open-source translation engine returned an empty translation")
        return translated

    def translate(
        self,
        text: str,
        source: str,
        target: str = "en",
    ) -> TranslationResult:
        source_norm = (source or "").strip().lower()
        target_norm = (target or "").strip().lower()
        original_text = str(text or "")
        if not original_text.strip():
            return TranslationResult(
                translated_text=original_text,
                source_language=source_norm,
                target_language=target_norm,
                provider=self.provider_name,
                model_name=self.model_name,
                model_version=self.model_version,
                metadata={"backend": "deep-translator", "engine": "google"},
            )
        if not source_norm:
            raise ValueError("A source language is required for translation")
        if not target_norm:
            raise ValueError("A target language is required for translation")

        try:
            translated = self._translate_with(
                self._translator_cls,
                text=original_text,
                source=source_norm,
                target=target_norm,
            )
        except Exception as google_error:
            LOGGER.warning(
                "Open-source translation engine failed; trying fallback.",
                extra={
                    "translation_engine": self.provider_name,
                    "translation_fallback_engine": self.fallback_provider_name,
                    "translation_source_language": source_norm,
                    "translation_target_language": target_norm,
                    "translation_error_type": type(google_error).__name__,
                    "translation_error_code": self._error_code(google_error),
                },
            )
            try:
                translated = self._translate_with(
                    self._fallback_translator_cls,
                    text=original_text,
                    source=source_norm,
                    target=target_norm,
                    email=self._mymemory_contact_email(),
                )
            except Exception as fallback_error:
                LOGGER.warning(
                    "Open-source translation fallback engine failed; no engines remain.",
                    extra={
                        "translation_engine": self.fallback_provider_name,
                        "translation_source_language": source_norm,
                        "translation_target_language": target_norm,
                        "translation_error_type": type(fallback_error).__name__,
                        "translation_error_code": self._error_code(fallback_error),
                    },
                )
                raise
            return TranslationResult(
                translated_text=translated,
                source_language=source_norm,
                target_language=target_norm,
                provider=self.fallback_provider_name,
                model_name=self.fallback_model_name,
                model_version=self.fallback_model_version,
                metadata={
                    "backend": "deep-translator",
                    "engine": "mymemory",
                    "fallback_from": self.provider_name,
                },
            )

        return TranslationResult(
            translated_text=translated,
            source_language=source_norm,
            target_language=target_norm,
            provider=self.provider_name,
            model_name=self.model_name,
            model_version=self.model_version,
            # deep-translator does not expose a calibrated confidence/quality
            # score, so leave quality_score absent instead of inventing certainty.
            metadata={"backend": "deep-translator", "engine": "google"},
        )


class DeepLTranslationProvider(BaseTranslationProvider):
    """Official DeepL API adapter for production multilingual evidence translation."""

    provider_name = "DEEPL_API"
    model_name = "deepl-api"
    model_version = "v2"

    def __init__(self) -> None:
        api_key = str(os.getenv("LT_DEEPL_API_KEY") or "").strip()
        if not api_key:
            raise ValueError(
                "LT_DEEPL_API_KEY is required when LT_LACEY_TRANSLATION_PROVIDER=deepl"
            )
        self.translator = deepl.Translator(api_key)

    @staticmethod
    def _target_language(code: str) -> str:
        normalized = str(code or "").strip().lower()
        mapping = {
            "en": "EN-US",
            "eng": "EN-US",
            "en-us": "EN-US",
            "en-gb": "EN-GB",
            "es": "ES",
            "spa": "ES",
            "pt": "PT-BR",
            "pt-br": "PT-BR",
            "pt-pt": "PT-PT",
            "zh": "ZH-HANS",
            "zh-cn": "ZH-HANS",
        }
        return mapping.get(normalized, normalized.upper())

    def translate(
        self,
        text: str,
        source: str,
        target: str = "en",
    ) -> TranslationResult:
        source_norm = str(source or "").strip().lower()
        target_norm = str(target or "").strip().lower()
        original_text = str(text or "")
        if not source_norm:
            raise ValueError("A source language is required for translation")
        if not target_norm:
            raise ValueError("A target language is required for translation")

        target_lang = self._target_language(target_norm)
        if not original_text.strip():
            return TranslationResult(
                translated_text=original_text,
                source_language=source_norm,
                target_language=target_norm,
                provider=self.provider_name,
                model_name=self.model_name,
                model_version=self.model_version,
                metadata={"backend": "deepl", "target_lang": target_lang},
            )

        response = self.translator.translate_text(
            original_text,
            target_lang=target_lang,
        )
        translated = str(getattr(response, "text", response) or "").strip()
        if not translated:
            raise RuntimeError("DeepL API returned an empty translation")
        detected_source = str(getattr(response, "detected_source_lang", "") or "").strip()
        metadata: dict[str, Any] = {"backend": "deepl", "target_lang": target_lang}
        if detected_source:
            metadata["detected_source_lang"] = detected_source
        return TranslationResult(
            translated_text=translated,
            source_language=source_norm,
            target_language=target_norm,
            provider=self.provider_name,
            model_name=self.model_name,
            model_version=self.model_version,
            metadata=metadata,
        )


class AwsTranslateProvider:
    """Lazy AWS Translate adapter kept as an optional provider.

    The boto3 client is created only on first use. A client may be injected for
    deterministic tests.
    """

    provider_name = "AWS_TRANSLATE"
    model_name = "aws-translate"
    model_version = "api-v1"

    def __init__(self, *, client: Any | None = None, region_name: str | None = None) -> None:
        self._client = client
        self._region_name = region_name

    def _get_client(self) -> Any:
        if self._client is None:
            self._client = boto3.client("translate", region_name=self._region_name)
        return self._client

    def translate(
        self,
        text: str,
        source: str,
        target: str = "en",
    ) -> TranslationResult:
        response = self._get_client().translate_text(
            Text=text,
            SourceLanguageCode=source,
            TargetLanguageCode=target,
        )
        translated = str(response.get("TranslatedText") or "")
        if not translated:
            raise RuntimeError("AWS Translate returned an empty translation")
        return TranslationResult(
            translated_text=translated,
            source_language=str(response.get("SourceLanguageCode") or source),
            target_language=str(response.get("TargetLanguageCode") or target),
            provider=self.provider_name,
            model_name=self.model_name,
            model_version=self.model_version,
            metadata={
                "applied_terminologies": response.get("AppliedTerminologies") or [],
            },
        )