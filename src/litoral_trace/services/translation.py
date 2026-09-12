"""Translation-provider abstraction for multilingual evidence processing.

Translations are interpretations of immutable source spans and must never be
counted as independent evidence.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, runtime_checkable

import boto3
from deep_translator import GoogleTranslator


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
    """Free deep-translator adapter using its Google Translate backend.

    This backend does not use the paid Google Cloud Translation API and therefore
    needs no cloud API key. The translator class is injectable so tests remain
    deterministic and never need outbound network access.
    """

    provider_name = "OPEN_SOURCE_GOOGLE"
    model_name = "deep-translator-google"
    model_version = "1"

    def __init__(self, *, translator_cls: Any = GoogleTranslator) -> None:
        self._translator_cls = translator_cls

    @staticmethod
    def _backend_language(code: str) -> str:
        normalized = (code or "").strip().lower()
        # deep-translator/Google expects its Chinese locale code rather than the
        # shadow detector's intentionally generic ISO-639 ``zh`` value.
        if normalized == "zh":
            return "zh-CN"
        return normalized

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
                metadata={"backend": "deep-translator"},
            )
        if not source_norm:
            raise ValueError("A source language is required for translation")
        if not target_norm:
            raise ValueError("A target language is required for translation")

        translator = self._translator_cls(
            source=self._backend_language(source_norm),
            target=self._backend_language(target_norm),
        )
        translated = str(translator.translate(original_text) or "").strip()
        if not translated:
            raise RuntimeError("Open-source translation provider returned an empty translation")
        return TranslationResult(
            translated_text=translated,
            source_language=source_norm,
            target_language=target_norm,
            provider=self.provider_name,
            model_name=self.model_name,
            model_version=self.model_version,
            # deep-translator does not expose a calibrated confidence/quality
            # score, so leave quality_score absent instead of inventing certainty.
            metadata={"backend": "deep-translator"},
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
