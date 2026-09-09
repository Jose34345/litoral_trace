"""Translation-provider abstraction for multilingual evidence processing.

Phase A/B foundation only: no production pipeline imports this module yet.
Translations are interpretations of immutable source spans and must never be
counted as independent evidence.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, runtime_checkable

import boto3


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


class AwsTranslateProvider:
    """Lazy AWS Translate adapter prepared for a later feature-flagged rollout.

    The boto3 client is created only on first use. Nothing in the current
    production extraction path instantiates or calls this provider in this PR.
    A client may be injected for deterministic tests.
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
