from __future__ import annotations

from litoral_trace.db.models import DocumentTextSpan, DocumentTextTranslation
from litoral_trace.services.translation import NoOpEnglishProvider, TranslationResult
from litoral_trace.us_lacey.shadow_evidence_snapshot import (
    SHADOW_FLAG,
    SnapshotMetrics,
    _translate_span_if_eligible,
    detect_supported_language,
    multilingual_shadow_enabled,
)


def test_supported_language_detector_handles_en_es_pt_zh_and_keeps_codes_undefined():
    assert detect_supported_language("Country of harvest declared in Brazil").code == "en"
    assert detect_supported_language("País de cosecha declarado en Brasil").code == "es"
    assert detect_supported_language("País de colheita declarado no Brasil").code == "pt"
    assert detect_supported_language("采伐国家为中国，植物材料来自该地区").code == "zh"

    for fragment in ("4419199010", "KG", "TGHU4821932", "1440"):
        detection = detect_supported_language(fragment)
        assert detection.code == "und"
        assert detection.confidence == 0.0


def test_multilingual_shadow_feature_flag_is_fail_closed():
    assert multilingual_shadow_enabled({}) is False
    assert multilingual_shadow_enabled({SHADOW_FLAG: "0"}) is False
    assert multilingual_shadow_enabled({SHADOW_FLAG: "false"}) is False
    assert multilingual_shadow_enabled({SHADOW_FLAG: "1"}) is True
    assert multilingual_shadow_enabled({SHADOW_FLAG: "true"}) is True


class _CapturingSession:
    def __init__(self) -> None:
        self.added: list[object] = []

    def scalar(self, _statement):
        return None

    def add(self, value: object) -> None:
        self.added.append(value)


class _FakeTranslationProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def translate(self, text: str, source: str, target: str = "en") -> TranslationResult:
        self.calls.append((text, source, target))
        return TranslationResult(
            translated_text="Country of harvest declared in Brazil",
            source_language=source,
            target_language=target,
            provider="TEST_TRANSLATOR",
            model_name="deterministic",
            model_version="1",
        )


def _span(*, language: str, text: str) -> DocumentTextSpan:
    return DocumentTextSpan(
        id=17,
        organization_id=3,
        assurance_document_id=4,
        extraction_run_id=5,
        page=1,
        block_id="legacy-field:7",
        original_text=text,
        original_language=language,
        language_confidence=0.99,
        extraction_method="LEGACY_ADAPTER",
        content_hash="a" * 64,
    )


def test_non_english_span_is_translated_only_with_real_active_provider():
    metrics = SnapshotMetrics()
    session = _CapturingSession()
    provider = _FakeTranslationProvider()
    span = _span(language="es", text="País de cosecha declarado en Brasil")

    _translate_span_if_eligible(
        span=span,
        provider=provider,
        metrics=metrics,
        session=session,  # type: ignore[arg-type]
    )

    assert provider.calls == [(span.original_text, "es", "en")]
    assert metrics.translations_created == 1
    assert metrics.translations_omitted == 0
    translation = next(item for item in session.added if isinstance(item, DocumentTextTranslation))
    assert translation.source_span_id == span.id
    assert translation.source_language == "es"
    assert translation.target_language == "en"
    assert translation.translated_text == "Country of harvest declared in Brazil"


def test_no_provider_or_noop_provider_never_persists_false_non_english_translation():
    span = _span(language="pt", text="País de colheita declarado no Brasil")

    no_provider_metrics = SnapshotMetrics()
    no_provider_session = _CapturingSession()
    _translate_span_if_eligible(
        span=span,
        provider=None,
        metrics=no_provider_metrics,
        session=no_provider_session,  # type: ignore[arg-type]
    )
    assert no_provider_metrics.translations_omitted == 1
    assert no_provider_session.added == []

    noop_metrics = SnapshotMetrics()
    noop_session = _CapturingSession()
    _translate_span_if_eligible(
        span=span,
        provider=NoOpEnglishProvider(),
        metrics=noop_metrics,
        session=noop_session,  # type: ignore[arg-type]
    )
    assert noop_metrics.translations_omitted == 1
    assert noop_session.added == []


def test_english_and_undefined_spans_do_not_create_translation_rows():
    for language, text in (
        ("en", "Country of harvest declared in Brazil"),
        ("und", "4419199010"),
    ):
        metrics = SnapshotMetrics()
        session = _CapturingSession()
        _translate_span_if_eligible(
            span=_span(language=language, text=text),
            provider=_FakeTranslationProvider(),
            metrics=metrics,
            session=session,  # type: ignore[arg-type]
        )
        assert metrics.translations_created == 0
        assert metrics.translations_omitted == 0
        assert session.added == []
