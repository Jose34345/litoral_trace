from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace

from fastapi import BackgroundTasks
from sqlalchemy.dialects import postgresql

from litoral_trace.services.translation import TranslationResult
from litoral_trace.us_lacey.semantic_evidence_read import EvidenceTextView, evidence_text_view
from litoral_trace.us_lacey.translation_backfill import (
    BACKFILL_DELAY_SECONDS,
    BACKFILL_LIMIT,
    TranslationBackfillCandidate,
    _missing_candidates_statement,
    run_translation_backfill,
)
from litoral_trace.web import us_lacey_pilot_app as pilot_app
from litoral_trace.web.us_lacey_operational_views import _decorate_review_fields


def test_read_projection_falls_back_to_original_text_without_translation():
    span = SimpleNamespace(
        id=42,
        assurance_document_id=7,
        page=3,
        source_locator="page:3:block:2",
        original_text="Factura comercial de productos de madera",
        original_language="es",
    )

    view = evidence_text_view(
        span=span,
        target_field="merchandise_description",
        translation=None,
    )

    assert view.original_text == "Factura comercial de productos de madera"
    assert view.original_language == "es"
    assert view.translated_text is None
    assert view.display_text == view.original_text
    assert view.is_translated is False


def test_read_projection_prefers_translation_but_preserves_original():
    span = SimpleNamespace(
        id=43,
        assurance_document_id=7,
        page=4,
        source_locator="page:4:block:1",
        original_text="Madeira serrada de pinus",
        original_language="pt",
    )
    translation = SimpleNamespace(translated_text="Sawn pine wood")

    view = evidence_text_view(
        span=span,
        target_field="merchandise_description",
        translation=translation,
    )

    assert view.translated_text == "Sawn pine wood"
    assert view.display_text == "Sawn pine wood"
    assert view.is_translated is True
    assert view.original_text == "Madeira serrada de pinus"
    assert view.original_language_label == "Portuguese"


def test_backfill_query_is_explicitly_tenant_scoped():
    statement = _missing_candidates_statement(14, BACKFILL_LIMIT)
    sql = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    ).lower()

    assert "document_text_spans" in sql
    assert "document_text_translations" in sql
    assert "organizations" not in sql
    assert "document_text_spans.organization_id = 14" in sql
    assert "document_text_translations.id is null" in sql
    assert "original_language" in sql
    assert "'es'" in sql
    assert "'pt'" in sql
    assert "'zh'" in sql
    assert "target_language = 'en'" in sql
    assert f"limit {BACKFILL_LIMIT}" in sql


class _FakeProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def translate(self, text: str, source: str, target: str = "en") -> TranslationResult:
        self.calls.append((text, source, target))
        return TranslationResult(
            translated_text=f"EN:{text}",
            source_language=source,
            target_language=target,
            provider="OPEN_SOURCE_GOOGLE",
            model_name="test-provider",
            model_version="1",
            metadata={},
        )


def test_backfill_hard_caps_at_50_and_yields_between_translations():
    provider = _FakeProvider()
    persisted = []
    sleeps = []

    def load_candidates(organization_id: int, limit: int):
        assert organization_id == 7
        assert limit == BACKFILL_LIMIT
        # Deliberately violate the loader contract to prove the async layer still
        # enforces the innegotiable per-trigger startup budget.
        return tuple(
            TranslationBackfillCandidate(
                organization_id=7,
                source_span_id=index + 1,
                original_text=f"texto {index + 1}",
                original_language="es",
            )
            for index in range(BACKFILL_LIMIT + 10)
        )

    def persist_writes(organization_id: int, writes):
        assert organization_id == 7
        persisted.extend(writes)
        return len(writes)

    async def fake_sleep(delay: float):
        sleeps.append(delay)

    created = asyncio.run(
        run_translation_backfill(
            7,
            provider=provider,
            load_candidates=load_candidates,
            persist_writes=persist_writes,
            sleeper=fake_sleep,
        )
    )

    assert created == BACKFILL_LIMIT
    assert len(provider.calls) == BACKFILL_LIMIT
    assert len(persisted) == BACKFILL_LIMIT
    assert sleeps == [BACKFILL_DELAY_SECONDS] * (BACKFILL_LIMIT - 1)


def test_jit_backfill_schedules_only_once_per_organization():
    tasks = BackgroundTasks()
    with pilot_app._TRANSLATION_BACKFILL_SCHEDULE_LOCK:
        pilot_app._TRANSLATION_BACKFILL_SCHEDULED_ORGANIZATIONS.clear()
    try:
        assert pilot_app._schedule_translation_backfill_once(tasks, 14) is True
        assert pilot_app._schedule_translation_backfill_once(tasks, 14) is False
        assert pilot_app._schedule_translation_backfill_once(tasks, 15) is True
        assert len(tasks.tasks) == 2
    finally:
        with pilot_app._TRANSLATION_BACKFILL_SCHEDULE_LOCK:
            pilot_app._TRANSLATION_BACKFILL_SCHEDULED_ORGANIZATIONS.clear()


def test_operations_endpoint_schedules_jit_backfill_for_authenticated_tenant(monkeypatch):
    identity = SimpleNamespace(organization_id=14)
    entitlement = SimpleNamespace()
    monkeypatch.setattr(
        pilot_app,
        "_operational_context",
        lambda _session: (identity, entitlement),
    )
    monkeypatch.setattr(
        pilot_app,
        "UsLaceyOperationService",
        lambda: SimpleNamespace(
            list_operations=lambda *, organization_id, limit: []
        ),
    )
    monkeypatch.setattr(
        pilot_app,
        "render_operations",
        lambda **_kwargs: "<main>ok</main>",
    )
    scheduled: list[int] = []
    monkeypatch.setattr(
        pilot_app,
        "_schedule_translation_backfill_once",
        lambda _tasks, organization_id: scheduled.append(organization_id) or True,
    )

    response = pilot_app.operations_page(
        request=SimpleNamespace(),
        background_tasks=BackgroundTasks(),
        us_session="opaque-session",
    )

    assert response.status_code == 200
    assert scheduled == [14]


@dataclass(frozen=True)
class _ReviewField:
    field_name: str
    proposed_value: str
    source_assurance_document_id: int | None
    source_page: int | None


def test_review_card_uses_display_text_and_keeps_original_in_tooltip():
    field = _ReviewField(
        field_name="merchandise_description",
        proposed_value="Normalized merchandise description",
        source_assurance_document_id=7,
        source_page=3,
    )
    evidence = EvidenceTextView(
        source_span_id=42,
        target_field="merchandise_description",
        original_text="Descripción de mercancía",
        original_language="es",
        translated_text="Merchandise description",
        display_text="Merchandise description",
        is_translated=True,
        original_language_label="Spanish",
        source_assurance_document_id=7,
        source_page=3,
        source_locator="page:3:block:2",
    )

    decorated = _decorate_review_fields(
        [field],
        {"merchandise_description": (evidence,)},
    )[0]
    rendered = str(decorated.proposed_value)

    assert "Evidence:</span> Merchandise description" in rendered
    assert "Translated from Spanish" in rendered
    assert 'title="Original evidence: Descripción de mercancía"' in rendered
    # Form/edit value remains separate; only the customer-facing proposed-value
    # presentation is enriched with semantic evidence.
    assert rendered.startswith("Normalized merchandise description")
