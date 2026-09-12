"""Bounded, best-effort backfill for historical multilingual source spans.

The live U.S. Lacey read/write workflow never waits for this module. One startup
attempt processes at most 50 immutable spans and then exits; any remainder is
left for a later process restart.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
import hashlib
import logging

from sqlalchemy import and_, select

from litoral_trace.db.models import DocumentTextSpan, DocumentTextTranslation
from litoral_trace.db.models.organization import Organization
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.translation import (
    OpenSourceTranslationProvider,
    TranslationProvider,
    TranslationResult,
)
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.shadow_evidence_snapshot import configured_translation_provider


LOGGER = logging.getLogger(__name__)
BACKFILL_LIMIT = 50
BACKFILL_DELAY_SECONDS = 0.5
TRANSLATABLE_LANGUAGES = ("es", "pt", "zh")
TARGET_LANGUAGE = "en"


@dataclass(frozen=True, slots=True)
class TranslationBackfillCandidate:
    organization_id: int
    source_span_id: int
    original_text: str
    original_language: str


@dataclass(frozen=True, slots=True)
class TranslationBackfillWrite:
    candidate: TranslationBackfillCandidate
    result: TranslationResult


LoadCandidates = Callable[[int], Sequence[TranslationBackfillCandidate]]
PersistWrites = Callable[[Sequence[TranslationBackfillWrite]], int]
Sleeper = Callable[[float], Awaitable[None]]


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _organization_ids() -> tuple[int, ...]:
    session = get_us_lacey_db_session()
    try:
        return tuple(
            int(value)
            for value in session.scalars(
                select(Organization.id).order_by(Organization.id.asc())
            ).all()
        )
    finally:
        session.close()


def _load_missing_candidates(limit: int) -> tuple[TranslationBackfillCandidate, ...]:
    """Load at most the hard process budget while respecting tenant RLS."""
    hard_limit = min(max(int(limit), 0), BACKFILL_LIMIT)
    if hard_limit == 0:
        return ()

    candidates: list[TranslationBackfillCandidate] = []
    for organization_id in _organization_ids():
        remaining = hard_limit - len(candidates)
        if remaining <= 0:
            break
        session = get_us_lacey_db_session()
        try:
            set_tenant_db_context(session, organization_id)
            spans = session.scalars(
                select(DocumentTextSpan)
                .outerjoin(
                    DocumentTextTranslation,
                    and_(
                        DocumentTextTranslation.organization_id
                        == DocumentTextSpan.organization_id,
                        DocumentTextTranslation.source_span_id == DocumentTextSpan.id,
                        DocumentTextTranslation.target_language == TARGET_LANGUAGE,
                    ),
                )
                .where(
                    DocumentTextSpan.organization_id == organization_id,
                    DocumentTextSpan.original_language.in_(TRANSLATABLE_LANGUAGES),
                    DocumentTextTranslation.id.is_(None),
                )
                .order_by(DocumentTextSpan.id.asc())
                .limit(remaining)
            ).all()
            candidates.extend(
                TranslationBackfillCandidate(
                    organization_id=organization_id,
                    source_span_id=int(span.id),
                    original_text=str(span.original_text or ""),
                    original_language=str(span.original_language or "und"),
                )
                for span in spans
            )
        finally:
            session.close()
    # Defense in depth: no loader refactor may raise the per-startup budget.
    return tuple(candidates[:BACKFILL_LIMIT])


def _persist_writes(writes: Sequence[TranslationBackfillWrite]) -> int:
    """Commit one small transaction per tenant; source spans are never mutated."""
    by_organization: dict[int, list[TranslationBackfillWrite]] = {}
    for write in writes[:BACKFILL_LIMIT]:
        by_organization.setdefault(write.candidate.organization_id, []).append(write)

    created = 0
    for organization_id, organization_writes in by_organization.items():
        session = get_us_lacey_db_session()
        try:
            set_tenant_db_context(session, organization_id)
            for write in organization_writes:
                candidate = write.candidate
                result = write.result
                # Recheck after the network call so concurrent startup attempts are
                # harmless even when two instances briefly overlap during a deploy.
                existing = session.scalar(
                    select(DocumentTextTranslation.id)
                    .where(
                        DocumentTextTranslation.organization_id == organization_id,
                        DocumentTextTranslation.source_span_id == candidate.source_span_id,
                        DocumentTextTranslation.target_language == TARGET_LANGUAGE,
                    )
                    .limit(1)
                )
                if existing is not None:
                    continue
                translated_text = str(result.translated_text or "").strip()
                if not translated_text:
                    continue
                quality = result.metadata.get("quality_score") if result.metadata else None
                session.add(
                    DocumentTextTranslation(
                        organization_id=organization_id,
                        source_span_id=candidate.source_span_id,
                        source_language=candidate.original_language,
                        target_language=TARGET_LANGUAGE,
                        translated_text=translated_text,
                        provider=result.provider,
                        model_name=result.model_name,
                        model_version=result.model_version,
                        quality_score=(
                            float(quality) if isinstance(quality, (int, float)) else None
                        ),
                        input_hash=_sha256_text(candidate.original_text),
                        output_hash=_sha256_text(translated_text),
                    )
                )
                created += 1
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    return created


async def _run_translation_backfill_batch(
    *,
    provider: TranslationProvider,
    load_candidates: LoadCandidates = _load_missing_candidates,
    persist_writes: PersistWrites = _persist_writes,
    sleeper: Sleeper = asyncio.sleep,
) -> int:
    candidates = tuple(
        (await asyncio.to_thread(load_candidates, BACKFILL_LIMIT))[:BACKFILL_LIMIT]
    )
    if not candidates:
        return 0

    writes: list[TranslationBackfillWrite] = []
    for index, candidate in enumerate(candidates):
        try:
            result = await asyncio.to_thread(
                provider.translate,
                candidate.original_text,
                candidate.original_language,
                TARGET_LANGUAGE,
            )
            if str(result.translated_text or "").strip():
                writes.append(TranslationBackfillWrite(candidate=candidate, result=result))
        except Exception:
            # A public provider can be temporarily unavailable. Keep immutable source
            # evidence untouched and leave this span eligible for the next restart.
            LOGGER.warning(
                "us_lacey_translation_backfill_span_failed source_span_id=%s language=%s",
                candidate.source_span_id,
                candidate.original_language,
                exc_info=True,
            )
        if index + 1 < len(candidates):
            await sleeper(BACKFILL_DELAY_SECONDS)

    if not writes:
        return 0
    created = await asyncio.to_thread(persist_writes, tuple(writes))
    LOGGER.info(
        "us_lacey_translation_backfill_complete selected=%s translated=%s persisted=%s limit=%s",
        len(candidates),
        len(writes),
        created,
        BACKFILL_LIMIT,
    )
    return int(created)


async def run_translation_backfill(
    *,
    provider: TranslationProvider | None = None,
    load_candidates: LoadCandidates = _load_missing_candidates,
    persist_writes: PersistWrites = _persist_writes,
    sleeper: Sleeper = asyncio.sleep,
) -> int:
    """Best-effort public entrypoint designed for a detached FastAPI startup task."""
    try:
        selected_provider = provider
        if selected_provider is None:
            selected_provider = configured_translation_provider()
            # Never make the automatic repair path silently fall back to a paid API.
            if not isinstance(selected_provider, OpenSourceTranslationProvider):
                return 0
        if selected_provider is None:
            return 0
        return await _run_translation_backfill_batch(
            provider=selected_provider,
            load_candidates=load_candidates,
            persist_writes=persist_writes,
            sleeper=sleeper,
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        LOGGER.warning(
            "us_lacey_translation_backfill_failed retry=next_startup",
            exc_info=True,
        )
        return 0
