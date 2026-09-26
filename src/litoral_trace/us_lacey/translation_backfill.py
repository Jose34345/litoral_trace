"""Bounded, best-effort backfill for historical multilingual source spans.

The live U.S. Lacey read/write workflow never waits for this module. Each
request-triggered attempt is scoped to one authenticated tenant, processes at
most 50 immutable spans, and then exits.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
import hashlib
import logging

from sqlalchemy import and_, select

from litoral_trace.db.models import DocumentTextSpan, DocumentTextTranslation
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


LoadCandidates = Callable[[int, int], Sequence[TranslationBackfillCandidate]]
PersistWrites = Callable[[int, Sequence[TranslationBackfillWrite]], int]
Sleeper = Callable[[float], Awaitable[None]]


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _missing_candidates_statement(organization_id: int, limit: int):
    """Build the tenant-scoped missing-translation query.

    The caller must also install the same organization in PostgreSQL's RLS GUC
    before executing the statement. Keeping the explicit predicate as well as
    RLS gives defense in depth and makes accidental cross-tenant reads fail
    closed.
    """
    tenant_id = int(organization_id)
    hard_limit = min(max(int(limit), 0), BACKFILL_LIMIT)
    return (
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
            DocumentTextSpan.organization_id == tenant_id,
            DocumentTextSpan.original_language.in_(TRANSLATABLE_LANGUAGES),
            DocumentTextTranslation.id.is_(None),
        )
        .order_by(DocumentTextSpan.id.asc())
        .limit(hard_limit)
    )


def _load_missing_candidates(
    organization_id: int,
    limit: int,
) -> tuple[TranslationBackfillCandidate, ...]:
    """Load only the authenticated tenant's candidates under FORCE RLS."""
    hard_limit = min(max(int(limit), 0), BACKFILL_LIMIT)
    if hard_limit == 0:
        return ()

    session = get_us_lacey_db_session()
    try:
        tenant_id = set_tenant_db_context(session, organization_id)
        spans = session.scalars(
            _missing_candidates_statement(tenant_id, hard_limit)
        ).all()
        return tuple(
            TranslationBackfillCandidate(
                organization_id=tenant_id,
                source_span_id=int(span.id),
                original_text=str(span.original_text or ""),
                original_language=str(span.original_language or "und"),
            )
            for span in spans[:BACKFILL_LIMIT]
        )
    finally:
        session.close()


def _persist_writes(
    organization_id: int,
    writes: Sequence[TranslationBackfillWrite],
) -> int:
    """Persist one tenant-only transaction; source spans are never mutated."""
    tenant_id = int(organization_id)
    tenant_writes = tuple(writes[:BACKFILL_LIMIT])
    if any(write.candidate.organization_id != tenant_id for write in tenant_writes):
        raise ValueError("translation backfill candidate tenant mismatch")
    if not tenant_writes:
        return 0

    created = 0
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, tenant_id)
        for write in tenant_writes:
            candidate = write.candidate
            result = write.result
            # Recheck after the network call so overlapping requests remain
            # harmless even if two web instances briefly schedule the same tenant.
            existing = session.scalar(
                select(DocumentTextTranslation.id)
                .where(
                    DocumentTextTranslation.organization_id == tenant_id,
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
                    organization_id=tenant_id,
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
    organization_id: int,
    provider: TranslationProvider,
    load_candidates: LoadCandidates = _load_missing_candidates,
    persist_writes: PersistWrites = _persist_writes,
    sleeper: Sleeper = asyncio.sleep,
) -> int:
    tenant_id = int(organization_id)
    candidates = tuple(
        (
            await asyncio.to_thread(
                load_candidates,
                tenant_id,
                BACKFILL_LIMIT,
            )
        )[:BACKFILL_LIMIT]
    )
    if any(candidate.organization_id != tenant_id for candidate in candidates):
        raise ValueError("translation backfill loader returned another tenant")
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
            # evidence untouched and leave this span eligible for a later tenant trigger.
            LOGGER.warning(
                "us_lacey_translation_backfill_span_failed organization_id=%s "
                "source_span_id=%s language=%s",
                tenant_id,
                candidate.source_span_id,
                candidate.original_language,
                exc_info=True,
            )
        if index + 1 < len(candidates):
            await sleeper(BACKFILL_DELAY_SECONDS)

    if not writes:
        return 0
    created = await asyncio.to_thread(
        persist_writes,
        tenant_id,
        tuple(writes),
    )
    LOGGER.info(
        "us_lacey_translation_backfill_complete organization_id=%s selected=%s "
        "translated=%s persisted=%s limit=%s",
        tenant_id,
        len(candidates),
        len(writes),
        created,
        BACKFILL_LIMIT,
    )
    return int(created)


async def run_translation_backfill(
    organization_id: int,
    *,
    provider: TranslationProvider | None = None,
    load_candidates: LoadCandidates = _load_missing_candidates,
    persist_writes: PersistWrites = _persist_writes,
    sleeper: Sleeper = asyncio.sleep,
) -> int:
    """Best-effort tenant-scoped entrypoint for authenticated request triggers."""
    try:
        tenant_id = int(organization_id)
        if tenant_id <= 0:
            raise ValueError("organization_id must be positive")
        selected_provider = provider
        if selected_provider is None:
            selected_provider = configured_translation_provider()
            # Never make the automatic repair path silently fall back to a paid API.
            if not isinstance(selected_provider, OpenSourceTranslationProvider):
                return 0
        if selected_provider is None:
            return 0
        return await _run_translation_backfill_batch(
            organization_id=tenant_id,
            provider=selected_provider,
            load_candidates=load_candidates,
            persist_writes=persist_writes,
            sleeper=sleeper,
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        LOGGER.warning(
            "us_lacey_translation_backfill_failed organization_id=%s "
            "retry=eligible_on_next_trigger",
            organization_id,
            exc_info=True,
        )
        return 0
