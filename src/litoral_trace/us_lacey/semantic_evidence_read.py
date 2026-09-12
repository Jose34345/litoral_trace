"""Read-only Phase D projection for multilingual semantic evidence.

The immutable source span remains authoritative. English translations are optional
interpretations attached to that span and are selected only for presentation.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import and_, case, select
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    DocumentTextSpan,
    DocumentTextTranslation,
    SemanticEvidenceNode,
    SemanticSnapshotNode,
    UsLaceyOperation,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session


SessionFactory = Callable[[], Session]
_LANGUAGE_LABELS = {
    "es": "Spanish",
    "pt": "Portuguese",
    "zh": "Chinese",
    "en": "English",
}


@dataclass(frozen=True, slots=True)
class EvidenceTextView:
    """Customer-safe text projection preserving the original source evidence."""

    source_span_id: int
    target_field: str
    original_text: str
    original_language: str
    translated_text: str | None
    display_text: str
    is_translated: bool
    original_language_label: str
    source_assurance_document_id: int
    source_page: int
    source_locator: str | None


def evidence_text_view(
    *,
    span: DocumentTextSpan,
    target_field: str,
    translation: DocumentTextTranslation | None,
) -> EvidenceTextView:
    """Build the immutable-source/fallback contract independently of SQL."""
    original_text = str(span.original_text or "")
    original_language = str(span.original_language or "und").strip().lower() or "und"
    translated = ""
    if translation is not None:
        translated = str(translation.translated_text or "").strip()
    return EvidenceTextView(
        source_span_id=int(span.id),
        target_field=str(target_field or ""),
        original_text=original_text,
        original_language=original_language,
        translated_text=translated or None,
        display_text=translated or original_text,
        is_translated=bool(translated),
        original_language_label=_LANGUAGE_LABELS.get(
            original_language, original_language.upper() if original_language != "und" else "Unknown"
        ),
        source_assurance_document_id=int(span.assurance_document_id),
        source_page=int(span.page),
        source_locator=span.source_locator,
    )


class SemanticEvidenceReadService:
    """Read the CURRENT immutable snapshot plus its optional English translations."""

    def __init__(self, *, session_factory: SessionFactory | None = None) -> None:
        self._session_factory = session_factory or get_us_lacey_db_session

    def get_operation_evidence(
        self,
        *,
        organization_id: int,
        operation_public_id: UUID | str,
    ) -> dict[str, tuple[EvidenceTextView, ...]]:
        org_id = int(organization_id)
        session = self._session_factory()
        try:
            set_tenant_db_context(session, org_id)
            operation = session.scalar(
                select(UsLaceyOperation).where(
                    UsLaceyOperation.organization_id == org_id,
                    UsLaceyOperation.public_id == UUID(str(operation_public_id)),
                )
            )
            if operation is None or operation.current_evidence_snapshot_id is None:
                return {}

            provider_preference = case(
                (DocumentTextTranslation.provider == "OPEN_SOURCE_GOOGLE", 0),
                else_=1,
            )
            translation_presence = case(
                (DocumentTextTranslation.id.is_(None), 1),
                else_=0,
            )
            rows = session.execute(
                select(SemanticEvidenceNode, DocumentTextSpan, DocumentTextTranslation)
                .join(
                    SemanticSnapshotNode,
                    and_(
                        SemanticSnapshotNode.organization_id == SemanticEvidenceNode.organization_id,
                        SemanticSnapshotNode.evidence_node_id == SemanticEvidenceNode.id,
                    ),
                )
                .join(
                    DocumentTextSpan,
                    and_(
                        DocumentTextSpan.organization_id == SemanticEvidenceNode.organization_id,
                        DocumentTextSpan.id == SemanticEvidenceNode.source_span_id,
                    ),
                )
                .outerjoin(
                    DocumentTextTranslation,
                    and_(
                        DocumentTextTranslation.organization_id == DocumentTextSpan.organization_id,
                        DocumentTextTranslation.source_span_id == DocumentTextSpan.id,
                        DocumentTextTranslation.target_language == "en",
                    ),
                )
                .where(
                    SemanticSnapshotNode.organization_id == org_id,
                    SemanticSnapshotNode.snapshot_id == operation.current_evidence_snapshot_id,
                    SemanticEvidenceNode.organization_id == org_id,
                )
                .order_by(
                    SemanticEvidenceNode.target_field.asc(),
                    DocumentTextSpan.id.asc(),
                    translation_presence.asc(),
                    provider_preference.asc(),
                    DocumentTextTranslation.id.desc(),
                )
            ).all()

            by_field: dict[str, list[EvidenceTextView]] = {}
            seen: set[tuple[str, int]] = set()
            for node, span, translation in rows:
                key = (str(node.target_field), int(span.id))
                # The ORDER BY puts the preferred English interpretation first.
                # Keep exactly one presentation row per immutable source span.
                if key in seen:
                    continue
                seen.add(key)
                by_field.setdefault(str(node.target_field), []).append(
                    evidence_text_view(
                        span=span,
                        target_field=node.target_field,
                        translation=translation,
                    )
                )
            return {key: tuple(values) for key, values in by_field.items()}
        finally:
            session.close()
