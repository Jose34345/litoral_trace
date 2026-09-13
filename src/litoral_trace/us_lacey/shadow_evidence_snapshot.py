"""Shadow multilingual evidence snapshots for the U.S. Lacey pilot.

This module is deliberately write-only with respect to the new Phase A schema.
The legacy Assurance/U.S. Lacey projection remains authoritative. A failure here
must never mutate legacy job state or become a prerequisite for the UI.
"""
from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
import logging
import os
from typing import Callable, Mapping

from lingua import Language, LanguageDetectorBuilder
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentExtractionRun,
    DocumentTextSpan,
    DocumentTextTranslation,
    ExtractedDocumentField,
    SemanticEvidenceNode,
    SemanticSnapshotNode,
    UsLaceyEvidenceSnapshot,
    UsLaceyEvidenceSnapshotDocument,
    UsLaceyOperation,
    UsLaceyOperationDocument,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.translation import (
    AwsTranslateProvider,
    DeepLTranslationProvider,
    NoOpEnglishProvider,
    OpenSourceTranslationProvider,
    TranslationProvider,
)
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.operation_lock import us_lacey_operation_projection_lock


LOGGER = logging.getLogger(__name__)

SHADOW_FLAG = "LT_LACEY_MULTILINGUAL_SHADOW"
TRANSLATION_PROVIDER_ENV = "LT_LACEY_TRANSLATION_PROVIDER"
TRANSLATION_AWS_REGION_ENV = "LT_LACEY_TRANSLATION_AWS_REGION"
GRAPH_VERSION = "semantic-evidence-shadow-v1"
ONTOLOGY_VERSION = "legacy-adapter-v1"
TRANSLATION_PIPELINE_VERSION = "multilingual-shadow-v1"

SessionFactory = Callable[[], Session]
LockFactory = Callable[..., AbstractContextManager[None]]


class ShadowEvidenceSnapshotError(RuntimeError):
    """A shadow-only failure. Callers must isolate it from the legacy job state."""


@dataclass(frozen=True, slots=True)
class LanguageDetection:
    code: str
    confidence: float


@dataclass(slots=True)
class SnapshotMetrics:
    generation: int = 0
    documents_included: int = 0
    spans_created: int = 0
    spans_without_page: int = 0
    language_en: int = 0
    language_es: int = 0
    language_pt: int = 0
    language_zh: int = 0
    language_und: int = 0
    translations_created: int = 0
    translations_omitted: int = 0
    semantic_nodes_created: int = 0

    def count_language(self, code: str) -> None:
        attr = f"language_{code if code in {'en', 'es', 'pt', 'zh'} else 'und'}"
        setattr(self, attr, int(getattr(self, attr)) + 1)

    def as_dict(self) -> dict[str, int]:
        return {key: int(value) for key, value in asdict(self).items()}


@dataclass(frozen=True, slots=True)
class SnapshotBuildResult:
    created: bool
    snapshot_id: int | None
    source_set_fingerprint: str | None
    reason: str
    metrics: SnapshotMetrics


@dataclass(frozen=True, slots=True)
class _DocumentSource:
    operation_document_id: int
    assurance_document_id: int
    extraction_run_id: int
    extraction_evidence_hash: str
    fields: tuple[ExtractedDocumentField, ...]
    source_sha256: str
    document_role: str
    document_type: str
    version_number: int


_LANGUAGE_TO_CODE: Mapping[Language, str] = {
    Language.ENGLISH: "en",
    Language.SPANISH: "es",
    Language.PORTUGUESE: "pt",
    Language.CHINESE: "zh",
}


@lru_cache(maxsize=1)
def _language_detector():
    return LanguageDetectorBuilder.from_languages(
        Language.ENGLISH,
        Language.SPANISH,
        Language.PORTUGUESE,
        Language.CHINESE,
    ).build()


def multilingual_shadow_enabled(environ: Mapping[str, str] | None = None) -> bool:
    env = os.environ if environ is None else environ
    return str(env.get(SHADOW_FLAG, "0")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
        "enabled",
    }


def _is_substantive_text(text: str) -> bool:
    value = str(text or "").strip()
    if len(value) < 6:
        return False
    if all(ch.isdigit() or ch.isspace() or ch in ".,;:/%$€¥+-_#()[]" for ch in value):
        return False
    cjk_count = sum("\u4e00" <= ch <= "\u9fff" for ch in value)
    if cjk_count >= 4:
        return True
    words = []
    current: list[str] = []
    for char in value:
        if char.isalpha():
            current.append(char)
        elif current:
            words.append("".join(current))
            current = []
    if current:
        words.append("".join(current))
    alpha_count = sum(len(word) for word in words)
    return len(words) >= 2 and alpha_count >= 6


def detect_supported_language(text: str) -> LanguageDetection:
    """Detect only EN/ES/PT/ZH; codes, numbers and short fragments stay ``und``."""
    if not _is_substantive_text(text):
        return LanguageDetection(code="und", confidence=0.0)
    detector = _language_detector()
    detected = detector.detect_language_of(text)
    code = _LANGUAGE_TO_CODE.get(detected, "und")
    if code == "und" or detected is None:
        return LanguageDetection(code="und", confidence=0.0)
    confidence = 0.0
    try:
        for item in detector.compute_language_confidence_values(text):
            if item.language == detected:
                confidence = float(item.value)
                break
    except Exception:
        confidence = 0.0
    return LanguageDetection(code=code, confidence=max(0.0, min(1.0, confidence)))


def configured_translation_provider() -> TranslationProvider | None:
    provider = str(os.environ.get(TRANSLATION_PROVIDER_ENV, "")).strip().upper()
    if provider in {"", "NONE", "DISABLED", "OFF"}:
        return None
    if provider in {"OPEN_SOURCE", "FREE", "OPEN_SOURCE_GOOGLE"}:
        return OpenSourceTranslationProvider()
    if provider in {"DEEPL", "DEEPL_API"}:
        return DeepLTranslationProvider()
    if provider in {"AWS", "AWS_TRANSLATE"}:
        region = str(os.environ.get(TRANSLATION_AWS_REGION_ENV, "")).strip() or None
        return AwsTranslateProvider(region_name=region)
    raise ShadowEvidenceSnapshotError(
        f"Unsupported {TRANSLATION_PROVIDER_ENV} value: {provider}"
    )


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _extraction_evidence_hash(fields: tuple[ExtractedDocumentField, ...]) -> str:
    """Fingerprint mutable semantic values from one legacy extraction run.

    Human Assurance review preserves original extraction provenance but may correct
    ``normalized_value`` in place on a ``NEEDS_REVIEW`` run. Capturing the semantic
    values here makes that review revision visible to the immutable snapshot layer
    without changing any legacy review/write semantics.
    """
    payload = [
        {
            "field_id": int(field.id),
            "field_name": str(field.field_name or ""),
            "normalized_value": field.normalized_value,
            "confidence": float(field.confidence or 0.0),
        }
        for field in fields
    ]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return _sha256_text(canonical)


def _source_set_fingerprint(sources: list[_DocumentSource]) -> str:
    payload = [
        {
            "operation_document_id": item.operation_document_id,
            "assurance_document_id": item.assurance_document_id,
            "extraction_run_id": item.extraction_run_id,
            "extraction_evidence_hash": item.extraction_evidence_hash,
            "source_sha256": item.source_sha256,
            "version_number": item.version_number,
        }
        for item in sorted(sources, key=lambda source: source.operation_document_id)
    ]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return _sha256_text(canonical)


def _bounded(value: str, limit: int) -> str:
    text = str(value or "")
    if len(text) <= limit:
        return text
    digest = _sha256_text(text)[:12]
    keep = max(1, limit - len(digest) - 1)
    return f"{text[:keep]}:{digest}"


def _target_field(field_name: str) -> str:
    return _bounded(field_name or "unknown", 100)


def _semantic_role(field_name: str) -> str:
    normalized = "".join(
        char if char.isalnum() else "_" for char in str(field_name or "unknown").upper()
    )
    normalized = "_".join(part for part in normalized.split("_") if part)
    return _bounded(normalized or "UNKNOWN", 100)


def _semantic_scope(field_name: str) -> str:
    name = str(field_name or "").lower()
    if any(token in name for token in ("genus", "species", "harvest", "plant", "quantity", "percent_recycled")):
        return "PLANT_COMPONENT"
    if name.startswith("raw."):
        return "DOCUMENT"
    return "SHIPMENT"


def _local_entity_key(*, assurance_document_id: int, field: ExtractedDocumentField) -> str:
    locator = str(field.source_locator or f"field:{field.id}")
    return _bounded(f"doc:{assurance_document_id}:{locator}", 512)


def _node_fingerprint(
    *,
    assurance_document_id: int,
    extraction_run_id: int,
    field: ExtractedDocumentField,
    span: DocumentTextSpan,
) -> str:
    payload = "|".join(
        (
            str(assurance_document_id),
            str(extraction_run_id),
            str(field.id),
            str(field.field_name),
            str(span.content_hash),
            str(field.normalized_value or ""),
            str(float(field.confidence or 0.0)),
        )
    )
    return _sha256_text(payload)


def _translate_span_if_eligible(
    *,
    span: DocumentTextSpan,
    provider: TranslationProvider | None,
    metrics: SnapshotMetrics,
    session: Session,
) -> None:
    language = str(span.original_language or "und")
    if language in {"en", "und"}:
        return
    if provider is None or isinstance(provider, NoOpEnglishProvider):
        metrics.translations_omitted += 1
        return
    try:
        result = provider.translate(span.original_text, language, "en")
        translated_text = str(result.translated_text or "").strip()
        if not translated_text:
            metrics.translations_omitted += 1
            return
        input_hash = _sha256_text(span.original_text)
        existing = session.scalar(
            select(DocumentTextTranslation).where(
                DocumentTextTranslation.organization_id == span.organization_id,
                DocumentTextTranslation.source_span_id == span.id,
                DocumentTextTranslation.target_language == "en",
                DocumentTextTranslation.provider == result.provider,
                DocumentTextTranslation.model_name == result.model_name,
                DocumentTextTranslation.model_version == result.model_version,
                DocumentTextTranslation.input_hash == input_hash,
            )
        )
        if existing is not None:
            return
        quality = result.metadata.get("quality_score") if result.metadata else None
        quality_score = float(quality) if isinstance(quality, (int, float)) else None
        session.add(
            DocumentTextTranslation(
                organization_id=span.organization_id,
                source_span_id=span.id,
                source_language=language,
                target_language="en",
                translated_text=translated_text,
                provider=result.provider,
                model_name=result.model_name,
                model_version=result.model_version,
                quality_score=quality_score,
                input_hash=input_hash,
                output_hash=_sha256_text(translated_text),
            )
        )
        metrics.translations_created += 1
    except Exception:
        metrics.translations_omitted += 1
        LOGGER.warning(
            "Lacey multilingual shadow translation failed; original evidence retained",
            exc_info=True,
            extra={
                "organization_id": span.organization_id,
                "assurance_document_id": span.assurance_document_id,
                "source_span_id": span.id,
                "source_language": language,
            },
        )


def _load_document_sources(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
) -> tuple[list[_DocumentSource], str | None]:
    """Return the complete current source set or refuse to build a partial snapshot."""
    rows = session.execute(
        select(UsLaceyOperationDocument, AssuranceDocument, VaultDocument)
        .join(
            AssuranceDocument,
            and_(
                AssuranceDocument.id == UsLaceyOperationDocument.assurance_document_id,
                AssuranceDocument.organization_id == UsLaceyOperationDocument.organization_id,
            ),
        )
        .join(
            VaultDocument,
            and_(
                VaultDocument.id == AssuranceDocument.vault_document_id,
                VaultDocument.organization_id == AssuranceDocument.organization_id,
            ),
        )
        .where(
            UsLaceyOperationDocument.organization_id == organization_id,
            UsLaceyOperationDocument.operation_id == operation_id,
            UsLaceyOperationDocument.is_current.is_(True),
        )
        .order_by(UsLaceyOperationDocument.id.asc())
    ).all()
    if not rows:
        return [], "NO_CURRENT_DOCUMENTS"

    sources: list[_DocumentSource] = []
    for operation_document, assurance_document, vault_document in rows:
        if vault_document.status != "available":
            return [], "SOURCE_SET_NOT_FULLY_AVAILABLE"
        extraction_run = session.scalar(
            select(DocumentExtractionRun)
            .where(
                DocumentExtractionRun.organization_id == organization_id,
                DocumentExtractionRun.assurance_document_id == assurance_document.id,
            )
            .order_by(DocumentExtractionRun.id.desc())
            .limit(1)
            .with_for_update()
        )
        if extraction_run is None:
            return [], "SOURCE_SET_NOT_FULLY_EXTRACTED"

        extraction_status = str(extraction_run.status or "").strip().upper()
        if extraction_status == "RUNNING":
            return [], "SOURCE_SET_EXTRACTION_IN_PROGRESS"
        if extraction_status not in {"SUCCEEDED", "NEEDS_REVIEW"}:
            return [], "SOURCE_SET_EXTRACTION_UNUSABLE"

        fields = tuple(
            session.scalars(
                select(ExtractedDocumentField)
                .where(
                    ExtractedDocumentField.organization_id == organization_id,
                    ExtractedDocumentField.assurance_document_id == assurance_document.id,
                    ExtractedDocumentField.extraction_run_id == extraction_run.id,
                )
                .order_by(ExtractedDocumentField.id.asc())
                .with_for_update()
            ).all()
        )
        sources.append(
            _DocumentSource(
                operation_document_id=operation_document.id,
                assurance_document_id=assurance_document.id,
                extraction_run_id=extraction_run.id,
                extraction_evidence_hash=_extraction_evidence_hash(fields),
                fields=fields,
                source_sha256=vault_document.sha256,
                document_role=operation_document.document_role,
                document_type=assurance_document.semantic_document_type,
                version_number=operation_document.version_number,
            )
        )
    return sources, None


def _log_metrics(
    *,
    organization_id: int,
    operation_id: int,
    fingerprint: str | None,
    reason: str,
    metrics: SnapshotMetrics,
) -> None:
    data = metrics.as_dict()
    LOGGER.info(
        "Lacey multilingual shadow metrics generation=%s documents_included=%s "
        "spans_created=%s spans_without_page=%s en=%s es=%s pt=%s zh=%s und=%s "
        "translations_created=%s translations_omitted=%s semantic_nodes_created=%s reason=%s",
        data["generation"],
        data["documents_included"],
        data["spans_created"],
        data["spans_without_page"],
        data["language_en"],
        data["language_es"],
        data["language_pt"],
        data["language_zh"],
        data["language_und"],
        data["translations_created"],
        data["translations_omitted"],
        data["semantic_nodes_created"],
        reason,
        extra={
            "organization_id": organization_id,
            "operation_id": operation_id,
            "source_set_fingerprint": fingerprint,
            "shadow_metrics": data,
            "shadow_reason": reason,
        },
    )


def build_shadow_evidence_snapshot(
    *,
    organization_id: int,
    operation_id: int,
    translation_provider: TranslationProvider | None = None,
    use_configured_translation_provider: bool = True,
    session_factory: SessionFactory = get_us_lacey_db_session,
    lock_factory: LockFactory = us_lacey_operation_projection_lock,
) -> SnapshotBuildResult:
    """Build one operation-wide shadow snapshot without affecting legacy state.

    The operation advisory lock serializes same-operation builds. The snapshot,
    memberships, spans, translations, semantic nodes, CURRENT/SUPERSEDED transition
    and operation pointer are committed in one database transaction.
    """
    org_id = int(organization_id)
    op_id = int(operation_id)
    metrics = SnapshotMetrics()
    fingerprint: str | None = None
    if not multilingual_shadow_enabled():
        result = SnapshotBuildResult(False, None, None, "DISABLED", metrics)
        _log_metrics(
            organization_id=org_id,
            operation_id=op_id,
            fingerprint=None,
            reason=result.reason,
            metrics=metrics,
        )
        return result

    provider = translation_provider
    if provider is None and use_configured_translation_provider:
        provider = configured_translation_provider()

    with lock_factory(organization_id=org_id, operation_id=op_id):
        session = session_factory()
        try:
            set_tenant_db_context(session, org_id)
            operation = session.scalar(
                select(UsLaceyOperation)
                .where(
                    UsLaceyOperation.organization_id == org_id,
                    UsLaceyOperation.id == op_id,
                )
                .with_for_update()
            )
            if operation is None:
                raise ShadowEvidenceSnapshotError("U.S. Lacey operation not found in tenant scope")

            sources, blocked_reason = _load_document_sources(
                session,
                organization_id=org_id,
                operation_id=op_id,
            )
            metrics.documents_included = len(sources)
            if blocked_reason is not None:
                session.rollback()
                result = SnapshotBuildResult(False, None, None, blocked_reason, metrics)
                _log_metrics(
                    organization_id=org_id,
                    operation_id=op_id,
                    fingerprint=None,
                    reason=result.reason,
                    metrics=metrics,
                )
                return result

            fingerprint = _source_set_fingerprint(sources)
            existing = session.scalar(
                select(UsLaceyEvidenceSnapshot).where(
                    UsLaceyEvidenceSnapshot.organization_id == org_id,
                    UsLaceyEvidenceSnapshot.operation_id == op_id,
                    UsLaceyEvidenceSnapshot.source_set_fingerprint == fingerprint,
                )
            )
            if existing is not None and existing.status == "CURRENT":
                metrics.generation = existing.generation
                session.rollback()
                result = SnapshotBuildResult(
                    False,
                    existing.id,
                    fingerprint,
                    "IDEMPOTENT_CURRENT",
                    metrics,
                )
                _log_metrics(
                    organization_id=org_id,
                    operation_id=op_id,
                    fingerprint=fingerprint,
                    reason=result.reason,
                    metrics=metrics,
                )
                return result
            if existing is not None:
                raise ShadowEvidenceSnapshotError(
                    f"Non-current snapshot already owns source fingerprint ({existing.status})"
                )

            generation = int(
                session.scalar(
                    select(func.coalesce(func.max(UsLaceyEvidenceSnapshot.generation), 0)).where(
                        UsLaceyEvidenceSnapshot.organization_id == org_id,
                        UsLaceyEvidenceSnapshot.operation_id == op_id,
                    )
                )
                or 0
            ) + 1
            metrics.generation = generation

            snapshot = UsLaceyEvidenceSnapshot(
                organization_id=org_id,
                operation_id=op_id,
                generation=generation,
                status="BUILDING",
                source_set_fingerprint=fingerprint,
                graph_version=GRAPH_VERSION,
                ontology_version=ONTOLOGY_VERSION,
                translation_pipeline_version=TRANSLATION_PIPELINE_VERSION,
                document_count=len(sources),
                node_count=0,
                conflict_count=0,
            )
            session.add(snapshot)
            session.flush()

            snapshot_node_count = 0
            for source in sources:
                session.add(
                    UsLaceyEvidenceSnapshotDocument(
                        organization_id=org_id,
                        snapshot_id=snapshot.id,
                        operation_document_id=source.operation_document_id,
                        assurance_document_id=source.assurance_document_id,
                        extraction_run_id=source.extraction_run_id,
                        source_sha256=source.source_sha256,
                        document_role=source.document_role,
                        processing_result="SUCCEEDED",
                    )
                )
                for field in source.fields:
                    original_text = str(field.original_value or "").strip()
                    if field.source_page is None or int(field.source_page) <= 0:
                        metrics.spans_without_page += 1
                        continue
                    if not original_text:
                        continue

                    block_id = f"legacy-field:{field.id}"
                    span = session.scalar(
                        select(DocumentTextSpan).where(
                            DocumentTextSpan.organization_id == org_id,
                            DocumentTextSpan.assurance_document_id == source.assurance_document_id,
                            DocumentTextSpan.extraction_run_id == source.extraction_run_id,
                            DocumentTextSpan.block_id == block_id,
                        )
                    )
                    if span is None:
                        detection = detect_supported_language(original_text)
                        metrics.count_language(detection.code)
                        span = DocumentTextSpan(
                            organization_id=org_id,
                            assurance_document_id=source.assurance_document_id,
                            extraction_run_id=source.extraction_run_id,
                            page=int(field.source_page),
                            block_id=block_id,
                            source_locator=field.source_locator,
                            original_text=original_text,
                            original_language=detection.code,
                            language_confidence=detection.confidence,
                            extraction_method="LEGACY_ADAPTER",
                            content_hash=_sha256_text(original_text),
                        )
                        session.add(span)
                        session.flush()
                        metrics.spans_created += 1
                    else:
                        metrics.count_language(span.original_language)

                    _translate_span_if_eligible(
                        span=span,
                        provider=provider,
                        metrics=metrics,
                        session=session,
                    )

                    fingerprint_node = _node_fingerprint(
                        assurance_document_id=source.assurance_document_id,
                        extraction_run_id=source.extraction_run_id,
                        field=field,
                        span=span,
                    )
                    node = session.scalar(
                        select(SemanticEvidenceNode).where(
                            SemanticEvidenceNode.organization_id == org_id,
                            SemanticEvidenceNode.fingerprint == fingerprint_node,
                        )
                    )
                    if node is None:
                        confidence = max(0.0, min(1.0, float(field.confidence or 0.0)))
                        score = confidence * 100.0
                        node = SemanticEvidenceNode(
                            organization_id=org_id,
                            assurance_document_id=source.assurance_document_id,
                            extraction_run_id=source.extraction_run_id,
                            source_span_id=span.id,
                            target_field=_target_field(field.field_name),
                            semantic_role=_semantic_role(field.field_name),
                            scope=_semantic_scope(field.field_name),
                            local_entity_key=_local_entity_key(
                                assurance_document_id=source.assurance_document_id,
                                field=field,
                            ),
                            original_value=original_text,
                            normalized_value=field.normalized_value,
                            evidence_class="EXPLICIT",
                            document_type=source.document_type or "UNKNOWN",
                            extraction_confidence=confidence,
                            authority_score=score,
                            candidate_score=score,
                            fingerprint=fingerprint_node,
                        )
                        session.add(node)
                        session.flush()
                        metrics.semantic_nodes_created += 1

                    session.add(
                        SemanticSnapshotNode(
                            organization_id=org_id,
                            snapshot_id=snapshot.id,
                            evidence_node_id=node.id,
                            canonical_entity_id=None,
                        )
                    )
                    snapshot_node_count += 1

            snapshot.node_count = snapshot_node_count
            snapshot.conflict_count = 0
            current_snapshots = session.scalars(
                select(UsLaceyEvidenceSnapshot).where(
                    UsLaceyEvidenceSnapshot.organization_id == org_id,
                    UsLaceyEvidenceSnapshot.operation_id == op_id,
                    UsLaceyEvidenceSnapshot.status == "CURRENT",
                    UsLaceyEvidenceSnapshot.id != snapshot.id,
                )
            ).all()
            for previous in current_snapshots:
                previous.status = "SUPERSEDED"
            snapshot.status = "CURRENT"
            snapshot.finalized_at = datetime.now(timezone.utc)
            operation.current_evidence_snapshot_id = snapshot.id
            session.commit()

            reason = "CREATED_EMPTY_EVIDENCE" if metrics.spans_created == 0 else "CREATED"
            result = SnapshotBuildResult(True, snapshot.id, fingerprint, reason, metrics)
            _log_metrics(
                organization_id=org_id,
                operation_id=op_id,
                fingerprint=fingerprint,
                reason=result.reason,
                metrics=metrics,
            )
            return result
        except Exception:
            session.rollback()
            _log_metrics(
                organization_id=org_id,
                operation_id=op_id,
                fingerprint=fingerprint,
                reason="FAILED",
                metrics=metrics,
            )
            raise
        finally:
            session.close()