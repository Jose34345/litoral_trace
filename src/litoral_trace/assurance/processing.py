"""Reproducible background processing for Assurance documents."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from litoral_trace.assurance.domain import (
    ConfidenceLevel,
    DocumentProcessingStatus,
    ExtractionRunStatus,
)
from litoral_trace.assurance.parsers import DocumentParseError, ParsedDocument, parse_document
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentExtractionRun,
    ExtractedDocumentField,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.vault import VaultService


SessionFactory = Callable[[], Session | None]
PARSER_ENGINE = "assurance-deterministic-parser"
PARSER_ENGINE_VERSION = "1.0.0"


class AssuranceProcessingError(RuntimeError):
    pass


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _confidence_level(confidence: float) -> str:
    if confidence >= 0.90:
        return ConfidenceLevel.HIGH.value
    if confidence >= 0.65:
        return ConfidenceLevel.MEDIUM.value
    return ConfidenceLevel.LOW.value


def _serialize_value(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _persist_parsed_fields(
    session: Session,
    *,
    organization_id: int,
    assurance_document: AssuranceDocument,
    extraction_run: DocumentExtractionRun,
    parsed: ParsedDocument,
) -> int:
    field_count = 0

    if parsed.text:
        session.add(
            ExtractedDocumentField(
                organization_id=organization_id,
                assurance_document_id=assurance_document.id,
                extraction_run_id=extraction_run.id,
                field_name="document_text",
                original_value=parsed.text,
                normalized_value=parsed.text,
                value_type="text",
                confidence=0.90,
                confidence_level=ConfidenceLevel.HIGH.value,
                source_page=None,
                source_locator="pdf:digital-text",
                auto_accepted=False,
                needs_review=True,
            )
        )
        field_count += 1

    for table_index, table in enumerate(parsed.tables, start=1):
        for row_index, record in enumerate(table.rows, start=1):
            for column_index, header in enumerate(table.headers, start=1):
                value = record.get(header)
                if value is None:
                    continue
                confidence = 0.98 if parsed.file_kind in {"XLSX", "XLS", "CSV"} else 0.90
                locator_parts = [table.source.locator or table.name]
                locator_parts.append(f"data_row:{row_index}")
                locator_parts.append(f"column:{column_index}")
                session.add(
                    ExtractedDocumentField(
                        organization_id=organization_id,
                        assurance_document_id=assurance_document.id,
                        extraction_run_id=extraction_run.id,
                        field_name=f"table.{table_index}.{header}",
                        original_value=_serialize_value(value),
                        normalized_value=_serialize_value(value),
                        value_type="cell",
                        confidence=confidence,
                        confidence_level=_confidence_level(confidence),
                        source_page=table.source.page,
                        source_locator=";".join(locator_parts),
                        auto_accepted=False,
                        needs_review=True,
                    )
                )
                field_count += 1

    return field_count


class AssuranceProcessingService:
    def __init__(
        self,
        *,
        session_factory: SessionFactory | None = None,
        vault_service: VaultService | None = None,
    ) -> None:
        self._session_factory = session_factory or get_db_session
        self._vault_service = vault_service or VaultService()

    def _new_session(self, organization_id: int) -> Session:
        session = self._session_factory()
        if session is None:
            raise AssuranceProcessingError("No se pudo abrir una sesion de procesamiento.")
        set_tenant_db_context(session, organization_id)
        return session

    def _load_document(
        self,
        session: Session,
        *,
        organization_id: int,
        assurance_public_id: UUID,
    ) -> tuple[AssuranceDocument, VaultDocument]:
        assurance_document = session.scalar(
            select(AssuranceDocument).where(
                AssuranceDocument.organization_id == organization_id,
                AssuranceDocument.public_id == assurance_public_id,
            )
        )
        if assurance_document is None:
            raise AssuranceProcessingError("Documento Assurance no encontrado.")
        vault_document = session.scalar(
            select(VaultDocument).where(
                VaultDocument.organization_id == organization_id,
                VaultDocument.id == assurance_document.vault_document_id,
                VaultDocument.status == "available",
            )
        )
        if vault_document is None:
            raise AssuranceProcessingError("Archivo original no disponible en Evidence Vault.")
        return assurance_document, vault_document

    def process(
        self,
        *,
        organization_id: int,
        assurance_public_id: UUID | str,
        force_reprocess: bool = False,
    ) -> str:
        public_id = (
            assurance_public_id
            if isinstance(assurance_public_id, UUID)
            else UUID(str(assurance_public_id))
        )
        session = self._new_session(int(organization_id))
        run: DocumentExtractionRun | None = None
        try:
            assurance_document, vault_document = self._load_document(
                session,
                organization_id=int(organization_id),
                assurance_public_id=public_id,
            )

            if (
                not force_reprocess
                and assurance_document.processing_status
                in {DocumentProcessingStatus.EXTRACTED.value, DocumentProcessingStatus.NEEDS_REVIEW.value}
            ):
                return assurance_document.processing_status

            run = DocumentExtractionRun(
                organization_id=int(organization_id),
                assurance_document_id=assurance_document.id,
                engine=PARSER_ENGINE,
                engine_version=PARSER_ENGINE_VERSION,
                status=ExtractionRunStatus.RUNNING.value,
                started_at=_utc_now(),
                extraction_metadata={"force_reprocess": bool(force_reprocess)},
            )
            session.add(run)
            assurance_document.processing_status = DocumentProcessingStatus.PROCESSING.value
            assurance_document.last_error_code = None
            assurance_document.last_error_message = None
            session.flush()
            session.commit()

            with self._vault_service.materialize_verified_download(
                organization_id=int(organization_id),
                document_id=vault_document.public_id,
            ) as verified:
                content = b"".join(verified.iter_chunks(chunk_size=1024 * 1024))

            parsed = parse_document(vault_document.original_filename, content)

            set_tenant_db_context(session, int(organization_id))
            assurance_document, _ = self._load_document(
                session,
                organization_id=int(organization_id),
                assurance_public_id=public_id,
            )
            run = session.scalar(
                select(DocumentExtractionRun).where(
                    DocumentExtractionRun.organization_id == int(organization_id),
                    DocumentExtractionRun.id == run.id,
                )
            )
            if run is None:
                raise AssuranceProcessingError("La corrida de extraccion desaparecio.")

            field_count = _persist_parsed_fields(
                session,
                organization_id=int(organization_id),
                assurance_document=assurance_document,
                extraction_run=run,
                parsed=parsed,
            )
            metadata = dict(parsed.metadata)
            metadata.update(
                {
                    "file_kind": parsed.file_kind,
                    "ocr_required": parsed.ocr_required,
                    "table_count": len(parsed.tables),
                    "field_count": field_count,
                }
            )
            run.extraction_metadata = metadata
            run.completed_at = _utc_now()

            if parsed.ocr_required:
                run.status = ExtractionRunStatus.NEEDS_REVIEW.value
                assurance_document.processing_status = DocumentProcessingStatus.NEEDS_REVIEW.value
                assurance_document.last_error_code = "OCR_REQUIRED"
                assurance_document.last_error_message = "PDF sin texto digital util; requiere OCR controlado."
            else:
                run.status = ExtractionRunStatus.SUCCEEDED.value
                assurance_document.processing_status = DocumentProcessingStatus.EXTRACTED.value
            session.commit()
            return assurance_document.processing_status

        except (DocumentParseError, ValueError) as exc:
            try:
                session.rollback()
                set_tenant_db_context(session, int(organization_id))
                assurance_document = session.scalar(
                    select(AssuranceDocument).where(
                        AssuranceDocument.organization_id == int(organization_id),
                        AssuranceDocument.public_id == public_id,
                    )
                )
                if assurance_document is not None:
                    assurance_document.processing_status = DocumentProcessingStatus.FAILED.value
                    assurance_document.last_error_code = "DOCUMENT_PARSE_FAILED"
                    assurance_document.last_error_message = str(exc)[:512]
                if run is not None:
                    persisted_run = session.scalar(
                        select(DocumentExtractionRun).where(
                            DocumentExtractionRun.organization_id == int(organization_id),
                            DocumentExtractionRun.id == run.id,
                        )
                    )
                    if persisted_run is not None:
                        persisted_run.status = ExtractionRunStatus.FAILED.value
                        persisted_run.error_code = "DOCUMENT_PARSE_FAILED"
                        persisted_run.error_detail = str(exc)[:2000]
                        persisted_run.completed_at = _utc_now()
                session.commit()
            finally:
                pass
            return DocumentProcessingStatus.FAILED.value
        finally:
            session.close()

    def progress(
        self,
        *,
        organization_id: int,
        assurance_public_id: UUID | str,
    ) -> dict[str, object]:
        public_id = (
            assurance_public_id
            if isinstance(assurance_public_id, UUID)
            else UUID(str(assurance_public_id))
        )
        session = self._new_session(int(organization_id))
        try:
            assurance_document, _ = self._load_document(
                session,
                organization_id=int(organization_id),
                assurance_public_id=public_id,
            )
            latest_run = session.scalar(
                select(DocumentExtractionRun)
                .where(
                    DocumentExtractionRun.organization_id == int(organization_id),
                    DocumentExtractionRun.assurance_document_id == assurance_document.id,
                )
                .order_by(DocumentExtractionRun.id.desc())
            )
            status = assurance_document.processing_status
            progress_percent = {
                DocumentProcessingStatus.UPLOADED.value: 20,
                DocumentProcessingStatus.PROCESSING.value: 60,
                DocumentProcessingStatus.EXTRACTED.value: 100,
                DocumentProcessingStatus.NEEDS_REVIEW.value: 100,
                DocumentProcessingStatus.FAILED.value: 100,
            }.get(status, 0)
            return {
                "assurance_document_id": str(assurance_document.public_id),
                "processing_status": status,
                "progress_percent": progress_percent,
                "last_error_code": assurance_document.last_error_code,
                "latest_run": None
                if latest_run is None
                else {
                    "id": latest_run.id,
                    "status": latest_run.status,
                    "engine": latest_run.engine,
                    "engine_version": latest_run.engine_version,
                    "metadata": latest_run.extraction_metadata,
                    "error_code": latest_run.error_code,
                },
            }
        finally:
            session.close()
