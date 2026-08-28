"""Reproducible background processing for Assurance documents."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, Sequence
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from litoral_trace.assurance.domain import (
    AssuranceDocumentType,
    ConfidenceLevel,
    DocumentProcessingStatus,
    ExtractionRunStatus,
)
from litoral_trace.assurance.extraction import (
    ExtractedCandidate,
    classify_document,
    extract_structured_fields,
    missing_required_fields,
)
from litoral_trace.assurance.matching import (
    HIGH_CONFIDENCE,
    EntityRecord,
    FieldDecisionStatus,
    decide_field_acceptance,
    match_candidate_entities,
)
from litoral_trace.assurance.parsers import DocumentParseError, ParsedDocument, parse_document
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentEntityLink,
    DocumentExtractionRun,
    ExtractedDocumentField,
    Lote,
    Shipment,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import (
    AuditAction,
    AuditActor,
    AuditOutcome,
    record_audit_event,
)
from litoral_trace.services.vault import VaultService


SessionFactory = Callable[[], Session | None]
PARSER_ENGINE = "assurance-deterministic-parser"
PARSER_ENGINE_VERSION = "1.2.0"


class AssuranceProcessingError(RuntimeError):
    pass


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _system_actor(organization_id: int) -> AuditActor:
    return AuditActor(
        organization_id=int(organization_id),
        user_id=None,
        username="system",
        role="system",
    )


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


def _persist_raw_parsed_fields(
    session: Session,
    *,
    organization_id: int,
    assurance_document: AssuranceDocument,
    extraction_run: DocumentExtractionRun,
    parsed: ParsedDocument,
) -> int:
    """Keep auditable raw parse output in addition to semantic candidates."""
    field_count = 0

    if parsed.text:
        session.add(
            ExtractedDocumentField(
                organization_id=organization_id,
                assurance_document_id=assurance_document.id,
                extraction_run_id=extraction_run.id,
                field_name="raw.document_text",
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
                        field_name=f"raw.table.{table_index}.{header}",
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


def _persist_structured_fields(
    session: Session,
    *,
    organization_id: int,
    assurance_document: AssuranceDocument,
    extraction_run: DocumentExtractionRun,
    candidates: Sequence[ExtractedCandidate],
) -> dict[str, int]:
    decisions = decide_field_acceptance(candidates)
    counts = {
        "structured": 0,
        "auto_accepted": 0,
        "needs_review": 0,
        "conflicts": 0,
        "low_confidence": 0,
    }
    for decision in decisions:
        candidate = decision.candidate
        auto_accepted = decision.status == FieldDecisionStatus.AUTO_ACCEPTED
        needs_review = not auto_accepted
        session.add(
            ExtractedDocumentField(
                organization_id=organization_id,
                assurance_document_id=assurance_document.id,
                extraction_run_id=extraction_run.id,
                field_name=candidate.field_name,
                original_value=candidate.original_value,
                normalized_value=candidate.normalized_value,
                value_type=candidate.value_type,
                confidence=candidate.confidence,
                confidence_level=_confidence_level(candidate.confidence),
                source_page=candidate.source_page,
                source_locator=candidate.source_locator,
                auto_accepted=auto_accepted,
                needs_review=needs_review,
            )
        )
        counts["structured"] += 1
        if auto_accepted:
            counts["auto_accepted"] += 1
        else:
            counts["needs_review"] += 1
        if decision.status == FieldDecisionStatus.CONFLICT:
            counts["conflicts"] += 1
        if decision.status == FieldDecisionStatus.LOW_CONFIDENCE:
            counts["low_confidence"] += 1
    return counts


def _entity_records(
    session: Session,
    *,
    organization_id: int,
) -> tuple[EntityRecord, ...]:
    """Build exact identifiers from already-known tenant data only."""
    records: dict[tuple[str, str], EntityRecord] = {}

    lotes = session.scalars(
        select(Lote).where(Lote.organization_id == organization_id)
    ).all()
    for lote in lotes:
        lot_ref = f"lote:{lote.id}"
        records[("LOT", lot_ref)] = EntityRecord(
            entity_type="LOT",
            entity_reference=lot_ref,
            identifiers=(lote.identificador,),
            display_name=lote.identificador,
        )
        producer_id = str(lote.productor_id or "").strip()
        if producer_id:
            supplier_ref = f"producer:{producer_id}"
            records[("SUPPLIER", supplier_ref)] = EntityRecord(
                entity_type="SUPPLIER",
                entity_reference=supplier_ref,
                identifiers=(producer_id,),
                display_name=producer_id,
            )

    shipments = session.scalars(
        select(Shipment).where(Shipment.organization_id == organization_id)
    ).all()
    for shipment in shipments:
        shipment_ref = f"shipment:{shipment.public_id}"
        identifiers = tuple(
            value
            for value in (
                shipment.shipment_code,
                str(shipment.public_id),
            )
            if str(value or "").strip()
        )
        records[("SHIPMENT", shipment_ref)] = EntityRecord(
            entity_type="SHIPMENT",
            entity_reference=shipment_ref,
            identifiers=identifiers,
            display_name=shipment.shipment_code,
        )
        if shipment.sale_reference and shipment.sale_reference.strip():
            order_ref = f"order:{shipment.public_id}"
            records[("ORDER", order_ref)] = EntityRecord(
                entity_type="ORDER",
                entity_reference=order_ref,
                identifiers=(shipment.sale_reference.strip(),),
                display_name=shipment.sale_reference.strip(),
            )

    return tuple(records.values())


def _persist_entity_links(
    session: Session,
    *,
    organization_id: int,
    assurance_document: AssuranceDocument,
    candidates: Sequence[ExtractedCandidate],
) -> dict[str, int]:
    matches = match_candidate_entities(
        candidates,
        _entity_records(session, organization_id=organization_id),
    )
    counts = {"linked": 0, "ambiguous": 0, "below_threshold": 0}
    for match in matches:
        if match.ambiguous:
            counts["ambiguous"] += 1
            continue
        if match.confidence < HIGH_CONFIDENCE:
            counts["below_threshold"] += 1
            continue
        existing = session.scalar(
            select(DocumentEntityLink).where(
                DocumentEntityLink.organization_id == organization_id,
                DocumentEntityLink.assurance_document_id == assurance_document.id,
                DocumentEntityLink.entity_type == match.entity_type,
                DocumentEntityLink.entity_reference == match.entity_reference,
            )
        )
        if existing is None:
            session.add(
                DocumentEntityLink(
                    organization_id=organization_id,
                    assurance_document_id=assurance_document.id,
                    entity_type=match.entity_type,
                    entity_reference=match.entity_reference,
                    link_confidence=match.confidence,
                    link_method=match.method,
                    human_confirmed=False,
                )
            )
            counts["linked"] += 1
    return counts


def _mark_processing_failure(
    session: Session,
    *,
    organization_id: int,
    public_id: UUID,
    run_id: int | None,
    error_code: str,
    detail: str,
) -> None:
    """Persist failure state and its safe audit record in one transaction."""
    try:
        session.rollback()
        set_tenant_db_context(session, organization_id)
        assurance_document = session.scalar(
            select(AssuranceDocument).where(
                AssuranceDocument.organization_id == organization_id,
                AssuranceDocument.public_id == public_id,
            )
        )
        persisted_run: DocumentExtractionRun | None = None
        if assurance_document is not None:
            assurance_document.processing_status = DocumentProcessingStatus.FAILED.value
            assurance_document.last_error_code = error_code
            assurance_document.last_error_message = detail[:512]
        if run_id is not None:
            persisted_run = session.scalar(
                select(DocumentExtractionRun).where(
                    DocumentExtractionRun.organization_id == organization_id,
                    DocumentExtractionRun.id == run_id,
                )
            )
            if persisted_run is not None:
                persisted_run.status = ExtractionRunStatus.FAILED.value
                persisted_run.error_code = error_code
                persisted_run.error_detail = detail[:2000]
                persisted_run.completed_at = _utc_now()
        if assurance_document is not None:
            record_audit_event(
                session,
                actor=_system_actor(organization_id),
                action=AuditAction.ASSURANCE_EXTRACTION_FAILED,
                entity_type="assurance_document",
                entity_id=assurance_document.id,
                outcome=AuditOutcome.FAILURE,
                metadata={
                    "extraction_run_id": run_id,
                    "engine": None if persisted_run is None else persisted_run.engine,
                    "engine_version": None if persisted_run is None else persisted_run.engine_version,
                    "error_code": error_code,
                },
                before_data={"processing_status": DocumentProcessingStatus.PROCESSING.value},
                after_data={
                    "processing_status": DocumentProcessingStatus.FAILED.value,
                    "error_code": error_code,
                },
                detail="Procesamiento automatico Assurance finalizado con error.",
            )
        session.commit()
    except Exception:
        try:
            session.rollback()
        except Exception:
            pass


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
        org_id = int(organization_id)
        session = self._new_session(org_id)
        run: DocumentExtractionRun | None = None
        run_id: int | None = None
        try:
            assurance_document, vault_document = self._load_document(
                session,
                organization_id=org_id,
                assurance_public_id=public_id,
            )

            if (
                not force_reprocess
                and assurance_document.processing_status
                in {DocumentProcessingStatus.EXTRACTED.value, DocumentProcessingStatus.NEEDS_REVIEW.value}
            ):
                return assurance_document.processing_status

            run = DocumentExtractionRun(
                organization_id=org_id,
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
            run_id = run.id
            session.commit()

            with self._vault_service.materialize_verified_download(
                organization_id=org_id,
                document_id=vault_document.public_id,
            ) as verified:
                content = b"".join(verified.iter_chunks(chunk_size=1024 * 1024))

            parsed = parse_document(vault_document.original_filename, content)
            classification = classify_document(vault_document.original_filename, parsed)
            structured_candidates = extract_structured_fields(parsed)
            missing_fields = missing_required_fields(
                classification.document_type,
                structured_candidates,
            )

            set_tenant_db_context(session, org_id)
            assurance_document, _ = self._load_document(
                session,
                organization_id=org_id,
                assurance_public_id=public_id,
            )
            run = session.scalar(
                select(DocumentExtractionRun).where(
                    DocumentExtractionRun.organization_id == org_id,
                    DocumentExtractionRun.id == run_id,
                )
            )
            if run is None:
                raise AssuranceProcessingError("La corrida de extraccion desaparecio.")

            assurance_document.semantic_document_type = classification.document_type.value
            assurance_document.type_confidence = classification.confidence

            raw_field_count = _persist_raw_parsed_fields(
                session,
                organization_id=org_id,
                assurance_document=assurance_document,
                extraction_run=run,
                parsed=parsed,
            )
            field_counts = _persist_structured_fields(
                session,
                organization_id=org_id,
                assurance_document=assurance_document,
                extraction_run=run,
                candidates=structured_candidates,
            )
            link_counts = _persist_entity_links(
                session,
                organization_id=org_id,
                assurance_document=assurance_document,
                candidates=structured_candidates,
            )
            metadata = dict(run.extraction_metadata or {})
            metadata.update(parsed.metadata)
            metadata.update(
                {
                    "file_kind": parsed.file_kind,
                    "ocr_required": parsed.ocr_required,
                    "table_count": len(parsed.tables),
                    "raw_field_count": raw_field_count,
                    "structured_field_count": field_counts["structured"],
                    "auto_accepted_field_count": field_counts["auto_accepted"],
                    "review_field_count": field_counts["needs_review"],
                    "field_conflict_count": field_counts["conflicts"],
                    "low_confidence_field_count": field_counts["low_confidence"],
                    "entity_link_count": link_counts["linked"],
                    "ambiguous_entity_match_count": link_counts["ambiguous"],
                    "below_threshold_entity_match_count": link_counts["below_threshold"],
                    "document_type": classification.document_type.value,
                    "type_confidence": classification.confidence,
                    "classification_evidence": list(classification.evidence),
                    "missing_required_fields": list(missing_fields),
                }
            )
            run.extraction_metadata = metadata
            run.completed_at = _utc_now()

            review_code: str | None = None
            review_message: str | None = None
            if parsed.ocr_required:
                review_code = "OCR_REQUIRED"
                review_message = "PDF sin texto digital util; requiere OCR controlado."
            elif classification.document_type == AssuranceDocumentType.UNKNOWN:
                review_code = "UNCLASSIFIED_DOCUMENT"
                review_message = "No se pudo clasificar el documento con evidencia suficiente."
            elif missing_fields:
                review_code = "REQUIRED_FIELDS_MISSING"
                review_message = (
                    "Faltan campos requeridos por el esquema: " + ", ".join(missing_fields)
                )[:512]
            elif field_counts["conflicts"]:
                review_code = "EXTRACTED_FIELD_CONFLICT"
                review_message = "Hay valores contradictorios para uno o mas campos extraidos."
            elif field_counts["needs_review"]:
                review_code = "EXTRACTED_FIELDS_NEED_REVIEW"
                review_message = "Uno o mas campos no alcanzan el umbral de autoaceptacion."
            elif (
                classification.document_type == AssuranceDocumentType.SPREADSHEET
                and field_counts["structured"] == 0
            ):
                review_code = "NO_STRUCTURED_FIELDS"
                review_message = "La planilla no contiene encabezados semanticos reconocidos."

            if review_code is not None:
                run.status = ExtractionRunStatus.NEEDS_REVIEW.value
                assurance_document.processing_status = DocumentProcessingStatus.NEEDS_REVIEW.value
                assurance_document.last_error_code = review_code
                assurance_document.last_error_message = review_message
            else:
                run.status = ExtractionRunStatus.SUCCEEDED.value
                assurance_document.processing_status = DocumentProcessingStatus.EXTRACTED.value

            record_audit_event(
                session,
                actor=_system_actor(org_id),
                action=AuditAction.ASSURANCE_EXTRACTION_COMPLETE,
                entity_type="assurance_document",
                entity_id=assurance_document.id,
                outcome=AuditOutcome.SUCCESS,
                metadata={
                    "extraction_run_id": run.id,
                    "engine": run.engine,
                    "engine_version": run.engine_version,
                    "force_reprocess": bool(force_reprocess),
                    "raw_field_count": raw_field_count,
                    "structured_field_count": field_counts["structured"],
                    "auto_accepted_field_count": field_counts["auto_accepted"],
                    "review_field_count": field_counts["needs_review"],
                    "field_conflict_count": field_counts["conflicts"],
                    "low_confidence_field_count": field_counts["low_confidence"],
                    "entity_link_count": link_counts["linked"],
                    "ambiguous_entity_match_count": link_counts["ambiguous"],
                    "ocr_required": bool(parsed.ocr_required),
                    "review_code": review_code,
                },
                before_data={"processing_status": DocumentProcessingStatus.PROCESSING.value},
                after_data={
                    "processing_status": assurance_document.processing_status,
                    "semantic_document_type": assurance_document.semantic_document_type,
                    "type_confidence": assurance_document.type_confidence,
                    "review_code": review_code,
                },
                detail="Procesamiento automatico Assurance completado.",
            )
            session.commit()
            return assurance_document.processing_status

        except (DocumentParseError, ValueError) as exc:
            _mark_processing_failure(
                session,
                organization_id=org_id,
                public_id=public_id,
                run_id=run_id,
                error_code="DOCUMENT_PARSE_FAILED",
                detail=str(exc),
            )
            return DocumentProcessingStatus.FAILED.value
        except AssuranceProcessingError:
            raise
        except Exception as exc:
            _mark_processing_failure(
                session,
                organization_id=org_id,
                public_id=public_id,
                run_id=run_id,
                error_code="PROCESSING_FAILED",
                detail="Unexpected Assurance processing failure.",
            )
            raise AssuranceProcessingError("No se pudo procesar el documento Assurance.") from exc
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
            status_value = assurance_document.processing_status
            progress_percent = {
                DocumentProcessingStatus.UPLOADED.value: 20,
                DocumentProcessingStatus.PROCESSING.value: 60,
                DocumentProcessingStatus.EXTRACTED.value: 100,
                DocumentProcessingStatus.NEEDS_REVIEW.value: 100,
                DocumentProcessingStatus.FAILED.value: 100,
            }.get(status_value, 0)
            return {
                "assurance_document_id": str(assurance_document.public_id),
                "processing_status": status_value,
                "progress_percent": progress_percent,
                "semantic_document_type": assurance_document.semantic_document_type,
                "type_confidence": assurance_document.type_confidence,
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
