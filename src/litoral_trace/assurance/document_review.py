"""Compact, tenant-scoped review read model for Assurance document extraction."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Mapping, Sequence
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from litoral_trace.assurance.domain import (
    DocumentProcessingStatus,
    ExtractionRunStatus,
    ReconciliationIssueStatus,
)
from litoral_trace.assurance.normalization import (
    NormalizationError,
    normalize_argentine_number,
    normalize_cuit,
    normalize_date,
    normalize_identifier,
)
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentEntityLink,
    DocumentExtractionRun,
    ExtractedDocumentField,
    ReconciliationIssue,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import (
    AuditAction,
    AuditActor,
    AuditOutcome,
    AuditRequestContext,
    record_audit_event,
)


SessionFactory = Callable[[], Session | None]


class AssuranceDocumentReviewError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ReviewField:
    id: int
    field_name: str
    original_value: str | None
    normalized_value: str | None
    value_type: str
    confidence: float
    confidence_level: str
    source_page: int | None
    source_locator: str | None
    auto_accepted: bool
    needs_review: bool


@dataclass(frozen=True, slots=True)
class ReviewLink:
    entity_type: str
    entity_reference: str
    confidence: float
    method: str
    human_confirmed: bool


@dataclass(frozen=True, slots=True)
class ReviewIssue:
    public_id: UUID
    operation_reference: str
    rule_code: str
    severity: str
    field_name: str
    left_source: str
    right_source: str | None
    left_value: str | None
    right_value: str | None
    explanation: str


@dataclass(frozen=True, slots=True)
class DocumentReviewView:
    assurance_document_id: UUID
    filename: str
    semantic_document_type: str
    type_confidence: float
    processing_status: str
    last_error_code: str | None
    last_error_message: str | None
    structured_field_count: int
    auto_accepted_count: int
    review_count: int
    fields: tuple[ReviewField, ...]
    links: tuple[ReviewLink, ...]
    issues: tuple[ReviewIssue, ...]


@dataclass(frozen=True, slots=True)
class ReviewApprovalResult:
    approved_count: int
    remaining_review_count: int
    processing_status: str


@dataclass(frozen=True, slots=True)
class ReviewCorrectionResult:
    corrected_count: int
    remaining_review_count: int
    processing_status: str


def _normalized_human_value(row: ExtractedDocumentField, value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError(f"El campo {row.field_name} no puede quedar vacío.")

    field_name = str(row.field_name or "").lower()
    value_type = str(row.value_type or "text").lower()
    try:
        if "cuit" in field_name:
            return normalize_cuit(raw)
        if value_type in {"number", "decimal", "quantity"}:
            return str(normalize_argentine_number(raw))
        if value_type in {"date", "datetime"} or field_name.endswith("_date"):
            return normalize_date(raw).isoformat()
        if value_type in {"identifier", "code"}:
            return normalize_identifier(raw)
    except NormalizationError as exc:
        raise ValueError(f"Valor inválido para {row.field_name}: {exc}") from exc
    return raw


def _remaining_review_count(
    session: Session,
    *,
    organization_id: int,
    document_id: int,
    run_id: int,
) -> int:
    return len(
        session.scalars(
            select(ExtractedDocumentField.id).where(
                ExtractedDocumentField.organization_id == organization_id,
                ExtractedDocumentField.assurance_document_id == document_id,
                ExtractedDocumentField.extraction_run_id == run_id,
                ExtractedDocumentField.needs_review.is_(True),
                ~ExtractedDocumentField.field_name.startswith("raw."),
            )
        ).all()
    )


def _finalize_review_state(
    *,
    document: AssuranceDocument,
    latest_run: DocumentExtractionRun,
    remaining_count: int,
) -> None:
    if remaining_count != 0:
        return
    if document.last_error_code == "EXTRACTED_FIELDS_NEED_REVIEW":
        document.processing_status = DocumentProcessingStatus.EXTRACTED.value
        document.last_error_code = None
        document.last_error_message = None
        latest_run.status = ExtractionRunStatus.SUCCEEDED.value


class AssuranceDocumentReviewService:
    def __init__(self, *, session_factory: SessionFactory | None = None) -> None:
        self._session_factory = session_factory or get_db_session

    def _new_session(self, organization_id: int) -> Session:
        session = self._session_factory()
        if session is None:
            raise AssuranceDocumentReviewError("No se pudo abrir una sesión de revisión.")
        set_tenant_db_context(session, int(organization_id))
        return session

    @staticmethod
    def _public_id(value: UUID | str) -> UUID:
        return value if isinstance(value, UUID) else UUID(str(value))

    @staticmethod
    def _load_document(
        session: Session,
        *,
        organization_id: int,
        public_id: UUID,
    ) -> tuple[AssuranceDocument, VaultDocument]:
        document = session.scalar(
            select(AssuranceDocument).where(
                AssuranceDocument.organization_id == organization_id,
                AssuranceDocument.public_id == public_id,
            )
        )
        if document is None:
            raise AssuranceDocumentReviewError("Documento Assurance no encontrado.")
        vault = session.scalar(
            select(VaultDocument).where(
                VaultDocument.organization_id == organization_id,
                VaultDocument.id == document.vault_document_id,
            )
        )
        if vault is None:
            raise AssuranceDocumentReviewError("Documento Vault no encontrado.")
        return document, vault

    @staticmethod
    def _latest_run(
        session: Session,
        *,
        organization_id: int,
        document_id: int,
    ) -> DocumentExtractionRun:
        latest_run = session.scalar(
            select(DocumentExtractionRun)
            .where(
                DocumentExtractionRun.organization_id == organization_id,
                DocumentExtractionRun.assurance_document_id == document_id,
            )
            .order_by(DocumentExtractionRun.id.desc())
        )
        if latest_run is None:
            raise AssuranceDocumentReviewError(
                "El documento todavía no tiene una extracción revisable."
            )
        return latest_run

    def get(
        self,
        *,
        organization_id: int,
        assurance_public_id: UUID | str,
    ) -> DocumentReviewView:
        org_id = int(organization_id)
        public_id = self._public_id(assurance_public_id)
        session = self._new_session(org_id)
        try:
            document, vault = self._load_document(
                session,
                organization_id=org_id,
                public_id=public_id,
            )
            latest_run = session.scalar(
                select(DocumentExtractionRun)
                .where(
                    DocumentExtractionRun.organization_id == org_id,
                    DocumentExtractionRun.assurance_document_id == document.id,
                )
                .order_by(DocumentExtractionRun.id.desc())
            )
            fields: list[ExtractedDocumentField] = []
            if latest_run is not None:
                fields = list(
                    session.scalars(
                        select(ExtractedDocumentField)
                        .where(
                            ExtractedDocumentField.organization_id == org_id,
                            ExtractedDocumentField.assurance_document_id == document.id,
                            ExtractedDocumentField.extraction_run_id == latest_run.id,
                            ~ExtractedDocumentField.field_name.startswith("raw."),
                        )
                        .order_by(
                            ExtractedDocumentField.needs_review.desc(),
                            ExtractedDocumentField.field_name.asc(),
                            ExtractedDocumentField.id.asc(),
                        )
                    ).all()
                )
            links = list(
                session.scalars(
                    select(DocumentEntityLink)
                    .where(
                        DocumentEntityLink.organization_id == org_id,
                        DocumentEntityLink.assurance_document_id == document.id,
                    )
                    .order_by(DocumentEntityLink.entity_type, DocumentEntityLink.id)
                ).all()
            )
            issues = list(
                session.scalars(
                    select(ReconciliationIssue)
                    .where(
                        ReconciliationIssue.organization_id == org_id,
                        ReconciliationIssue.status == ReconciliationIssueStatus.OPEN.value,
                        or_(
                            ReconciliationIssue.left_document_id == document.id,
                            ReconciliationIssue.right_document_id == document.id,
                        ),
                    )
                    .order_by(
                        ReconciliationIssue.severity.asc(),
                        ReconciliationIssue.id.asc(),
                    )
                ).all()
            )
            field_views = tuple(
                ReviewField(
                    id=row.id,
                    field_name=row.field_name,
                    original_value=row.original_value,
                    normalized_value=row.normalized_value,
                    value_type=row.value_type,
                    confidence=float(row.confidence),
                    confidence_level=row.confidence_level,
                    source_page=row.source_page,
                    source_locator=row.source_locator,
                    auto_accepted=bool(row.auto_accepted),
                    needs_review=bool(row.needs_review),
                )
                for row in fields
            )
            return DocumentReviewView(
                assurance_document_id=document.public_id,
                filename=vault.original_filename,
                semantic_document_type=document.semantic_document_type,
                type_confidence=float(document.type_confidence),
                processing_status=document.processing_status,
                last_error_code=document.last_error_code,
                last_error_message=document.last_error_message,
                structured_field_count=len(field_views),
                auto_accepted_count=sum(1 for row in field_views if row.auto_accepted),
                review_count=sum(1 for row in field_views if row.needs_review),
                fields=field_views,
                links=tuple(
                    ReviewLink(
                        entity_type=row.entity_type,
                        entity_reference=row.entity_reference,
                        confidence=float(row.link_confidence),
                        method=row.link_method,
                        human_confirmed=bool(row.human_confirmed),
                    )
                    for row in links
                ),
                issues=tuple(
                    ReviewIssue(
                        public_id=row.public_id,
                        operation_reference=row.operation_reference,
                        rule_code=row.rule_code,
                        severity=row.severity,
                        field_name=row.field_name,
                        left_source=row.left_source,
                        right_source=row.right_source,
                        left_value=row.left_value,
                        right_value=row.right_value,
                        explanation=row.explanation,
                    )
                    for row in issues
                ),
            )
        except (ValueError, AssuranceDocumentReviewError):
            raise
        except Exception as exc:
            raise AssuranceDocumentReviewError("No se pudo consultar la revisión documental.") from exc
        finally:
            session.close()

    def approve_fields(
        self,
        *,
        organization_id: int,
        assurance_public_id: UUID | str,
        field_ids: Sequence[int],
        actor: AuditActor,
        request_context: AuditRequestContext | None = None,
    ) -> ReviewApprovalResult:
        org_id = int(organization_id)
        public_id = self._public_id(assurance_public_id)
        requested_ids = tuple(dict.fromkeys(int(value) for value in field_ids))
        if not requested_ids:
            raise ValueError("Debe seleccionarse al menos un campo para aprobar.")
        if actor.organization_id != org_id:
            raise ValueError("El actor de auditoría no pertenece al tenant solicitado.")

        session = self._new_session(org_id)
        try:
            document, _ = self._load_document(
                session,
                organization_id=org_id,
                public_id=public_id,
            )
            latest_run = self._latest_run(
                session,
                organization_id=org_id,
                document_id=document.id,
            )
            rows = list(
                session.scalars(
                    select(ExtractedDocumentField).where(
                        ExtractedDocumentField.organization_id == org_id,
                        ExtractedDocumentField.assurance_document_id == document.id,
                        ExtractedDocumentField.extraction_run_id == latest_run.id,
                        ExtractedDocumentField.id.in_(requested_ids),
                        ~ExtractedDocumentField.field_name.startswith("raw."),
                    )
                ).all()
            )
            if {row.id for row in rows} != set(requested_ids):
                raise AssuranceDocumentReviewError(
                    "Uno o más campos no pertenecen a la última extracción del documento."
                )

            before = {
                "field_ids": [row.id for row in rows],
                "review_count": sum(1 for row in rows if row.needs_review),
            }
            approved = 0
            for row in rows:
                if not row.needs_review:
                    continue
                row.needs_review = False
                row.auto_accepted = False
                approved += 1

            remaining_count = _remaining_review_count(
                session,
                organization_id=org_id,
                document_id=document.id,
                run_id=latest_run.id,
            )
            _finalize_review_state(
                document=document,
                latest_run=latest_run,
                remaining_count=remaining_count,
            )

            record_audit_event(
                session,
                actor=actor,
                action=AuditAction.ASSURANCE_REVIEW_APPROVE,
                entity_type="assurance_document",
                entity_id=document.id,
                outcome=AuditOutcome.SUCCESS,
                request_context=request_context,
                metadata={
                    "assurance_document_id": str(document.public_id),
                    "approved_field_ids": [row.id for row in rows if not row.needs_review],
                    "approved_count": approved,
                    "remaining_review_count": remaining_count,
                },
                before_data=before,
                after_data={
                    "processing_status": document.processing_status,
                    "remaining_review_count": remaining_count,
                },
            )
            session.commit()
            return ReviewApprovalResult(
                approved_count=approved,
                remaining_review_count=remaining_count,
                processing_status=document.processing_status,
            )
        except (ValueError, AssuranceDocumentReviewError):
            session.rollback()
            raise
        except Exception as exc:
            session.rollback()
            raise AssuranceDocumentReviewError("No se pudieron aprobar los campos seleccionados.") from exc
        finally:
            session.close()

    def correct_fields(
        self,
        *,
        organization_id: int,
        assurance_public_id: UUID | str,
        corrections: Mapping[int, object],
        actor: AuditActor,
        request_context: AuditRequestContext | None = None,
    ) -> ReviewCorrectionResult:
        """Persist explicit human corrections while preserving extracted originals."""
        org_id = int(organization_id)
        public_id = self._public_id(assurance_public_id)
        normalized_input = {int(field_id): value for field_id, value in corrections.items()}
        if not normalized_input:
            raise ValueError("Debe enviarse al menos una corrección.")
        if len(normalized_input) > 200:
            raise ValueError("Se permiten hasta 200 correcciones por solicitud.")
        if actor.organization_id != org_id:
            raise ValueError("El actor de auditoría no pertenece al tenant solicitado.")

        session = self._new_session(org_id)
        try:
            document, _ = self._load_document(
                session,
                organization_id=org_id,
                public_id=public_id,
            )
            latest_run = self._latest_run(
                session,
                organization_id=org_id,
                document_id=document.id,
            )
            rows = list(
                session.scalars(
                    select(ExtractedDocumentField).where(
                        ExtractedDocumentField.organization_id == org_id,
                        ExtractedDocumentField.assurance_document_id == document.id,
                        ExtractedDocumentField.extraction_run_id == latest_run.id,
                        ExtractedDocumentField.id.in_(tuple(normalized_input)),
                        ~ExtractedDocumentField.field_name.startswith("raw."),
                    )
                ).all()
            )
            if {row.id for row in rows} != set(normalized_input):
                raise AssuranceDocumentReviewError(
                    "Uno o más campos no pertenecen a la última extracción del documento."
                )

            before_fields: list[dict[str, object]] = []
            after_fields: list[dict[str, object]] = []
            corrected = 0
            for row in rows:
                previous = row.normalized_value if row.normalized_value is not None else row.original_value
                corrected_value = _normalized_human_value(row, normalized_input[row.id])
                before_fields.append(
                    {
                        "field_id": row.id,
                        "field_name": row.field_name,
                        "effective_value": previous,
                        "needs_review": bool(row.needs_review),
                        "auto_accepted": bool(row.auto_accepted),
                    }
                )
                if str(previous or "") != corrected_value:
                    corrected += 1
                row.normalized_value = corrected_value
                row.needs_review = False
                # A human-corrected field is trusted for downstream deterministic
                # rules, but its provenance must never be mislabeled as automatic.
                row.auto_accepted = False
                after_fields.append(
                    {
                        "field_id": row.id,
                        "field_name": row.field_name,
                        "effective_value": corrected_value,
                        "needs_review": False,
                        "auto_accepted": False,
                    }
                )

            remaining_count = _remaining_review_count(
                session,
                organization_id=org_id,
                document_id=document.id,
                run_id=latest_run.id,
            )
            _finalize_review_state(
                document=document,
                latest_run=latest_run,
                remaining_count=remaining_count,
            )
            record_audit_event(
                session,
                actor=actor,
                action=AuditAction.ASSURANCE_REVIEW_CORRECT,
                entity_type="assurance_document",
                entity_id=document.id,
                outcome=AuditOutcome.SUCCESS,
                request_context=request_context,
                metadata={
                    "assurance_document_id": str(document.public_id),
                    "corrected_count": corrected,
                    "submitted_field_count": len(rows),
                    "remaining_review_count": remaining_count,
                },
                before_data={"fields": before_fields},
                after_data={
                    "fields": after_fields,
                    "processing_status": document.processing_status,
                    "remaining_review_count": remaining_count,
                },
            )
            session.commit()
            return ReviewCorrectionResult(
                corrected_count=corrected,
                remaining_review_count=remaining_count,
                processing_status=document.processing_status,
            )
        except (ValueError, AssuranceDocumentReviewError):
            session.rollback()
            raise
        except Exception as exc:
            session.rollback()
            raise AssuranceDocumentReviewError("No se pudieron corregir los campos seleccionados.") from exc
        finally:
            session.close()
