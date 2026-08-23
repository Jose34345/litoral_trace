"""Contextual documentary evidence for the industrial traceability graph.

Vault remains the single binary store. This service only links an available
Vault document to exactly one tenant-scoped traceability subject and preserves
unlink history instead of deleting evidence relationships.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import (
    Lote,
    Shipment,
    TraceabilityBatch,
    TraceabilityEvent,
    TraceabilityEvidenceLink,
    VaultDocument,
)
from litoral_trace.db.models.traceability_evidence_link import (
    TRACEABILITY_EVIDENCE_SUBJECT_TYPES,
    TRACEABILITY_EVIDENCE_TYPES,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import AuditActor


_EVIDENCE_REQUIRED_CONTENT_TYPE = {
    "PHYTOSANITARY_CERTIFICATE": "application/pdf",
    "EPHYTO_XML": "application/xml",
}


class TraceabilityEvidenceError(RuntimeError):
    """Base safe error for contextual evidence workflows."""


class TraceabilityEvidenceAuthorizationError(TraceabilityEvidenceError):
    pass


class TraceabilityEvidenceNotFoundError(TraceabilityEvidenceError):
    pass


class TraceabilityEvidenceConflictError(TraceabilityEvidenceError):
    pass


class TraceabilityEvidencePersistenceError(TraceabilityEvidenceError):
    pass


class TraceabilityEvidenceValidationError(TraceabilityEvidenceError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail)


@dataclass(frozen=True)
class EvidenceSubjectChoice:
    subject_type: str
    reference: str
    label: str
    secondary: str
    status: str


@dataclass(frozen=True)
class TraceabilityEvidenceView:
    link_public_id: UUID
    subject_type: str
    subject_reference: str
    subject_label: str
    evidence_type: str
    reference_number: str | None
    issuer: str | None
    document_date: date | None
    valid_from: date | None
    valid_until: date | None
    notes: str | None
    linked_at: datetime
    linked_by_user_id: int | None
    vault_document_public_id: UUID
    document_filename: str
    document_type: str
    document_content_type: str
    document_size_bytes: int
    document_sha256: str
    document_status: str


@dataclass(frozen=True)
class EvidenceCoverageView:
    total_subjects: int
    subjects_with_evidence: int
    percentage: int
    by_subject_type: dict[str, tuple[int, int]]


@dataclass(frozen=True)
class EvidenceWorkspaceSnapshot:
    subjects: tuple[EvidenceSubjectChoice, ...]
    documents: tuple[Any, ...]
    evidence: tuple[TraceabilityEvidenceView, ...]
    coverage: EvidenceCoverageView


@dataclass(frozen=True)
class TraceabilityEvidenceLinkResult:
    evidence: TraceabilityEvidenceView
    replayed: bool


def _normalize_subject_type(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in TRACEABILITY_EVIDENCE_SUBJECT_TYPES:
        raise TraceabilityEvidenceValidationError(
            "INVALID_SUBJECT_TYPE",
            "El tipo de eslabón de trazabilidad no está permitido.",
        )
    return normalized


def _normalize_evidence_type(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in TRACEABILITY_EVIDENCE_TYPES:
        raise TraceabilityEvidenceValidationError(
            "INVALID_EVIDENCE_TYPE",
            "El tipo de evidencia no está permitido.",
        )
    return normalized


def _optional_text(value: Any, *, maximum: int) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    if len(normalized) > maximum:
        raise TraceabilityEvidenceValidationError(
            "TEXT_TOO_LONG",
            f"El texto supera el máximo de {maximum} caracteres.",
        )
    return normalized


def _coerce_uuid(value: Any) -> UUID:
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError) as exc:
        raise TraceabilityEvidenceNotFoundError(
            "El eslabón de trazabilidad no existe en la organización."
        ) from exc


def _validate_evidence_document_content_type(
    evidence_type: str,
    document: VaultDocument,
) -> None:
    required = _EVIDENCE_REQUIRED_CONTENT_TYPE.get(evidence_type)
    if required is None:
        return
    observed = str(document.content_type or "").split(";", 1)[0].strip().lower()
    if observed != required:
        label = "XML ePhyto" if evidence_type == "EPHYTO_XML" else "certificado fitosanitario PDF"
        raise TraceabilityEvidenceValidationError(
            "EVIDENCE_CONTENT_TYPE_MISMATCH",
            f"La evidencia {label} debe provenir de un archivo {required} validado en Vault.",
        )


class TraceabilityEvidenceService:
    """Link and inspect Vault evidence without duplicating stored documents."""

    def __init__(self, *, session_factory=None) -> None:
        self._session_factory = session_factory or get_db_session

    def _session(self, organization_id: int) -> Session:
        session = self._session_factory()
        if session is None:
            raise TraceabilityEvidencePersistenceError(
                "Servicio de base de datos no disponible."
            )
        set_tenant_db_context(session, int(organization_id))
        return session

    @staticmethod
    def _actor_scope(actor: AuditActor, organization_id: int) -> None:
        if int(actor.organization_id) != int(organization_id):
            raise TraceabilityEvidenceAuthorizationError(
                "El actor autenticado no pertenece a la organización activa."
            )

    @staticmethod
    def _resolve_subject(
        session: Session,
        *,
        organization_id: int,
        subject_type: str,
        reference: Any,
    ) -> tuple[Any, str, str, dict[str, int | None]]:
        if subject_type == "SOURCE_LOTE":
            identifier = str(reference or "").strip()
            record = session.scalar(
                select(Lote).where(
                    Lote.organization_id == organization_id,
                    func.lower(Lote.identificador) == identifier.lower(),
                )
            )
            if record is None:
                raise TraceabilityEvidenceNotFoundError(
                    "La parcela o rodal no existe en la organización."
                )
            return (
                record,
                record.identificador,
                f"Origen · {record.identificador}",
                {
                    "source_lote_id": int(record.id),
                    "traceability_event_id": None,
                    "traceability_batch_id": None,
                    "shipment_id": None,
                },
            )

        public_id = _coerce_uuid(reference)
        if subject_type == "TRACEABILITY_EVENT":
            record = session.scalar(
                select(TraceabilityEvent).where(
                    TraceabilityEvent.organization_id == organization_id,
                    TraceabilityEvent.public_id == public_id,
                )
            )
            label = f"Movimiento · {record.event_code}" if record else ""
            field = "traceability_event_id"
        elif subject_type == "TRACEABILITY_BATCH":
            record = session.scalar(
                select(TraceabilityBatch).where(
                    TraceabilityBatch.organization_id == organization_id,
                    TraceabilityBatch.public_id == public_id,
                )
            )
            label = f"Lote · {record.code}" if record else ""
            field = "traceability_batch_id"
        else:
            record = session.scalar(
                select(Shipment).where(
                    Shipment.organization_id == organization_id,
                    Shipment.public_id == public_id,
                )
            )
            label = f"Despacho · {record.shipment_code}" if record else ""
            field = "shipment_id"

        if record is None:
            raise TraceabilityEvidenceNotFoundError(
                "El eslabón de trazabilidad no existe en la organización."
            )
        subject_ids = {
            "source_lote_id": None,
            "traceability_event_id": None,
            "traceability_batch_id": None,
            "shipment_id": None,
        }
        subject_ids[field] = int(record.id)
        return record, str(public_id), label, subject_ids

    @staticmethod
    def _subject_identity(
        session: Session,
        link: TraceabilityEvidenceLink,
    ) -> tuple[str, str, str]:
        if link.source_lote_id is not None:
            record = session.get(Lote, link.source_lote_id)
            return (
                "SOURCE_LOTE",
                record.identificador if record else "—",
                f"Origen · {record.identificador}" if record else "Origen no disponible",
            )
        if link.traceability_event_id is not None:
            record = session.get(TraceabilityEvent, link.traceability_event_id)
            return (
                "TRACEABILITY_EVENT",
                str(record.public_id) if record else "—",
                f"Movimiento · {record.event_code}" if record else "Movimiento no disponible",
            )
        if link.traceability_batch_id is not None:
            record = session.get(TraceabilityBatch, link.traceability_batch_id)
            return (
                "TRACEABILITY_BATCH",
                str(record.public_id) if record else "—",
                f"Lote · {record.code}" if record else "Lote no disponible",
            )
        record = session.get(Shipment, link.shipment_id)
        return (
            "SHIPMENT",
            str(record.public_id) if record else "—",
            f"Despacho · {record.shipment_code}" if record else "Despacho no disponible",
        )

    @classmethod
    def _view(
        cls,
        session: Session,
        link: TraceabilityEvidenceLink,
        document: VaultDocument,
    ) -> TraceabilityEvidenceView:
        subject_type, subject_reference, subject_label = cls._subject_identity(
            session, link
        )
        return TraceabilityEvidenceView(
            link_public_id=link.public_id,
            subject_type=subject_type,
            subject_reference=subject_reference,
            subject_label=subject_label,
            evidence_type=link.evidence_type,
            reference_number=link.reference_number,
            issuer=link.issuer,
            document_date=link.document_date,
            valid_from=link.valid_from,
            valid_until=link.valid_until,
            notes=link.notes,
            linked_at=link.created_at,
            linked_by_user_id=link.created_by_user_id,
            vault_document_public_id=document.public_id,
            document_filename=document.original_filename,
            document_type=document.document_type,
            document_content_type=document.content_type,
            document_size_bytes=int(document.size_bytes),
            document_sha256=document.sha256,
            document_status=document.status,
        )

    def list_subjects(self, *, organization_id: int) -> tuple[EvidenceSubjectChoice, ...]:
        session = self._session(int(organization_id))
        try:
            result: list[EvidenceSubjectChoice] = []
            for lote in session.scalars(
                select(Lote).where(Lote.organization_id == organization_id).order_by(Lote.identificador)
            ):
                result.append(
                    EvidenceSubjectChoice(
                        subject_type="SOURCE_LOTE",
                        reference=lote.identificador,
                        label=lote.identificador,
                        secondary=f"{lote.productor_id} · {lote.producto_forestal}",
                        status=lote.estatus,
                    )
                )
            for event in session.scalars(
                select(TraceabilityEvent)
                .where(TraceabilityEvent.organization_id == organization_id)
                .order_by(TraceabilityEvent.occurred_at.desc())
            ):
                result.append(
                    EvidenceSubjectChoice(
                        subject_type="TRACEABILITY_EVENT",
                        reference=str(event.public_id),
                        label=event.event_code,
                        secondary=f"Movimiento · {event.event_type}",
                        status=event.status,
                    )
                )
            for batch in session.scalars(
                select(TraceabilityBatch)
                .where(TraceabilityBatch.organization_id == organization_id)
                .order_by(TraceabilityBatch.created_at.desc())
            ):
                result.append(
                    EvidenceSubjectChoice(
                        subject_type="TRACEABILITY_BATCH",
                        reference=str(batch.public_id),
                        label=batch.code,
                        secondary=f"{batch.product_name} · {batch.unit}",
                        status=batch.status,
                    )
                )
            for shipment in session.scalars(
                select(Shipment)
                .where(Shipment.organization_id == organization_id)
                .order_by(Shipment.created_at.desc())
            ):
                result.append(
                    EvidenceSubjectChoice(
                        subject_type="SHIPMENT",
                        reference=str(shipment.public_id),
                        label=shipment.shipment_code,
                        secondary=shipment.buyer_reference or shipment.sale_reference or "Despacho",
                        status=shipment.status,
                    )
                )
            return tuple(result)
        except SQLAlchemyError as exc:
            raise TraceabilityEvidencePersistenceError(
                "No fue posible consultar los eslabones de trazabilidad."
            ) from exc
        finally:
            session.close()

    def list_evidence(
        self,
        *,
        organization_id: int,
        subject_type: str | None = None,
        subject_reference: Any | None = None,
    ) -> tuple[TraceabilityEvidenceView, ...]:
        session = self._session(int(organization_id))
        try:
            statement = (
                select(TraceabilityEvidenceLink, VaultDocument)
                .join(
                    VaultDocument,
                    VaultDocument.id == TraceabilityEvidenceLink.vault_document_id,
                )
                .where(
                    TraceabilityEvidenceLink.organization_id == organization_id,
                    TraceabilityEvidenceLink.unlinked_at.is_(None),
                )
                .order_by(TraceabilityEvidenceLink.created_at.desc())
            )
            if subject_type and subject_reference is not None:
                normalized_type = _normalize_subject_type(subject_type)
                _, _, _, subject_ids = self._resolve_subject(
                    session,
                    organization_id=organization_id,
                    subject_type=normalized_type,
                    reference=subject_reference,
                )
                for field, value in subject_ids.items():
                    if value is not None:
                        statement = statement.where(
                            getattr(TraceabilityEvidenceLink, field) == value
                        )
                        break
            rows = session.execute(statement).all()
            return tuple(self._view(session, link, document) for link, document in rows)
        except TraceabilityEvidenceError:
            raise
        except SQLAlchemyError as exc:
            raise TraceabilityEvidencePersistenceError(
                "No fue posible consultar la evidencia de trazabilidad."
            ) from exc
        finally:
            session.close()

    def coverage(self, *, organization_id: int) -> EvidenceCoverageView:
        subjects = self.list_subjects(organization_id=organization_id)
        evidence = self.list_evidence(organization_id=organization_id)
        evidenced_keys = {
            (item.subject_type, item.subject_reference) for item in evidence
        }
        by_type: dict[str, tuple[int, int]] = {}
        for subject_type in TRACEABILITY_EVIDENCE_SUBJECT_TYPES:
            typed = [item for item in subjects if item.subject_type == subject_type]
            evidenced = sum(
                (item.subject_type, item.reference) in evidenced_keys for item in typed
            )
            by_type[subject_type] = (evidenced, len(typed))
        total = len(subjects)
        covered = sum((item.subject_type, item.reference) in evidenced_keys for item in subjects)
        percentage = int(round((covered / total) * 100)) if total else 0
        return EvidenceCoverageView(
            total_subjects=total,
            subjects_with_evidence=covered,
            percentage=percentage,
            by_subject_type=by_type,
        )

    def workspace_snapshot(self, *, organization_id: int, documents: tuple[Any, ...]) -> EvidenceWorkspaceSnapshot:
        return EvidenceWorkspaceSnapshot(
            subjects=self.list_subjects(organization_id=organization_id),
            documents=documents,
            evidence=self.list_evidence(organization_id=organization_id),
            coverage=self.coverage(organization_id=organization_id),
        )

    def link_evidence(
        self,
        *,
        organization_id: int,
        actor: AuditActor,
        subject_type: str,
        subject_reference: Any,
        vault_document_id: UUID | str,
        evidence_type: str,
        reference_number: Any = None,
        issuer: Any = None,
        document_date: date | None = None,
        valid_from: date | None = None,
        valid_until: date | None = None,
        notes: Any = None,
    ) -> TraceabilityEvidenceLinkResult:
        organization_id = int(organization_id)
        self._actor_scope(actor, organization_id)
        normalized_subject = _normalize_subject_type(subject_type)
        normalized_evidence = _normalize_evidence_type(evidence_type)
        if valid_from and valid_until and valid_until < valid_from:
            raise TraceabilityEvidenceValidationError(
                "INVALID_VALIDITY_RANGE",
                "La fecha de vencimiento no puede ser anterior al inicio de vigencia.",
            )
        document_public_id = _coerce_uuid(vault_document_id)
        session = self._session(organization_id)
        try:
            _, subject_reference_value, _, subject_ids = self._resolve_subject(
                session,
                organization_id=organization_id,
                subject_type=normalized_subject,
                reference=subject_reference,
            )
            document = session.scalar(
                select(VaultDocument).where(
                    VaultDocument.organization_id == organization_id,
                    VaultDocument.public_id == document_public_id,
                    VaultDocument.status == "available",
                )
            )
            if document is None:
                raise TraceabilityEvidenceNotFoundError(
                    "El documento no existe o no está disponible en Documentos y evidencias."
                )
            _validate_evidence_document_content_type(normalized_evidence, document)

            duplicate_filters = [
                TraceabilityEvidenceLink.organization_id == organization_id,
                TraceabilityEvidenceLink.vault_document_id == document.id,
                TraceabilityEvidenceLink.unlinked_at.is_(None),
            ]
            for field, value in subject_ids.items():
                duplicate_filters.append(
                    getattr(TraceabilityEvidenceLink, field).is_(None)
                    if value is None
                    else getattr(TraceabilityEvidenceLink, field) == value
                )
            existing = session.scalar(select(TraceabilityEvidenceLink).where(*duplicate_filters))
            if existing is not None:
                if existing.evidence_type != normalized_evidence:
                    raise TraceabilityEvidenceConflictError(
                        "El documento ya está vinculado a ese eslabón con otro tipo de evidencia."
                    )
                return TraceabilityEvidenceLinkResult(
                    evidence=self._view(session, existing, document),
                    replayed=True,
                )

            link = TraceabilityEvidenceLink(
                organization_id=organization_id,
                vault_document_id=document.id,
                evidence_type=normalized_evidence,
                reference_number=_optional_text(reference_number, maximum=160),
                issuer=_optional_text(issuer, maximum=200),
                document_date=document_date,
                valid_from=valid_from,
                valid_until=valid_until,
                notes=_optional_text(notes, maximum=2000),
                created_by_user_id=actor.user_id,
                **subject_ids,
            )
            session.add(link)
            session.flush()
            view = self._view(session, link, document)
            session.commit()
            return TraceabilityEvidenceLinkResult(evidence=view, replayed=False)
        except TraceabilityEvidenceError:
            session.rollback()
            raise
        except IntegrityError as exc:
            session.rollback()
            raise TraceabilityEvidenceConflictError(
                "El documento ya está vinculado a ese eslabón."
            ) from exc
        except SQLAlchemyError as exc:
            session.rollback()
            raise TraceabilityEvidencePersistenceError(
                "No fue posible vincular la evidencia."
            ) from exc
        finally:
            session.close()

    def unlink_evidence(
        self,
        *,
        organization_id: int,
        actor: AuditActor,
        link_public_id: UUID | str,
    ) -> None:
        organization_id = int(organization_id)
        self._actor_scope(actor, organization_id)
        public_id = _coerce_uuid(link_public_id)
        session = self._session(organization_id)
        try:
            link = session.scalar(
                select(TraceabilityEvidenceLink).where(
                    TraceabilityEvidenceLink.organization_id == organization_id,
                    TraceabilityEvidenceLink.public_id == public_id,
                    TraceabilityEvidenceLink.unlinked_at.is_(None),
                )
            )
            if link is None:
                raise TraceabilityEvidenceNotFoundError(
                    "El vínculo de evidencia no existe o ya fue desvinculado."
                )
            link.unlinked_at = datetime.now(timezone.utc)
            link.unlinked_by_user_id = actor.user_id
            session.commit()
        except TraceabilityEvidenceError:
            session.rollback()
            raise
        except SQLAlchemyError as exc:
            session.rollback()
            raise TraceabilityEvidencePersistenceError(
                "No fue posible desvincular la evidencia."
            ) from exc
        finally:
            session.close()
