"""Tenant-safe linkage between completed batch imports and Vault evidence."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import (
    BatchEvidenceLink,
    BatchImport,
    VaultDocument,
)
from litoral_trace.db.models.batch_evidence_link import (
    BATCH_EVIDENCE_TYPES,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import (
    AuditAction,
    AuditActor,
    AuditOutcome,
    AuditRequestContext,
    record_audit_event,
)


ACTIVE_PAIR_INDEX = "uq_batch_evidence_links_active_pair"
ACTIVE_SOURCE_INDEX = "uq_batch_evidence_links_active_source"


class BatchEvidenceError(RuntimeError):
    """Base sanitized error for batch evidence linkage."""


class BatchEvidenceAuthorizationError(BatchEvidenceError):
    """Trusted actor and requested tenant disagree."""


class BatchEvidenceValidationError(BatchEvidenceError):
    """Client-supplied evidence linkage metadata is invalid."""

    def __init__(
        self,
        code: str,
        message: str,
    ) -> None:
        self.code = code
        super().__init__(message)


class BatchEvidenceNotFoundError(BatchEvidenceError):
    """Tenant-visible batch, Vault document, or active link does not exist."""

    def __init__(
        self,
        code: str = "EVIDENCE_NOT_FOUND",
        message: str = "Recurso de evidencia no encontrado.",
    ) -> None:
        self.code = code
        super().__init__(message)


class BatchEvidenceConflictError(BatchEvidenceError):
    """Requested linkage conflicts with immutable or active evidence state."""

    def __init__(
        self,
        code: str,
        message: str,
    ) -> None:
        self.code = code
        super().__init__(message)


class BatchEvidencePersistenceError(BatchEvidenceError):
    """Sanitized database failure."""


@dataclass(frozen=True)
class BatchEvidenceView:
    link_internal_id: int
    link_public_id: UUID
    organization_id: int
    batch_import_public_id: UUID
    vault_document_public_id: UUID
    evidence_type: str
    linked_at: datetime
    linked_by_user_id: int | None
    document_filename: str
    document_type: str
    document_content_type: str
    document_size_bytes: int
    document_sha256: str
    document_status: str

    @property
    def document_available(self) -> bool:
        return self.document_status == "available"


@dataclass(frozen=True)
class BatchEvidenceLinkResult:
    evidence: BatchEvidenceView
    replayed: bool


SessionFactory = Callable[[], Session | None]


def normalize_evidence_type(
    evidence_type: str,
) -> str:
    normalized = str(evidence_type or "").strip().upper()
    if normalized not in BATCH_EVIDENCE_TYPES:
        raise BatchEvidenceValidationError(
            "INVALID_EVIDENCE_TYPE",
            "El tipo de evidencia no está permitido.",
        )
    return normalized


def _normalize_organization_id(
    organization_id: int | str,
) -> int:
    try:
        normalized = int(organization_id)
    except (TypeError, ValueError) as exc:
        raise BatchEvidenceAuthorizationError(
            "El tenant de evidencia no es válido."
        ) from exc

    if normalized <= 0:
        raise BatchEvidenceAuthorizationError(
            "El tenant de evidencia no es válido."
        )
    return normalized


def _coerce_uuid(
    value: UUID | str,
    *,
    not_found_code: str,
) -> UUID:
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError) as exc:
        raise BatchEvidenceNotFoundError(
            not_found_code,
            "Recurso de evidencia no encontrado.",
        ) from exc


def _constraint_name(
    exc: IntegrityError,
) -> str | None:
    original = getattr(exc, "orig", None)
    diagnostic = getattr(original, "diag", None)
    value = getattr(diagnostic, "constraint_name", None)
    return str(value) if value else None


def _view(
    *,
    link: BatchEvidenceLink,
    batch_import: BatchImport,
    document: VaultDocument,
) -> BatchEvidenceView:
    return BatchEvidenceView(
        link_internal_id=int(link.id),
        link_public_id=link.public_id,
        organization_id=int(link.organization_id),
        batch_import_public_id=batch_import.public_id,
        vault_document_public_id=document.public_id,
        evidence_type=link.evidence_type,
        linked_at=link.created_at,
        linked_by_user_id=link.created_by_user_id,
        document_filename=document.original_filename,
        document_type=document.document_type,
        document_content_type=document.content_type,
        document_size_bytes=int(document.size_bytes),
        document_sha256=document.sha256,
        document_status=document.status,
    )


def _audit_metadata(
    evidence: BatchEvidenceView,
) -> dict[str, Any]:
    return {
        "batch_import_public_id": str(
            evidence.batch_import_public_id
        ),
        "vault_document_public_id": str(
            evidence.vault_document_public_id
        ),
        "evidence_type": evidence.evidence_type,
        "vault_document_type": evidence.document_type,
        "vault_sha256": evidence.document_sha256,
        "vault_status": evidence.document_status,
    }


class BatchEvidenceService:
    """
    Persist active evidence relationships without duplicating Vault objects.

    The database enforces tenant-consistent composite foreign keys. The service
    additionally applies RLS tenant context, requires a completed BatchImport,
    and only creates links to currently available Vault documents.

    SOURCE_WORKBOOK is stronger than a generic supporting link: it must point
    to a REMITO_EXCEL document whose SHA-256 exactly matches the imported XLSX.
    """

    def __init__(
        self,
        *,
        session_factory: SessionFactory | None = None,
    ) -> None:
        self._session_factory = session_factory or get_db_session

    @staticmethod
    def _validate_actor_scope(
        *,
        actor: AuditActor,
        organization_id: int,
    ) -> None:
        if int(actor.organization_id) != organization_id:
            raise BatchEvidenceAuthorizationError(
                "El actor autenticado no pertenece al tenant de evidencia."
            )

    @staticmethod
    def _get_import(
        session: Session,
        *,
        organization_id: int,
        public_id: UUID,
        lock: bool = False,
    ) -> BatchImport:
        statement = select(BatchImport).where(
            BatchImport.organization_id == organization_id,
            BatchImport.public_id == public_id,
            BatchImport.status == "completed",
        )
        if lock:
            statement = statement.with_for_update()

        record = session.scalar(statement)
        if record is None:
            raise BatchEvidenceNotFoundError(
                "BATCH_IMPORT_NOT_FOUND",
                "Importación batch no encontrada.",
            )
        return record

    @staticmethod
    def _get_document(
        session: Session,
        *,
        organization_id: int,
        public_id: UUID,
        require_available: bool,
        lock: bool = False,
    ) -> VaultDocument:
        statement = select(VaultDocument).where(
            VaultDocument.organization_id == organization_id,
            VaultDocument.public_id == public_id,
        )
        if require_available:
            statement = statement.where(
                VaultDocument.status == "available"
            )
        if lock:
            statement = statement.with_for_update()

        document = session.scalar(statement)
        if document is None:
            raise BatchEvidenceNotFoundError(
                "VAULT_DOCUMENT_NOT_FOUND",
                "Documento Vault no encontrado.",
            )
        return document

    @staticmethod
    def _validate_source_workbook(
        *,
        batch_import: BatchImport,
        document: VaultDocument,
    ) -> None:
        if document.document_type != "REMITO_EXCEL":
            raise BatchEvidenceConflictError(
                "SOURCE_WORKBOOK_REQUIRES_REMITO_EXCEL",
                (
                    "La evidencia SOURCE_WORKBOOK debe ser "
                    "un documento Vault REMITO_EXCEL."
                ),
            )

        if document.sha256 != batch_import.source_sha256:
            raise BatchEvidenceConflictError(
                "SOURCE_WORKBOOK_HASH_MISMATCH",
                (
                    "La huella SHA-256 del documento Vault no coincide "
                    "con la planilla importada."
                ),
            )

    @staticmethod
    def _active_pair(
        session: Session,
        *,
        batch_import_id: int,
        vault_document_id: int,
        lock: bool = False,
    ) -> BatchEvidenceLink | None:
        statement = select(BatchEvidenceLink).where(
            BatchEvidenceLink.batch_import_id == batch_import_id,
            BatchEvidenceLink.vault_document_id == vault_document_id,
            BatchEvidenceLink.unlinked_at.is_(None),
        )
        if lock:
            statement = statement.with_for_update()
        return session.scalar(statement)

    @staticmethod
    def _active_source(
        session: Session,
        *,
        batch_import_id: int,
        lock: bool = False,
    ) -> BatchEvidenceLink | None:
        statement = select(BatchEvidenceLink).where(
            BatchEvidenceLink.batch_import_id == batch_import_id,
            BatchEvidenceLink.evidence_type == "SOURCE_WORKBOOK",
            BatchEvidenceLink.unlinked_at.is_(None),
        )
        if lock:
            statement = statement.with_for_update()
        return session.scalar(statement)

    def _recover_link_collision(
        self,
        session: Session,
        *,
        organization_id: int,
        batch_public_id: UUID,
        document_public_id: UUID,
        evidence_type: str,
    ) -> BatchEvidenceLinkResult:
        set_tenant_db_context(
            session,
            organization_id,
        )

        batch_import = self._get_import(
            session,
            organization_id=organization_id,
            public_id=batch_public_id,
        )
        document = self._get_document(
            session,
            organization_id=organization_id,
            public_id=document_public_id,
            require_available=True,
        )

        active_pair = self._active_pair(
            session,
            batch_import_id=batch_import.id,
            vault_document_id=document.id,
        )
        if active_pair is not None:
            if active_pair.evidence_type != evidence_type:
                raise BatchEvidenceConflictError(
                    "EVIDENCE_TYPE_CONFLICT",
                    (
                        "El documento ya está vinculado a la importación "
                        "con otro tipo de evidencia."
                    ),
                )

            return BatchEvidenceLinkResult(
                evidence=_view(
                    link=active_pair,
                    batch_import=batch_import,
                    document=document,
                ),
                replayed=True,
            )

        if (
            evidence_type == "SOURCE_WORKBOOK"
            and self._active_source(
                session,
                batch_import_id=batch_import.id,
            )
            is not None
        ):
            raise BatchEvidenceConflictError(
                "SOURCE_WORKBOOK_ALREADY_LINKED",
                (
                    "La importación ya posee una evidencia "
                    "SOURCE_WORKBOOK activa."
                ),
            )

        raise BatchEvidencePersistenceError(
            "No fue posible recuperar el vínculo de evidencia."
        )

    def link_evidence(
        self,
        *,
        organization_id: int | str,
        batch_import_id: UUID | str,
        vault_document_id: UUID | str,
        evidence_type: str,
        actor: AuditActor,
        request_context: AuditRequestContext | None = None,
    ) -> BatchEvidenceLinkResult:
        org_id = _normalize_organization_id(
            organization_id
        )
        self._validate_actor_scope(
            actor=actor,
            organization_id=org_id,
        )
        batch_public_id = _coerce_uuid(
            batch_import_id,
            not_found_code="BATCH_IMPORT_NOT_FOUND",
        )
        document_public_id = _coerce_uuid(
            vault_document_id,
            not_found_code="VAULT_DOCUMENT_NOT_FOUND",
        )
        normalized_type = normalize_evidence_type(
            evidence_type
        )

        session = self._session_factory()
        if session is None:
            raise BatchEvidencePersistenceError(
                "Servicio de base de datos no disponible."
            )

        try:
            set_tenant_db_context(
                session,
                org_id,
            )
            batch_import = self._get_import(
                session,
                organization_id=org_id,
                public_id=batch_public_id,
                lock=True,
            )
            document = self._get_document(
                session,
                organization_id=org_id,
                public_id=document_public_id,
                require_available=True,
                lock=True,
            )

            if normalized_type == "SOURCE_WORKBOOK":
                self._validate_source_workbook(
                    batch_import=batch_import,
                    document=document,
                )

            existing_pair = self._active_pair(
                session,
                batch_import_id=batch_import.id,
                vault_document_id=document.id,
                lock=True,
            )
            if existing_pair is not None:
                if existing_pair.evidence_type != normalized_type:
                    raise BatchEvidenceConflictError(
                        "EVIDENCE_TYPE_CONFLICT",
                        (
                            "El documento ya está vinculado a la "
                            "importación con otro tipo de evidencia."
                        ),
                    )
                return BatchEvidenceLinkResult(
                    evidence=_view(
                        link=existing_pair,
                        batch_import=batch_import,
                        document=document,
                    ),
                    replayed=True,
                )

            if (
                normalized_type == "SOURCE_WORKBOOK"
                and self._active_source(
                    session,
                    batch_import_id=batch_import.id,
                    lock=True,
                )
                is not None
            ):
                raise BatchEvidenceConflictError(
                    "SOURCE_WORKBOOK_ALREADY_LINKED",
                    (
                        "La importación ya posee una evidencia "
                        "SOURCE_WORKBOOK activa."
                    ),
                )

            link = BatchEvidenceLink(
                organization_id=org_id,
                batch_import_id=batch_import.id,
                vault_document_id=document.id,
                evidence_type=normalized_type,
                created_by_user_id=actor.user_id,
            )
            session.add(link)

            try:
                session.flush(
                    [link]
                )
            except IntegrityError as exc:
                constraint = _constraint_name(
                    exc
                )
                session.rollback()

                if constraint not in {
                    None,
                    ACTIVE_PAIR_INDEX,
                    ACTIVE_SOURCE_INDEX,
                }:
                    raise BatchEvidencePersistenceError(
                        "No fue posible persistir el vínculo de evidencia."
                    ) from exc

                return self._recover_link_collision(
                    session,
                    organization_id=org_id,
                    batch_public_id=batch_public_id,
                    document_public_id=document_public_id,
                    evidence_type=normalized_type,
                )

            evidence = _view(
                link=link,
                batch_import=batch_import,
                document=document,
            )

            record_audit_event(
                session,
                actor=actor,
                action=AuditAction.LOTE_BATCH_EVIDENCE_LINK,
                entity_type="batch_evidence_link",
                entity_id=link.id,
                outcome=AuditOutcome.SUCCESS,
                request_context=request_context,
                metadata=_audit_metadata(
                    evidence
                ),
            )

            session.commit()

            return BatchEvidenceLinkResult(
                evidence=evidence,
                replayed=False,
            )

        except BatchEvidenceError:
            session.rollback()
            raise
        except SQLAlchemyError as exc:
            session.rollback()
            raise BatchEvidencePersistenceError(
                "No fue posible persistir el vínculo de evidencia."
            ) from exc
        except Exception as exc:
            session.rollback()
            raise BatchEvidencePersistenceError(
                "No fue posible completar el vínculo de evidencia."
            ) from exc
        finally:
            session.close()

    def list_evidence(
        self,
        *,
        organization_id: int | str,
        batch_import_id: UUID | str,
    ) -> tuple[BatchEvidenceView, ...]:
        org_id = _normalize_organization_id(
            organization_id
        )
        batch_public_id = _coerce_uuid(
            batch_import_id,
            not_found_code="BATCH_IMPORT_NOT_FOUND",
        )

        session = self._session_factory()
        if session is None:
            raise BatchEvidencePersistenceError(
                "Servicio de base de datos no disponible."
            )

        try:
            set_tenant_db_context(
                session,
                org_id,
            )
            batch_import = self._get_import(
                session,
                organization_id=org_id,
                public_id=batch_public_id,
            )

            rows = session.execute(
                select(
                    BatchEvidenceLink,
                    VaultDocument,
                )
                .join(
                    VaultDocument,
                    VaultDocument.id
                    == BatchEvidenceLink.vault_document_id,
                )
                .where(
                    BatchEvidenceLink.organization_id == org_id,
                    BatchEvidenceLink.batch_import_id == batch_import.id,
                    BatchEvidenceLink.unlinked_at.is_(None),
                    VaultDocument.organization_id == org_id,
                )
                .order_by(
                    BatchEvidenceLink.created_at.asc(),
                    BatchEvidenceLink.id.asc(),
                )
            ).all()

            return tuple(
                _view(
                    link=link,
                    batch_import=batch_import,
                    document=document,
                )
                for link, document in rows
            )

        except BatchEvidenceError:
            raise
        except SQLAlchemyError as exc:
            session.rollback()
            raise BatchEvidencePersistenceError(
                "No fue posible consultar la evidencia de la importación."
            ) from exc
        finally:
            session.close()

    def unlink_evidence(
        self,
        *,
        organization_id: int | str,
        batch_import_id: UUID | str,
        vault_document_id: UUID | str,
        actor: AuditActor,
        request_context: AuditRequestContext | None = None,
    ) -> BatchEvidenceView:
        org_id = _normalize_organization_id(
            organization_id
        )
        self._validate_actor_scope(
            actor=actor,
            organization_id=org_id,
        )
        batch_public_id = _coerce_uuid(
            batch_import_id,
            not_found_code="BATCH_IMPORT_NOT_FOUND",
        )
        document_public_id = _coerce_uuid(
            vault_document_id,
            not_found_code="VAULT_DOCUMENT_NOT_FOUND",
        )

        session = self._session_factory()
        if session is None:
            raise BatchEvidencePersistenceError(
                "Servicio de base de datos no disponible."
            )

        try:
            set_tenant_db_context(
                session,
                org_id,
            )
            batch_import = self._get_import(
                session,
                organization_id=org_id,
                public_id=batch_public_id,
                lock=True,
            )
            document = self._get_document(
                session,
                organization_id=org_id,
                public_id=document_public_id,
                require_available=False,
                lock=True,
            )
            link = self._active_pair(
                session,
                batch_import_id=batch_import.id,
                vault_document_id=document.id,
                lock=True,
            )
            if link is None:
                raise BatchEvidenceNotFoundError(
                    "EVIDENCE_LINK_NOT_FOUND",
                    "Vínculo de evidencia no encontrado.",
                )

            evidence_before = _view(
                link=link,
                batch_import=batch_import,
                document=document,
            )

            link.unlinked_at = datetime.now(
                timezone.utc
            )
            link.unlinked_by_user_id = actor.user_id
            session.flush(
                [link]
            )

            record_audit_event(
                session,
                actor=actor,
                action=AuditAction.LOTE_BATCH_EVIDENCE_UNLINK,
                entity_type="batch_evidence_link",
                entity_id=link.id,
                outcome=AuditOutcome.SUCCESS,
                request_context=request_context,
                metadata=_audit_metadata(
                    evidence_before
                ),
            )

            session.commit()
            return evidence_before

        except BatchEvidenceError:
            session.rollback()
            raise
        except SQLAlchemyError as exc:
            session.rollback()
            raise BatchEvidencePersistenceError(
                "No fue posible desvincular la evidencia."
            ) from exc
        except Exception as exc:
            session.rollback()
            raise BatchEvidencePersistenceError(
                "No fue posible completar la desvinculación de evidencia."
            ) from exc
        finally:
            session.close()
