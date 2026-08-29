"""Universal Vault-first ingestion for Assurance v1.

The original bytes always land in the existing tenant-scoped Evidence Vault
metadata/storage boundary before extraction starts. SHA-256 is the primary
content identity, so identical files are reused rather than silently reprocessed.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import PurePath
from typing import Callable
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from litoral_trace.assurance.domain import AssuranceDocumentType, DocumentProcessingStatus
from litoral_trace.assurance.parsers import (
    DocumentParseError,
    parse_csv,
    validate_xls_bytes,
    validate_xlsx_bytes,
)
from litoral_trace.config import get_settings
from litoral_trace.config.settings import StorageSettings
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import AssuranceDocument, VaultDocument
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.vault import sanitize_vault_filename
from litoral_trace.storage import (
    Boto3S3ObjectStorage,
    ObjectStorageClient,
    ObjectStorageConfigurationError,
    ObjectStorageError,
)


SUPPORTED_EXTENSIONS = frozenset({".pdf", ".xlsx", ".xls", ".csv"})
CANONICAL_MIME_BY_EXTENSION = {
    ".pdf": "application/pdf",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".xls": "application/vnd.ms-excel",
    ".csv": "text/csv",
}
_ACCEPTED_MIMES_BY_EXTENSION = {
    ".pdf": frozenset({"application/pdf", "application/octet-stream"}),
    ".xlsx": frozenset(
        {
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/octet-stream",
            "application/zip",
        }
    ),
    ".xls": frozenset({"application/vnd.ms-excel", "application/octet-stream"}),
    ".csv": frozenset(
        {
            "text/csv",
            "application/csv",
            "text/plain",
            "application/vnd.ms-excel",
            "application/octet-stream",
        }
    ),
}

SessionFactory = Callable[[], Session | None]


class AssuranceIngestionError(RuntimeError):
    """Sanitized ingestion-domain failure."""


class AssuranceIngestionValidationError(AssuranceIngestionError):
    pass


class AssuranceIngestionStorageError(AssuranceIngestionError):
    pass


class AssuranceIngestionPersistenceError(AssuranceIngestionError):
    pass


@dataclass(frozen=True, slots=True)
class ValidatedIncomingFile:
    filename: str
    extension: str
    content_type: str
    content: bytes
    size_bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class IngestionResult:
    vault_document_id: int
    vault_public_id: UUID
    assurance_document_id: int
    assurance_public_id: UUID
    filename: str
    content_type: str
    size_bytes: int
    sha256: str
    duplicate: bool
    processing_status: str


def _canonical_mime(content_type: str, extension: str) -> str:
    supplied = str(content_type or "").split(";", 1)[0].strip().lower()
    if supplied and supplied not in _ACCEPTED_MIMES_BY_EXTENSION[extension]:
        raise AssuranceIngestionValidationError(
            "El tipo MIME no coincide con la extension del archivo."
        )
    return CANONICAL_MIME_BY_EXTENSION[extension]


def _validate_pdf(content: bytes) -> None:
    if not content.startswith(b"%PDF-"):
        raise AssuranceIngestionValidationError("El archivo no corresponde a un PDF valido.")
    if b"%%EOF" not in content[-4096:]:
        raise AssuranceIngestionValidationError("El PDF no contiene un cierre valido.")


def validate_incoming_file(
    *,
    filename: str,
    content_type: str,
    content: bytes | bytearray | memoryview,
    storage_settings: StorageSettings | None = None,
) -> ValidatedIncomingFile:
    settings = storage_settings or get_settings().storage
    safe_filename = sanitize_vault_filename(filename)
    extension = PurePath(safe_filename).suffix.lower()
    if extension not in SUPPORTED_EXTENSIONS:
        raise AssuranceIngestionValidationError(
            "Formato no soportado. Se acepta PDF, XLSX, XLS o CSV."
        )

    payload = bytes(content)
    if not payload:
        raise AssuranceIngestionValidationError("El archivo no puede estar vacio.")
    if len(payload) > settings.max_upload_bytes:
        raise AssuranceIngestionValidationError("El archivo excede el tamano maximo permitido.")

    canonical_mime = _canonical_mime(content_type, extension)
    try:
        if extension == ".pdf":
            _validate_pdf(payload)
        elif extension == ".xlsx":
            validate_xlsx_bytes(payload)
        elif extension == ".xls":
            validate_xls_bytes(payload)
        elif extension == ".csv":
            parsed = parse_csv(payload)
            if not parsed.tables or not parsed.tables[0].headers:
                raise AssuranceIngestionValidationError("El CSV no contiene datos tabulares utiles.")
    except DocumentParseError as exc:
        raise AssuranceIngestionValidationError(str(exc)) from exc

    return ValidatedIncomingFile(
        filename=safe_filename,
        extension=extension,
        content_type=canonical_mime,
        content=payload,
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )


class AssuranceIngestionService:
    """Store original evidence, deduplicate by hash, then expose processing state."""

    def __init__(
        self,
        *,
        storage_settings: StorageSettings | None = None,
        storage: ObjectStorageClient | None = None,
        session_factory: SessionFactory | None = None,
    ) -> None:
        self._storage_settings = storage_settings or get_settings().storage
        self._storage = storage
        self._session_factory = session_factory or get_db_session

    def _new_session(self, organization_id: int) -> Session:
        session = self._session_factory()
        if session is None:
            raise AssuranceIngestionPersistenceError("No se pudo abrir una sesion de base de datos.")
        set_tenant_db_context(session, organization_id)
        return session

    def _get_storage(self) -> ObjectStorageClient:
        if self._storage is not None:
            return self._storage
        try:
            self._storage = Boto3S3ObjectStorage(self._storage_settings)
        except ObjectStorageConfigurationError as exc:
            raise AssuranceIngestionStorageError("Evidence Vault no esta configurado.") from exc
        return self._storage

    def _cleanup_uploaded_object(
        self,
        *,
        object_key: str | None,
        storage_version_id: str | None,
        storage_written: bool,
    ) -> bool:
        if not object_key or not storage_written:
            return True
        try:
            self._get_storage().delete_object(
                key=object_key,
                version_id=storage_version_id,
            )
            return True
        except Exception:
            return False

    def _mark_persistence_failure(
        self,
        session: Session,
        *,
        organization_id: int,
        vault_public_id: UUID | None,
        cleanup_succeeded: bool,
    ) -> None:
        if vault_public_id is None:
            return
        try:
            session.rollback()
            set_tenant_db_context(session, organization_id)
            persisted = session.scalar(
                select(VaultDocument).where(
                    VaultDocument.organization_id == organization_id,
                    VaultDocument.public_id == vault_public_id,
                )
            )
            if persisted is None:
                return
            persisted.status = "upload_failed"
            persisted.last_error_code = (
                "ASSURANCE_FINALIZE_FAILED_COMPENSATED"
                if cleanup_succeeded
                else "ASSURANCE_FINALIZE_FAILED_CLEANUP_FAILED"
            )
            persisted.last_error_message = "Assurance upload finalization failed."
            session.commit()
        except Exception:
            try:
                session.rollback()
            except Exception:
                pass

    def _ensure_assurance_document(
        self,
        session: Session,
        *,
        organization_id: int,
        vault_document: VaultDocument,
    ) -> AssuranceDocument:
        assurance_document = session.scalar(
            select(AssuranceDocument).where(
                AssuranceDocument.organization_id == organization_id,
                AssuranceDocument.vault_document_id == vault_document.id,
            )
        )
        if assurance_document is None:
            semantic_type = (
                AssuranceDocumentType.SPREADSHEET.value
                if PurePath(vault_document.original_filename).suffix.lower() in {".xlsx", ".xls", ".csv"}
                else AssuranceDocumentType.UNKNOWN.value
            )
            assurance_document = AssuranceDocument(
                organization_id=organization_id,
                vault_document_id=vault_document.id,
                semantic_document_type=semantic_type,
                type_confidence=1.0 if semantic_type == AssuranceDocumentType.SPREADSHEET.value else 0.0,
                processing_status=DocumentProcessingStatus.UPLOADED.value,
                source_system="ASSURANCE_UNIVERSAL_UPLOAD",
            )
            session.add(assurance_document)
            session.flush()
        return assurance_document

    @staticmethod
    def _result(
        *,
        vault_document: VaultDocument,
        assurance_document: AssuranceDocument,
        duplicate: bool,
    ) -> IngestionResult:
        return IngestionResult(
            vault_document_id=vault_document.id,
            vault_public_id=vault_document.public_id,
            assurance_document_id=assurance_document.id,
            assurance_public_id=assurance_document.public_id,
            filename=vault_document.original_filename,
            content_type=vault_document.content_type,
            size_bytes=vault_document.size_bytes,
            sha256=vault_document.sha256,
            duplicate=duplicate,
            processing_status=assurance_document.processing_status,
        )

    def ingest(
        self,
        *,
        organization_id: int,
        created_by_user_id: int | None,
        filename: str,
        content_type: str,
        content: bytes,
    ) -> IngestionResult:
        if int(organization_id) <= 0:
            raise AssuranceIngestionValidationError("organization_id invalido.")
        validated = validate_incoming_file(
            filename=filename,
            content_type=content_type,
            content=content,
            storage_settings=self._storage_settings,
        )

        org_id = int(organization_id)
        session = self._new_session(org_id)
        object_key: str | None = None
        storage_version_id: str | None = None
        storage_written = False
        vault_public_id: UUID | None = None
        try:
            duplicate = session.scalar(
                select(VaultDocument)
                .where(
                    VaultDocument.organization_id == org_id,
                    VaultDocument.sha256 == validated.sha256,
                    VaultDocument.size_bytes == validated.size_bytes,
                    VaultDocument.status != "deleted",
                )
                .order_by(VaultDocument.id.asc())
            )
            if duplicate is not None:
                assurance_document = self._ensure_assurance_document(
                    session,
                    organization_id=org_id,
                    vault_document=duplicate,
                )
                session.commit()
                return self._result(
                    vault_document=duplicate,
                    assurance_document=assurance_document,
                    duplicate=True,
                )

            bucket = str(self._storage_settings.bucket_name or "").strip()
            if not self._storage_settings.is_configured or not bucket:
                raise AssuranceIngestionStorageError("Evidence Vault no esta configurado.")

            object_key = (
                f"{self._storage_settings.normalized_key_prefix}/tenants/"
                f"{org_id}/objects/{uuid4().hex}"
            )
            vault_document = VaultDocument(
                organization_id=org_id,
                created_by_user_id=created_by_user_id,
                original_filename=validated.filename,
                content_type=validated.content_type,
                size_bytes=validated.size_bytes,
                sha256=validated.sha256,
                object_key=object_key,
                storage_backend="s3",
                storage_bucket=bucket,
                document_type="OTHER_EVIDENCE",
                status="pending_upload",
                idempotency_key=f"assurance-sha256:{validated.sha256}",
            )
            session.add(vault_document)
            session.flush()
            vault_public_id = vault_document.public_id
            session.commit()

            storage = self._get_storage()
            try:
                write = storage.put_object(
                    key=object_key,
                    body=validated.content,
                    content_type=validated.content_type,
                    content_length=validated.size_bytes,
                    metadata={
                        "sha256": validated.sha256,
                        "public-id": str(vault_public_id),
                        "document-type": "OTHER_EVIDENCE",
                        "ingestion": "assurance-v1",
                    },
                )
                storage_written = True
                storage_version_id = write.version_id
            except ObjectStorageError as exc:
                set_tenant_db_context(session, org_id)
                persisted = session.scalar(
                    select(VaultDocument).where(
                        VaultDocument.organization_id == org_id,
                        VaultDocument.public_id == vault_public_id,
                    )
                )
                if persisted is not None:
                    persisted.status = "upload_failed"
                    persisted.last_error_code = "ASSURANCE_STORAGE_UPLOAD_FAILED"
                    persisted.last_error_message = "Object storage upload failed."
                    session.commit()
                raise AssuranceIngestionStorageError("No se pudo almacenar el archivo original.") from exc

            set_tenant_db_context(session, org_id)
            vault_document = session.scalar(
                select(VaultDocument).where(
                    VaultDocument.organization_id == org_id,
                    VaultDocument.public_id == vault_public_id,
                )
            )
            if vault_document is None:
                raise AssuranceIngestionPersistenceError("La metadata del archivo desaparecio.")
            vault_document.status = "available"
            vault_document.storage_etag = write.etag
            vault_document.storage_version_id = write.version_id
            vault_document.last_error_code = None
            vault_document.last_error_message = None
            assurance_document = self._ensure_assurance_document(
                session,
                organization_id=org_id,
                vault_document=vault_document,
            )
            session.commit()
            return self._result(
                vault_document=vault_document,
                assurance_document=assurance_document,
                duplicate=False,
            )
        except AssuranceIngestionStorageError:
            raise
        except AssuranceIngestionValidationError:
            raise
        except (AssuranceIngestionPersistenceError, SQLAlchemyError) as exc:
            try:
                session.rollback()
            except Exception:
                pass
            cleanup_succeeded = self._cleanup_uploaded_object(
                object_key=object_key,
                storage_version_id=storage_version_id,
                storage_written=storage_written,
            )
            self._mark_persistence_failure(
                session,
                organization_id=org_id,
                vault_public_id=vault_public_id,
                cleanup_succeeded=cleanup_succeeded,
            )
            if isinstance(exc, AssuranceIngestionPersistenceError):
                raise
            raise AssuranceIngestionPersistenceError("No se pudo persistir la carga Assurance.") from exc
        except Exception as exc:
            try:
                session.rollback()
            except Exception:
                pass
            cleanup_succeeded = self._cleanup_uploaded_object(
                object_key=object_key,
                storage_version_id=storage_version_id,
                storage_written=storage_written,
            )
            self._mark_persistence_failure(
                session,
                organization_id=org_id,
                vault_public_id=vault_public_id,
                cleanup_succeeded=cleanup_succeeded,
            )
            raise AssuranceIngestionPersistenceError("No se pudo finalizar la carga Assurance.") from exc
        finally:
            session.close()

    def get_status(
        self,
        *,
        organization_id: int,
        assurance_public_id: UUID | str,
    ) -> IngestionResult:
        try:
            public_id = (
                assurance_public_id
                if isinstance(assurance_public_id, UUID)
                else UUID(str(assurance_public_id))
            )
        except (ValueError, TypeError, AttributeError) as exc:
            raise AssuranceIngestionValidationError("Identificador de documento invalido.") from exc

        session = self._new_session(int(organization_id))
        try:
            assurance_document = session.scalar(
                select(AssuranceDocument).where(
                    AssuranceDocument.organization_id == int(organization_id),
                    AssuranceDocument.public_id == public_id,
                )
            )
            if assurance_document is None:
                raise AssuranceIngestionValidationError("Documento Assurance no encontrado.")
            vault_document = session.scalar(
                select(VaultDocument).where(
                    VaultDocument.organization_id == int(organization_id),
                    VaultDocument.id == assurance_document.vault_document_id,
                )
            )
            if vault_document is None:
                raise AssuranceIngestionPersistenceError("Evidence Vault inconsistente.")
            return self._result(
                vault_document=vault_document,
                assurance_document=assurance_document,
                duplicate=False,
            )
        finally:
            session.close()
