"""Enterprise tenant-scoped Vault service with content validation and lifecycle control."""
from __future__ import annotations

import hashlib
import io
import json
import re
import tempfile
import unicodedata
import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePath
from typing import Any, BinaryIO
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from litoral_trace.config import get_settings
from litoral_trace.config.settings import StorageSettings
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import VaultDocument
from litoral_trace.db.models.vault_document import VAULT_DOCUMENT_TYPES
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.storage import (
    Boto3S3ObjectStorage,
    ObjectHead,
    ObjectStorageClient,
    ObjectStorageConfigurationError,
    ObjectStorageError,
    ObjectStorageNotFoundError,
)


PDF_CONTENT_TYPE = "application/pdf"
JSON_CONTENT_TYPE = "application/json"
XLSX_CONTENT_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

_CONTENT_TYPE_EXTENSION = {
    PDF_CONTENT_TYPE: ".pdf",
    JSON_CONTENT_TYPE: ".json",
    XLSX_CONTENT_TYPE: ".xlsx",
}

_DOCUMENT_TYPE_CONTENT_TYPES = {
    "PDF_CERTIFICADO": frozenset({PDF_CONTENT_TYPE}),
    "DDS_JSON_TRACES": frozenset({JSON_CONTENT_TYPE}),
    "REMITO_EXCEL": frozenset({XLSX_CONTENT_TYPE}),
    "OTHER_EVIDENCE": frozenset(_CONTENT_TYPE_EXTENSION),
}

_FILENAME_FORBIDDEN_RE = re.compile(r'[<>:"/\\|?*]')
_WHITESPACE_RE = re.compile(r"\s+")
_MAX_XLSX_MEMBER_COUNT = 10_000
_MAX_XLSX_UNCOMPRESSED_BYTES = 250 * 1024 * 1024
_DOWNLOAD_SPOOL_MEMORY_BYTES = 2 * 1024 * 1024

SessionFactory = Callable[[], Session | None]


class VaultError(RuntimeError):
    """Base sanitized domain error for the enterprise Vault."""


class VaultValidationError(VaultError):
    """Client-provided document content or metadata is invalid."""


class VaultNotFoundError(VaultError):
    """Tenant-visible document does not exist."""


class VaultConflictError(VaultError):
    """Requested operation conflicts with persisted Vault state."""


class VaultPersistenceError(VaultError):
    """Vault metadata persistence failed."""


class VaultStorageOperationError(VaultError):
    """Private object-storage operation failed."""


class VaultIntegrityError(VaultError):
    """Persisted metadata and private object storage disagree."""


@dataclass(frozen=True)
class ValidatedVaultUpload:
    filename: str
    document_type: str
    content_type: str
    content: bytes
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class VaultDocumentView:
    internal_id: int
    public_id: UUID
    filename: str
    document_type: str
    content_type: str
    size_bytes: int
    sha256: str
    status: str
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None


class VerifiedVaultDownload:
    """Fully verified temporary download materialized before HTTP streaming."""

    def __init__(
        self,
        *,
        document: VaultDocumentView,
        fileobj: BinaryIO,
    ):
        self.document = document
        self._fileobj = fileobj

    def iter_chunks(
        self,
        *,
        chunk_size: int = 64 * 1024,
    ) -> Iterator[bytes]:
        if chunk_size <= 0:
            raise ValueError("chunk_size debe ser positivo.")

        while True:
            chunk = self._fileobj.read(chunk_size)
            if not chunk:
                break
            yield chunk

    def close(self) -> None:
        self._fileobj.close()

    def __enter__(self) -> "VerifiedVaultDownload":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


def _normalize_organization_id(organization_id: int | str) -> int:
    try:
        normalized = int(organization_id)
    except (TypeError, ValueError) as exc:
        raise VaultValidationError(
            "organization_id invalido."
        ) from exc

    if normalized <= 0:
        raise VaultValidationError(
            "organization_id invalido."
        )

    return normalized


def _normalize_optional_user_id(user_id: int | str | None) -> int | None:
    if user_id is None:
        return None

    try:
        normalized = int(user_id)
    except (TypeError, ValueError) as exc:
        raise VaultValidationError(
            "created_by_user_id invalido."
        ) from exc

    if normalized <= 0:
        raise VaultValidationError(
            "created_by_user_id invalido."
        )

    return normalized


def _canonical_content_type(content_type: str) -> str:
    canonical = str(content_type or "").split(";", 1)[0].strip().lower()
    if not canonical or "/" not in canonical:
        raise VaultValidationError(
            "Tipo de contenido invalido."
        )
    return canonical


def sanitize_vault_filename(filename: str) -> str:
    """Return a safe canonical display/download filename."""
    normalized = unicodedata.normalize(
        "NFKC",
        str(filename or ""),
    ).strip()

    if not normalized:
        raise VaultValidationError(
            "El nombre del archivo es obligatorio."
        )
    if any(ord(character) < 32 or ord(character) == 127 for character in normalized):
        raise VaultValidationError(
            "El nombre del archivo contiene caracteres de control."
        )
    if "/" in normalized or "\\" in normalized:
        raise VaultValidationError(
            "El nombre del archivo no puede contener rutas."
        )

    normalized = _FILENAME_FORBIDDEN_RE.sub("_", normalized)
    normalized = _WHITESPACE_RE.sub(" ", normalized).strip(" .")

    if not normalized or normalized in {".", ".."}:
        raise VaultValidationError(
            "El nombre del archivo es invalido."
        )

    suffix = PurePath(normalized).suffix
    stem = normalized[: -len(suffix)] if suffix else normalized
    max_stem_length = 255 - len(suffix)
    stem = stem[:max_stem_length].rstrip(" .")
    normalized = f"{stem}{suffix}"

    if not stem or len(normalized) > 255:
        raise VaultValidationError(
            "El nombre del archivo es invalido."
        )

    return normalized


def _normalize_document_type(document_type: str) -> str:
    normalized = str(document_type or "").strip().upper()
    if normalized not in VAULT_DOCUMENT_TYPES:
        raise VaultValidationError(
            "Tipo documental no permitido."
        )
    return normalized


def _normalize_idempotency_key(idempotency_key: str | None) -> str | None:
    if idempotency_key is None:
        return None

    normalized = str(idempotency_key).strip()
    if not normalized:
        return None
    if len(normalized) > 255:
        raise VaultValidationError(
            "Idempotency key demasiado largo."
        )
    if any(ord(character) < 32 or ord(character) == 127 for character in normalized):
        raise VaultValidationError(
            "Idempotency key invalido."
        )

    return normalized


def _validate_pdf(content: bytes) -> None:
    if not content.startswith(b"%PDF-"):
        raise VaultValidationError(
            "El contenido no corresponde a un PDF valido."
        )
    if b"%%EOF" not in content[-4096:]:
        raise VaultValidationError(
            "El PDF no contiene un cierre valido."
        )


def _validate_json(content: bytes, *, require_object: bool) -> None:
    try:
        decoded = content.decode("utf-8-sig")
        payload = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise VaultValidationError(
            "El contenido JSON es invalido."
        ) from exc

    if require_object and not isinstance(payload, dict):
        raise VaultValidationError(
            "DDS JSON debe contener un objeto JSON."
        )
    if not isinstance(payload, (dict, list)):
        raise VaultValidationError(
            "El JSON debe contener un objeto o arreglo."
        )


def _validate_xlsx(content: bytes, *, max_upload_bytes: int) -> None:
    try:
        with zipfile.ZipFile(io.BytesIO(content), "r") as workbook:
            members = workbook.infolist()
            if len(members) > _MAX_XLSX_MEMBER_COUNT:
                raise VaultValidationError(
                    "El XLSX contiene demasiadas entradas."
                )

            required_members = {
                "[Content_Types].xml",
                "_rels/.rels",
                "xl/workbook.xml",
            }
            observed_members = {member.filename for member in members}
            if not required_members.issubset(observed_members):
                raise VaultValidationError(
                    "El archivo no corresponde a un XLSX valido."
                )

            total_uncompressed = sum(member.file_size for member in members)
            allowed_uncompressed = min(
                _MAX_XLSX_UNCOMPRESSED_BYTES,
                max(max_upload_bytes * 10, 10 * 1024 * 1024),
            )
            if total_uncompressed > allowed_uncompressed:
                raise VaultValidationError(
                    "El XLSX excede el limite de expansion permitido."
                )
    except zipfile.BadZipFile as exc:
        raise VaultValidationError(
            "El contenido no corresponde a un XLSX valido."
        ) from exc


def validate_vault_upload(
    *,
    filename: str,
    document_type: str,
    content_type: str,
    content: bytes | bytearray | memoryview,
    settings: StorageSettings,
) -> ValidatedVaultUpload:
    """Validate evidence before it is persisted or sent to object storage."""
    safe_filename = sanitize_vault_filename(filename)
    normalized_document_type = _normalize_document_type(document_type)
    canonical_content_type = _canonical_content_type(content_type)
    payload = bytes(content)
    size_bytes = len(payload)

    if size_bytes <= 0:
        raise VaultValidationError(
            "No se permiten archivos vacios."
        )
    if size_bytes > settings.max_upload_bytes:
        raise VaultValidationError(
            "El archivo excede el tamano maximo permitido."
        )
    if canonical_content_type not in settings.allowed_content_types:
        raise VaultValidationError(
            "Tipo de contenido no permitido."
        )

    allowed_for_document_type = _DOCUMENT_TYPE_CONTENT_TYPES[
        normalized_document_type
    ]
    if canonical_content_type not in allowed_for_document_type:
        raise VaultValidationError(
            "El tipo documental no coincide con el contenido."
        )

    expected_extension = _CONTENT_TYPE_EXTENSION.get(canonical_content_type)
    observed_extension = PurePath(safe_filename).suffix.lower()
    if expected_extension and observed_extension != expected_extension:
        raise VaultValidationError(
            "La extension del archivo no coincide con su contenido."
        )

    if canonical_content_type == PDF_CONTENT_TYPE:
        _validate_pdf(payload)
    elif canonical_content_type == JSON_CONTENT_TYPE:
        _validate_json(
            payload,
            require_object=(normalized_document_type == "DDS_JSON_TRACES"),
        )
    elif canonical_content_type == XLSX_CONTENT_TYPE:
        _validate_xlsx(
            payload,
            max_upload_bytes=settings.max_upload_bytes,
        )
    else:
        raise VaultValidationError(
            "Tipo de contenido no soportado."
        )

    return ValidatedVaultUpload(
        filename=safe_filename,
        document_type=normalized_document_type,
        content_type=canonical_content_type,
        content=payload,
        size_bytes=size_bytes,
        sha256=hashlib.sha256(payload).hexdigest(),
    )


def _coerce_public_id(document_id: UUID | str) -> UUID:
    if isinstance(document_id, UUID):
        return document_id
    try:
        return UUID(str(document_id))
    except (TypeError, ValueError, AttributeError) as exc:
        raise VaultNotFoundError(
            "Documento no encontrado."
        ) from exc


def _document_view(document: VaultDocument) -> VaultDocumentView:
    return VaultDocumentView(
        internal_id=document.id,
        public_id=document.public_id,
        filename=document.original_filename,
        document_type=document.document_type,
        content_type=document.content_type,
        size_bytes=document.size_bytes,
        sha256=document.sha256,
        status=document.status,
        created_at=document.created_at,
        updated_at=document.updated_at,
        deleted_at=document.deleted_at,
    )


def _generic_error_message(code: str) -> str:
    messages = {
        "STORAGE_UPLOAD_FAILED": "Object storage upload failed.",
        "UPLOAD_FINALIZE_DB_FAILED_COMPENSATED": "Upload persistence failed after compensated storage write.",
        "UPLOAD_FINALIZE_DB_FAILED_CLEANUP_FAILED": "Upload persistence failed and storage compensation could not be confirmed.",
        "OBJECT_KEY_COLLISION": "Existing object did not match the expected immutable content.",
        "STORAGE_DELETE_FAILED": "Object storage delete failed.",
        "DELETE_FINALIZE_DB_FAILED": "Delete persistence failed after storage deletion.",
    }
    return messages.get(code, "Vault operation failed.")


class VaultService:
    """Tenant-scoped metadata and object-storage orchestration."""

    def __init__(
        self,
        *,
        storage_settings: StorageSettings | None = None,
        storage: ObjectStorageClient | None = None,
        session_factory: SessionFactory | None = None,
    ):
        self._storage_settings = storage_settings or get_settings().storage
        self._storage = storage
        self._session_factory = session_factory or get_db_session

    def _new_session(self, organization_id: int) -> Session:
        session = self._session_factory()
        if session is None:
            raise VaultPersistenceError(
                "No se pudo abrir una sesion de Vault."
            )
        set_tenant_db_context(session, organization_id)
        return session

    def _get_storage(self) -> ObjectStorageClient:
        if self._storage is not None:
            return self._storage
        try:
            self._storage = Boto3S3ObjectStorage(
                self._storage_settings
            )
        except ObjectStorageConfigurationError as exc:
            raise VaultStorageOperationError(
                "Object storage no esta configurado."
            ) from exc
        return self._storage

    def _build_object_key(self, organization_id: int) -> str:
        prefix = self._storage_settings.normalized_key_prefix
        return (
            f"{prefix}/tenants/{organization_id}/objects/"
            f"{uuid4().hex}"
        )

    def _ensure_storage_binding(self, document: VaultDocument) -> None:
        configured_bucket = (
            str(self._storage_settings.bucket_name or "").strip()
        )
        if (
            document.storage_backend != "s3"
            or not configured_bucket
            or document.storage_bucket != configured_bucket
        ):
            raise VaultIntegrityError(
                "La ubicacion de almacenamiento del documento no coincide con la configuracion activa."
            )

    @staticmethod
    def _upload_matches_document(
        document: VaultDocument,
        upload: ValidatedVaultUpload,
    ) -> bool:
        return (
            document.original_filename == upload.filename
            and document.document_type == upload.document_type
            and document.content_type == upload.content_type
            and document.size_bytes == upload.size_bytes
            and document.sha256 == upload.sha256
        )

    @staticmethod
    def _object_head_matches_upload(
        head: ObjectHead,
        upload: ValidatedVaultUpload,
    ) -> bool:
        metadata_sha256 = str(
            head.metadata.get("sha256", "")
        ).strip().lower()
        return (
            head.size_bytes == upload.size_bytes
            and (head.content_type or "").lower() == upload.content_type
            and metadata_sha256 == upload.sha256
        )

    @staticmethod
    def _mark_failure(
        session: Session,
        *,
        public_id: UUID,
        organization_id: int,
        status: str,
        code: str,
    ) -> None:
        try:
            session.rollback()
            set_tenant_db_context(session, organization_id)
            document = session.scalar(
                select(VaultDocument).where(
                    VaultDocument.organization_id == organization_id,
                    VaultDocument.public_id == public_id,
                )
            )
            if document is None:
                return
            document.status = status
            document.last_error_code = code
            document.last_error_message = _generic_error_message(code)
            session.commit()
        except Exception:
            try:
                session.rollback()
            except Exception:
                pass

    def upload_document(
        self,
        *,
        organization_id: int | str,
        created_by_user_id: int | str | None,
        filename: str,
        document_type: str,
        content_type: str,
        content: bytes | bytearray | memoryview,
        idempotency_key: str | None = None,
    ) -> VaultDocumentView:
        org_id = _normalize_organization_id(organization_id)
        uploader_id = _normalize_optional_user_id(created_by_user_id)
        normalized_idempotency_key = _normalize_idempotency_key(
            idempotency_key
        )
        upload = validate_vault_upload(
            filename=filename,
            document_type=document_type,
            content_type=content_type,
            content=content,
            settings=self._storage_settings,
        )

        session = self._new_session(org_id)
        public_id: UUID | None = None
        object_key: str | None = None
        resumed = False
        storage_version_id: str | None = None

        try:
            document: VaultDocument | None = None
            if normalized_idempotency_key:
                document = session.scalar(
                    select(VaultDocument).where(
                        VaultDocument.organization_id == org_id,
                        VaultDocument.idempotency_key
                        == normalized_idempotency_key,
                    )
                )

            if document is not None:
                if not self._upload_matches_document(document, upload):
                    raise VaultConflictError(
                        "La idempotency key ya pertenece a otra carga."
                    )
                if document.status == "available":
                    return _document_view(document)
                if document.status not in {
                    "pending_upload",
                    "upload_failed",
                }:
                    raise VaultConflictError(
                        "El documento no puede reutilizarse para una carga."
                    )

                self._ensure_storage_binding(document)
                document.status = "pending_upload"
                document.last_error_code = None
                document.last_error_message = None
                session.flush()
                public_id = document.public_id
                object_key = document.object_key
                storage_version_id = document.storage_version_id
                session.commit()
                resumed = True
            else:
                bucket_name = str(
                    self._storage_settings.bucket_name or ""
                ).strip()
                if not self._storage_settings.is_configured or not bucket_name:
                    raise VaultStorageOperationError(
                        "Object storage no esta configurado."
                    )

                document = VaultDocument(
                    organization_id=org_id,
                    created_by_user_id=uploader_id,
                    original_filename=upload.filename,
                    content_type=upload.content_type,
                    size_bytes=upload.size_bytes,
                    sha256=upload.sha256,
                    object_key=self._build_object_key(org_id),
                    storage_backend="s3",
                    storage_bucket=bucket_name,
                    document_type=upload.document_type,
                    status="pending_upload",
                    idempotency_key=normalized_idempotency_key,
                )
                session.add(document)
                session.flush()
                public_id = document.public_id
                object_key = document.object_key
                storage_version_id = document.storage_version_id
                session.commit()

            if public_id is None or object_key is None:
                raise VaultPersistenceError(
                    "No se pudo materializar la identidad del documento."
                )

            storage = self._get_storage()
            write_etag: str | None = None
            write_version_id: str | None = None
            object_already_present = False

            if resumed:
                try:
                    existing_head = storage.head_object(
                        key=object_key,
                        version_id=storage_version_id,
                    )
                except ObjectStorageNotFoundError:
                    existing_head = None
                except ObjectStorageError as exc:
                    raise VaultStorageOperationError(
                        "No se pudo verificar el objeto existente."
                    ) from exc

                if existing_head is not None:
                    if not self._object_head_matches_upload(
                        existing_head,
                        upload,
                    ):
                        self._mark_failure(
                            session,
                            public_id=public_id,
                            organization_id=org_id,
                            status="upload_failed",
                            code="OBJECT_KEY_COLLISION",
                        )
                        raise VaultIntegrityError(
                            "El objeto existente no coincide con la carga esperada."
                        )
                    object_already_present = True
                    write_etag = existing_head.etag
                    write_version_id = existing_head.version_id

            if not object_already_present:
                try:
                    write_result = storage.put_object(
                        key=object_key,
                        body=upload.content,
                        content_type=upload.content_type,
                        content_length=upload.size_bytes,
                        metadata={
                            "sha256": upload.sha256,
                            "public-id": str(public_id),
                            "document-type": upload.document_type,
                        },
                    )
                except ObjectStorageError as exc:
                    self._mark_failure(
                        session,
                        public_id=public_id,
                        organization_id=org_id,
                        status="upload_failed",
                        code="STORAGE_UPLOAD_FAILED",
                    )
                    raise VaultStorageOperationError(
                        "No se pudo almacenar el documento."
                    ) from exc

                write_etag = write_result.etag
                write_version_id = write_result.version_id

            storage_version_id = write_version_id

            try:
                set_tenant_db_context(session, org_id)
                document = session.scalar(
                    select(VaultDocument).where(
                        VaultDocument.organization_id == org_id,
                        VaultDocument.public_id == public_id,
                    )
                )
                if document is None:
                    raise VaultPersistenceError(
                        "La metadata del documento desaparecio durante la carga."
                    )

                document.status = "available"
                document.storage_etag = write_etag
                document.storage_version_id = write_version_id
                document.last_error_code = None
                document.last_error_message = None
                session.flush()
                session.refresh(document)
                completed_view = _document_view(document)
                session.commit()
            except Exception as exc:
                try:
                    session.rollback()
                except Exception:
                    pass

                cleanup_succeeded = False
                try:
                    storage.delete_object(
                        key=object_key,
                        version_id=storage_version_id,
                    )
                    cleanup_succeeded = True
                except ObjectStorageError:
                    cleanup_succeeded = False

                failure_code = (
                    "UPLOAD_FINALIZE_DB_FAILED_COMPENSATED"
                    if cleanup_succeeded
                    else "UPLOAD_FINALIZE_DB_FAILED_CLEANUP_FAILED"
                )
                self._mark_failure(
                    session,
                    public_id=public_id,
                    organization_id=org_id,
                    status="upload_failed",
                    code=failure_code,
                )
                raise VaultPersistenceError(
                    "No se pudo finalizar la metadata de la carga."
                ) from exc

            return completed_view

        except VaultError:
            raise
        except SQLAlchemyError as exc:
            try:
                session.rollback()
            except Exception:
                pass
            raise VaultPersistenceError(
                "No se pudo persistir la operacion de Vault."
            ) from exc
        finally:
            session.close()

    def list_documents(
        self,
        *,
        organization_id: int | str,
        query_search: str | None = None,
        document_type: str | None = None,
        include_deleted: bool = False,
    ) -> list[VaultDocumentView]:
        org_id = _normalize_organization_id(organization_id)
        normalized_type: str | None = None
        if document_type and str(document_type).strip().upper() != "TODOS":
            normalized_type = _normalize_document_type(document_type)

        session = self._new_session(org_id)
        try:
            statement = select(VaultDocument).where(
                VaultDocument.organization_id == org_id
            )
            if not include_deleted:
                statement = statement.where(
                    VaultDocument.status != "deleted"
                )
            if normalized_type:
                statement = statement.where(
                    VaultDocument.document_type == normalized_type
                )
            if query_search and str(query_search).strip():
                query = str(query_search).strip().lower()
                statement = statement.where(
                    func.lower(VaultDocument.original_filename).contains(query)
                )

            statement = statement.order_by(
                VaultDocument.created_at.desc(),
                VaultDocument.id.desc(),
            )
            documents = session.scalars(statement).all()
            return [_document_view(document) for document in documents]
        except SQLAlchemyError as exc:
            session.rollback()
            raise VaultPersistenceError(
                "No se pudo consultar la boveda."
            ) from exc
        finally:
            session.close()

    def get_document(
        self,
        *,
        organization_id: int | str,
        document_id: UUID | str,
        include_deleted: bool = False,
    ) -> VaultDocumentView:
        org_id = _normalize_organization_id(organization_id)
        public_id = _coerce_public_id(document_id)
        session = self._new_session(org_id)
        try:
            statement = select(VaultDocument).where(
                VaultDocument.organization_id == org_id,
                VaultDocument.public_id == public_id,
            )
            if not include_deleted:
                statement = statement.where(
                    VaultDocument.status != "deleted"
                )
            document = session.scalar(statement)
            if document is None:
                raise VaultNotFoundError(
                    "Documento no encontrado."
                )
            return _document_view(document)
        finally:
            session.close()

    def materialize_verified_download(
        self,
        *,
        organization_id: int | str,
        document_id: UUID | str,
    ) -> VerifiedVaultDownload:
        org_id = _normalize_organization_id(organization_id)
        public_id = _coerce_public_id(document_id)
        session = self._new_session(org_id)
        try:
            document = session.scalar(
                select(VaultDocument).where(
                    VaultDocument.organization_id == org_id,
                    VaultDocument.public_id == public_id,
                    VaultDocument.status == "available",
                )
            )
            if document is None:
                raise VaultNotFoundError(
                    "Documento no encontrado."
                )
            self._ensure_storage_binding(document)

            object_key = document.object_key
            expected_size = document.size_bytes
            expected_content_type = document.content_type
            expected_sha256 = document.sha256
            version_id = document.storage_version_id
            view = _document_view(document)
        finally:
            session.close()

        storage = self._get_storage()
        try:
            stream = storage.get_object_stream(
                key=object_key,
                version_id=version_id,
            )
        except ObjectStorageNotFoundError as exc:
            raise VaultIntegrityError(
                "El objeto privado del documento no existe."
            ) from exc
        except ObjectStorageError as exc:
            raise VaultStorageOperationError(
                "No se pudo obtener el documento privado."
            ) from exc

        temporary_file = tempfile.SpooledTemporaryFile(
            max_size=_DOWNLOAD_SPOOL_MEMORY_BYTES,
            mode="w+b",
        )
        digest = hashlib.sha256()
        observed_size = 0

        try:
            if stream.head.size_bytes != expected_size:
                raise VaultIntegrityError(
                    "El tamano almacenado no coincide con la metadata."
                )
            if (
                stream.head.content_type
                and stream.head.content_type.lower() != expected_content_type
            ):
                raise VaultIntegrityError(
                    "El tipo de contenido almacenado no coincide con la metadata."
                )
            metadata_sha256 = str(
                stream.head.metadata.get("sha256", "")
            ).strip().lower()
            if metadata_sha256 and metadata_sha256 != expected_sha256:
                raise VaultIntegrityError(
                    "El hash declarado por storage no coincide con la metadata."
                )

            for chunk in stream.iter_chunks():
                observed_size += len(chunk)
                if observed_size > expected_size:
                    raise VaultIntegrityError(
                        "El objeto almacenado excede el tamano esperado."
                    )
                digest.update(chunk)
                temporary_file.write(chunk)

            if observed_size != expected_size:
                raise VaultIntegrityError(
                    "El objeto almacenado tiene un tamano incompleto."
                )
            if digest.hexdigest() != expected_sha256:
                raise VaultIntegrityError(
                    "La integridad SHA-256 del documento no coincide."
                )

            temporary_file.seek(0)
            return VerifiedVaultDownload(
                document=view,
                fileobj=temporary_file,
            )
        except Exception:
            temporary_file.close()
            raise
        finally:
            stream.close()

    def delete_document(
        self,
        *,
        organization_id: int | str,
        document_id: UUID | str,
    ) -> VaultDocumentView:
        org_id = _normalize_organization_id(organization_id)
        public_id = _coerce_public_id(document_id)
        session = self._new_session(org_id)

        try:
            document = session.scalar(
                select(VaultDocument).where(
                    VaultDocument.organization_id == org_id,
                    VaultDocument.public_id == public_id,
                )
            )
            if document is None:
                raise VaultNotFoundError(
                    "Documento no encontrado."
                )
            if document.status == "deleted":
                return _document_view(document)
            if document.status not in {
                "available",
                "delete_pending",
                "delete_failed",
            }:
                raise VaultConflictError(
                    "El documento no se encuentra en un estado eliminable."
                )

            self._ensure_storage_binding(document)
            object_key = document.object_key
            version_id = document.storage_version_id
            document.status = "delete_pending"
            document.last_error_code = None
            document.last_error_message = None
            session.flush()
            session.commit()

            storage = self._get_storage()

            try:
                storage.delete_object(
                    key=object_key,
                    version_id=version_id,
                )
            except ObjectStorageNotFoundError:
                pass
            except ObjectStorageError as exc:
                self._mark_failure(
                    session,
                    public_id=public_id,
                    organization_id=org_id,
                    status="delete_failed",
                    code="STORAGE_DELETE_FAILED",
                )
                raise VaultStorageOperationError(
                    "No se pudo eliminar el objeto privado."
                ) from exc

            try:
                set_tenant_db_context(session, org_id)
                document = session.scalar(
                    select(VaultDocument).where(
                        VaultDocument.organization_id == org_id,
                        VaultDocument.public_id == public_id,
                    )
                )
                if document is None:
                    raise VaultPersistenceError(
                        "La metadata del documento desaparecio durante la eliminacion."
                    )
                document.status = "deleted"
                document.deleted_at = datetime.now(timezone.utc)
                document.last_error_code = None
                document.last_error_message = None
                session.flush()
                session.refresh(document)
                deleted_view = _document_view(document)
                session.commit()
            except Exception as exc:
                self._mark_failure(
                    session,
                    public_id=public_id,
                    organization_id=org_id,
                    status="delete_failed",
                    code="DELETE_FINALIZE_DB_FAILED",
                )
                raise VaultPersistenceError(
                    "El objeto fue eliminado pero no se pudo finalizar su metadata."
                ) from exc

            return deleted_view

        except VaultError:
            raise
        except SQLAlchemyError as exc:
            session.rollback()
            raise VaultPersistenceError(
                "No se pudo persistir la eliminacion del Vault."
            ) from exc
        finally:
            session.close()


def _legacy_document_dict(document: VaultDocumentView) -> dict[str, Any]:
    """Temporary compatibility payload until the P2.3D API contract replaces it."""
    return {
        "id": str(document.public_id),
        "organization_id": None,
        "filename": document.filename,
        "doc_type": document.document_type,
        "commodity": "",
        "parcel_name": "",
        "provider_tax_id": "",
        "file_size_kb": round(document.size_bytes / 1024, 1),
        "status": document.status.upper(),
        "created_at": document.created_at.astimezone(timezone.utc).strftime(
            "%Y-%m-%d %H:%M"
        ),
        "download_url": (
            f"/api/v1/vault/download/{document.public_id}"
        ),
    }


def listar_documentos_boveda_tenant(
    organization_id: int,
    query_search: str | None = None,
    doc_type_filter: str | None = None,
) -> list[dict[str, Any]]:
    """Compatibility facade backed by persistent tenant metadata, never demo data."""
    org_id = _normalize_organization_id(organization_id)
    service = VaultService()
    documents = service.list_documents(
        organization_id=org_id,
        query_search=query_search,
        document_type=doc_type_filter,
    )
    payload = []
    for document in documents:
        item = _legacy_document_dict(document)
        item["organization_id"] = org_id
        payload.append(item)
    return payload