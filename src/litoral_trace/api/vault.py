"""Enterprise REST API for the private tenant-scoped Document Vault."""
from __future__ import annotations

import unicodedata
from collections.abc import Iterator
from urllib.parse import quote
from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
    UploadFile,
    status,
)
from fastapi.responses import JSONResponse, StreamingResponse

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.config import get_settings
from litoral_trace.services.audit import (
    AuditAction,
    AuditOutcome,
    build_audit_actor_from_user,
    build_request_audit_context,
    record_audit_event_now,
)
from litoral_trace.services.vault import (
    VaultConflictError,
    VaultDocumentView,
    VaultIntegrityError,
    VaultNotFoundError,
    VaultPersistenceError,
    VaultService,
    VaultStorageOperationError,
    VaultValidationError,
    VerifiedVaultDownload,
)


router = APIRouter(
    prefix="/api/v1/vault",
    tags=["Bóveda Documental B2B"],
)

_UPLOAD_READ_CHUNK_BYTES = 1024 * 1024
_DOWNLOAD_CHUNK_BYTES = 64 * 1024


def _new_vault_service() -> VaultService:
    """Factory kept patchable for focused integration/unit acceptance tests."""
    return VaultService()


def _serialize_document(
    document: VaultDocumentView,
    *,
    organization_id: int,
) -> dict[str, object]:
    public_id = str(document.public_id)
    return {
        "id": public_id,
        "public_id": public_id,
        "organization_id": int(organization_id),
        "filename": document.filename,
        "document_type": document.document_type,
        "doc_type": document.document_type,
        "content_type": document.content_type,
        "size_bytes": document.size_bytes,
        "file_size_kb": round(document.size_bytes / 1024, 1),
        "sha256": document.sha256,
        "status": document.status,
        "created_at": document.created_at.isoformat(),
        "updated_at": document.updated_at.isoformat(),
        "deleted_at": (
            document.deleted_at.isoformat()
            if document.deleted_at is not None
            else None
        ),
        "download_url": (
            f"/api/v1/vault/documents/{public_id}/download"
        ),
    }


def _safe_content_disposition(filename: str) -> str:
    """RFC 5987-compatible attachment header without CR/LF or path material."""
    normalized = unicodedata.normalize("NFKC", str(filename or "document"))
    normalized = normalized.replace("\r", "_").replace("\n", "_")
    normalized = normalized.replace("/", "_").replace("\\", "_")
    normalized = normalized.strip() or "document"

    ascii_fallback = (
        unicodedata.normalize("NFKD", normalized)
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    ascii_fallback = ascii_fallback.replace('"', "'").strip()
    ascii_fallback = ascii_fallback or "document"
    ascii_fallback = ascii_fallback[:180]

    encoded_utf8 = quote(normalized, safe="")
    return (
        f'attachment; filename="{ascii_fallback}"; '
        f"filename*=UTF-8''{encoded_utf8}"
    )


def _audit_metadata_for_document(
    document: VaultDocumentView,
) -> dict[str, object]:
    return {
        "document_public_id": str(document.public_id),
        "document_type": document.document_type,
        "content_type": document.content_type,
        "size_bytes": document.size_bytes,
        "sha256": document.sha256,
        "status": document.status,
    }


def _record_vault_audit(
    *,
    user: UserTenantContext,
    request: Request | None,
    action: AuditAction,
    outcome: AuditOutcome,
    document: VaultDocumentView | None = None,
    attempted_document_id: str | None = None,
    extra_metadata: dict[str, object] | None = None,
) -> None:
    metadata: dict[str, object] = {}
    entity_id: int | None = None

    if document is not None:
        entity_id = document.internal_id
        metadata.update(_audit_metadata_for_document(document))
    elif attempted_document_id:
        try:
            metadata["document_public_id"] = str(
                UUID(str(attempted_document_id))
            )
        except (TypeError, ValueError, AttributeError):
            pass

    if extra_metadata:
        metadata.update(extra_metadata)

    record_audit_event_now(
        actor=build_audit_actor_from_user(user),
        action=action,
        entity_type="vault_document",
        entity_id=entity_id,
        outcome=outcome,
        request_context=build_request_audit_context(request),
        metadata=metadata or None,
        best_effort=True,
    )


def _raise_http_for_vault_error(error: Exception) -> None:
    if isinstance(error, VaultValidationError):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Documento o metadata de Vault inválidos.",
        ) from None
    if isinstance(error, VaultNotFoundError):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Documento no encontrado.",
        ) from None
    if isinstance(error, VaultConflictError):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="La operación entra en conflicto con el estado del documento.",
        ) from None
    if isinstance(error, VaultIntegrityError):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Se detectó una inconsistencia de integridad en el documento.",
        ) from None
    if isinstance(error, (VaultStorageOperationError, VaultPersistenceError)):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de bóveda temporalmente no disponible.",
        ) from None

    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Error interno de bóveda.",
    ) from None


async def _read_upload_bounded(file: UploadFile) -> bytes:
    max_upload_bytes = get_settings().storage.max_upload_bytes
    payload = bytearray()

    while True:
        chunk = await file.read(_UPLOAD_READ_CHUNK_BYTES)
        if not chunk:
            break
        payload.extend(chunk)
        if len(payload) > max_upload_bytes:
            raise VaultValidationError(
                "El archivo excede el límite de tamaño permitido."
            )

    if not payload:
        raise VaultValidationError(
            "El archivo no puede estar vacío."
        )

    return bytes(payload)


def _verified_download_iterator(
    download: VerifiedVaultDownload,
) -> Iterator[bytes]:
    try:
        yield from download.iter_chunks(
            chunk_size=_DOWNLOAD_CHUNK_BYTES,
        )
    finally:
        download.close()


def _download_response(
    *,
    document_id: str,
    request: Request | None,
    user: UserTenantContext,
) -> StreamingResponse:
    service = _new_vault_service()

    try:
        verified = service.materialize_verified_download(
            organization_id=user.organization_id,
            document_id=document_id,
        )
    except VaultIntegrityError as exc:
        _record_vault_audit(
            user=user,
            request=request,
            action=AuditAction.VAULT_INTEGRITY_FAILURE,
            outcome=AuditOutcome.FAILURE,
            attempted_document_id=document_id,
            extra_metadata={"operation": "download"},
        )
        _raise_http_for_vault_error(exc)
        raise AssertionError("unreachable")
    except (
        VaultValidationError,
        VaultNotFoundError,
        VaultConflictError,
        VaultStorageOperationError,
        VaultPersistenceError,
    ) as exc:
        _raise_http_for_vault_error(exc)
        raise AssertionError("unreachable")

    _record_vault_audit(
        user=user,
        request=request,
        action=AuditAction.VAULT_DOWNLOAD,
        outcome=AuditOutcome.SUCCESS,
        document=verified.document,
    )

    return StreamingResponse(
        _verified_download_iterator(verified),
        media_type=verified.document.content_type,
        headers={
            "Content-Disposition": _safe_content_disposition(
                verified.document.filename
            ),
            "Content-Length": str(verified.document.size_bytes),
            "X-Content-Type-Options": "nosniff",
            "Cache-Control": "private, no-store",
        },
    )


@router.post(
    "/documents",
    status_code=status.HTTP_201_CREATED,
    tags=["Bóveda Documental B2B"],
)
async def subir_documento_boveda(
    file: UploadFile = File(...),
    document_type: str = Form(...),
    request: Request = None,
    idempotency_key: str | None = Header(
        default=None,
        alias="Idempotency-Key",
    ),
    user: UserTenantContext = Depends(
        require_permission(Permission.VAULT_UPLOAD)
    ),
) -> JSONResponse:
    """Validate, persist and privately store one tenant evidence document."""
    try:
        content = await _read_upload_bounded(file)
        service = _new_vault_service()
        document = service.upload_document(
            organization_id=user.organization_id,
            created_by_user_id=user.user_id,
            filename=file.filename or "document",
            document_type=document_type,
            content_type=file.content_type or "",
            content=content,
            idempotency_key=idempotency_key,
        )
    except (
        VaultValidationError,
        VaultNotFoundError,
        VaultConflictError,
        VaultIntegrityError,
        VaultStorageOperationError,
        VaultPersistenceError,
    ) as exc:
        _record_vault_audit(
            user=user,
            request=request,
            action=AuditAction.VAULT_UPLOAD,
            outcome=AuditOutcome.FAILURE,
            extra_metadata={
                "document_type": str(document_type or "")[:50],
                "failure_class": exc.__class__.__name__,
            },
        )
        _raise_http_for_vault_error(exc)
        raise AssertionError("unreachable")
    except Exception:
        _record_vault_audit(
            user=user,
            request=request,
            action=AuditAction.VAULT_UPLOAD,
            outcome=AuditOutcome.FAILURE,
            extra_metadata={
                "document_type": str(document_type or "")[:50],
                "failure_class": "unexpected_upload_error",
            },
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No se pudo leer el archivo recibido.",
        ) from None
    finally:
        await file.close()

    _record_vault_audit(
        user=user,
        request=request,
        action=AuditAction.VAULT_UPLOAD,
        outcome=AuditOutcome.SUCCESS,
        document=document,
    )

    return JSONResponse(
        status_code=status.HTTP_201_CREATED,
        content=_serialize_document(
            document,
            organization_id=user.organization_id,
        ),
    )


@router.get(
    "/documents",
    tags=["Bóveda Documental B2B"],
)
async def consultar_documentos_boveda(
    q: str | None = Query(
        None,
        description="Buscador por nombre de archivo",
    ),
    type: str | None = Query(
        None,
        description="Filtro por tipo de documento",
    ),
    user: UserTenantContext = Depends(
        require_permission(Permission.VAULT_READ)
    ),
) -> JSONResponse:
    """List persistent Vault documents visible to the authenticated tenant."""
    service = _new_vault_service()
    try:
        documents = service.list_documents(
            organization_id=user.organization_id,
            query_search=q,
            document_type=type,
        )
    except (
        VaultValidationError,
        VaultPersistenceError,
    ) as exc:
        _raise_http_for_vault_error(exc)
        raise AssertionError("unreachable")

    serialized = [
        _serialize_document(
            document,
            organization_id=user.organization_id,
        )
        for document in documents
    ]
    total_bytes = sum(document.size_bytes for document in documents)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "organization_id": user.organization_id,
            "organization_name": user.organization_name,
            "total_documents": len(serialized),
            "total_storage_bytes": total_bytes,
            "total_storage_kb": round(total_bytes / 1024, 1),
            "documents": serialized,
        },
    )


@router.get(
    "/documents/{document_id}",
    tags=["Bóveda Documental B2B"],
)
async def obtener_documento_boveda(
    document_id: str,
    user: UserTenantContext = Depends(
        require_permission(Permission.VAULT_READ)
    ),
) -> JSONResponse:
    service = _new_vault_service()
    try:
        document = service.get_document(
            organization_id=user.organization_id,
            document_id=document_id,
        )
    except (
        VaultValidationError,
        VaultNotFoundError,
        VaultPersistenceError,
    ) as exc:
        _raise_http_for_vault_error(exc)
        raise AssertionError("unreachable")

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=_serialize_document(
            document,
            organization_id=user.organization_id,
        ),
    )


@router.get(
    "/documents/{document_id}/download",
    tags=["Bóveda Documental B2B"],
)
async def descargar_documento_vault(
    document_id: str,
    request: Request = None,
    user: UserTenantContext = Depends(
        require_permission(Permission.VAULT_READ)
    ),
) -> StreamingResponse:
    return _download_response(
        document_id=document_id,
        request=request,
        user=user,
    )


@router.delete(
    "/documents/{document_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["Bóveda Documental B2B"],
)
async def eliminar_documento_boveda(
    document_id: str,
    request: Request = None,
    user: UserTenantContext = Depends(
        require_permission(Permission.VAULT_DELETE)
    ),
) -> Response:
    service = _new_vault_service()
    try:
        document = service.delete_document(
            organization_id=user.organization_id,
            document_id=document_id,
        )
    except VaultIntegrityError as exc:
        _record_vault_audit(
            user=user,
            request=request,
            action=AuditAction.VAULT_INTEGRITY_FAILURE,
            outcome=AuditOutcome.FAILURE,
            attempted_document_id=document_id,
            extra_metadata={"operation": "delete"},
        )
        _raise_http_for_vault_error(exc)
        raise AssertionError("unreachable")
    except (
        VaultValidationError,
        VaultNotFoundError,
        VaultConflictError,
        VaultStorageOperationError,
        VaultPersistenceError,
    ) as exc:
        _record_vault_audit(
            user=user,
            request=request,
            action=AuditAction.VAULT_DELETE,
            outcome=AuditOutcome.FAILURE,
            attempted_document_id=document_id,
            extra_metadata={"failure_class": exc.__class__.__name__},
        )
        _raise_http_for_vault_error(exc)
        raise AssertionError("unreachable")

    _record_vault_audit(
        user=user,
        request=request,
        action=AuditAction.VAULT_DELETE,
        outcome=AuditOutcome.SUCCESS,
        document=document,
    )

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/download/{doc_id}",
    include_in_schema=False,
    tags=["Bóveda Documental B2B"],
)
async def descargar_documento_boveda(
    doc_id: str,
    request: Request = None,
    user: UserTenantContext = Depends(
        require_permission(Permission.VAULT_READ)
    ),
) -> StreamingResponse:
    """Backward-compatible alias; no synthetic document fallback exists."""
    return _download_response(
        document_id=doc_id,
        request=request,
        user=user,
    )