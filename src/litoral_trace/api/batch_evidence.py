"""REST API for tenant-safe BatchImport <-> Vault evidence linkage."""
from __future__ import annotations

from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Request,
    Response,
    status,
)
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import (
    Permission,
    ensure_permission,
    require_permission,
)
from litoral_trace.services.audit import (
    build_audit_actor_from_user,
    build_request_audit_context,
)
from litoral_trace.services.batch_evidence import (
    BatchEvidenceAuthorizationError,
    BatchEvidenceConflictError,
    BatchEvidenceLinkResult,
    BatchEvidenceNotFoundError,
    BatchEvidencePersistenceError,
    BatchEvidenceService,
    BatchEvidenceValidationError,
    BatchEvidenceView,
)


router = APIRouter(
    prefix="/api/v1/batch",
    tags=["Procesamiento Batch"],
)


class BatchEvidenceLinkRequest(BaseModel):
    document_id: UUID
    evidence_type: str = Field(
        min_length=1,
        max_length=40,
    )


def _new_batch_evidence_service() -> BatchEvidenceService:
    return BatchEvidenceService()


def _detail(
    *,
    code: str,
    message: str,
) -> dict[str, str]:
    return {
        "code": code,
        "message": message,
    }


def _raise_http(
    error: Exception,
) -> None:
    if isinstance(
        error,
        BatchEvidenceValidationError,
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=_detail(
                code=error.code,
                message=str(error),
            ),
        ) from None

    if isinstance(
        error,
        BatchEvidenceNotFoundError,
    ):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=_detail(
                code=error.code,
                message=str(error),
            ),
        ) from None

    if isinstance(
        error,
        BatchEvidenceConflictError,
    ):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=_detail(
                code=error.code,
                message=str(error),
            ),
        ) from None

    if isinstance(
        error,
        BatchEvidenceAuthorizationError,
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=_detail(
                code="EVIDENCE_TENANT_DENIED",
                message="La operación de evidencia fue denegada.",
            ),
        ) from None

    if isinstance(
        error,
        BatchEvidencePersistenceError,
    ):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_detail(
                code="EVIDENCE_SERVICE_UNAVAILABLE",
                message=(
                    "El servicio de evidencia no está "
                    "disponible temporalmente."
                ),
            ),
        ) from None

    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=_detail(
            code="EVIDENCE_INTERNAL_ERROR",
            message="Error interno de evidencia.",
        ),
    ) from None


def _serialize_evidence(
    evidence: BatchEvidenceView,
) -> dict[str, object]:
    return {
        "link_id": str(
            evidence.link_public_id
        ),
        "organization_id": (
            evidence.organization_id
        ),
        "batch_import_id": str(
            evidence.batch_import_public_id
        ),
        "document_id": str(
            evidence.vault_document_public_id
        ),
        "evidence_type": (
            evidence.evidence_type
        ),
        "linked_at": (
            evidence.linked_at.isoformat()
        ),
        "document": {
            "id": str(
                evidence.vault_document_public_id
            ),
            "filename": (
                evidence.document_filename
            ),
            "document_type": (
                evidence.document_type
            ),
            "content_type": (
                evidence.document_content_type
            ),
            "size_bytes": (
                evidence.document_size_bytes
            ),
            "sha256": (
                evidence.document_sha256
            ),
            "status": (
                evidence.document_status
            ),
            "available": (
                evidence.document_available
            ),
        },
    }


def _ensure_read_capabilities(
    user: UserTenantContext,
) -> None:
    ensure_permission(
        user,
        Permission.VAULT_READ,
    )


def _ensure_write_capabilities(
    user: UserTenantContext,
) -> None:
    ensure_permission(
        user,
        Permission.VAULT_READ,
    )


@router.get(
    "/imports/{import_id}/evidence",
)
async def listar_evidencia_batch_endpoint(
    import_id: UUID,
    user: UserTenantContext = Depends(
        require_permission(
            Permission.LOTE_READ
        )
    ),
) -> JSONResponse:
    """
    List active evidence links for one tenant-visible completed batch import.

    Vault objects are never duplicated and storage coordinates are never
    serialized. A soft-deleted Vault document remains visible as a tombstone
    with ``available=false`` so evidence loss is explicit rather than silent.
    """
    _ensure_read_capabilities(
        user
    )
    service = _new_batch_evidence_service()

    try:
        evidence = service.list_evidence(
            organization_id=(
                user.organization_id
            ),
            batch_import_id=import_id,
        )
    except (
        BatchEvidenceAuthorizationError,
        BatchEvidenceValidationError,
        BatchEvidenceNotFoundError,
        BatchEvidenceConflictError,
        BatchEvidencePersistenceError,
    ) as exc:
        _raise_http(
            exc
        )
        raise AssertionError(
            "unreachable"
        )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "organization_id": (
                user.organization_id
            ),
            "batch_import_id": str(
                import_id
            ),
            "evidence_count": len(
                evidence
            ),
            "evidence": [
                _serialize_evidence(
                    item
                )
                for item in evidence
            ],
        },
        headers={
            "Cache-Control": "no-store",
        },
    )


@router.post(
    "/imports/{import_id}/evidence",
)
async def vincular_evidencia_batch_endpoint(
    import_id: UUID,
    payload: BatchEvidenceLinkRequest,
    request: Request = None,
    user: UserTenantContext = Depends(
        require_permission(
            Permission.LOTE_UPDATE
        )
    ),
) -> JSONResponse:
    """
    Link one existing available Vault document to a completed batch import.

    Repeating the same active relationship is idempotent and returns HTTP 200.
    Creating a new relationship returns HTTP 201.
    """
    _ensure_write_capabilities(
        user
    )
    service = _new_batch_evidence_service()

    try:
        result: BatchEvidenceLinkResult = (
            service.link_evidence(
                organization_id=(
                    user.organization_id
                ),
                batch_import_id=import_id,
                vault_document_id=(
                    payload.document_id
                ),
                evidence_type=(
                    payload.evidence_type
                ),
                actor=(
                    build_audit_actor_from_user(
                        user
                    )
                ),
                request_context=(
                    build_request_audit_context(
                        request
                    )
                ),
            )
        )
    except (
        BatchEvidenceAuthorizationError,
        BatchEvidenceValidationError,
        BatchEvidenceNotFoundError,
        BatchEvidenceConflictError,
        BatchEvidencePersistenceError,
    ) as exc:
        _raise_http(
            exc
        )
        raise AssertionError(
            "unreachable"
        )

    return JSONResponse(
        status_code=(
            status.HTTP_200_OK
            if result.replayed
            else status.HTTP_201_CREATED
        ),
        content={
            "replayed": (
                result.replayed
            ),
            "evidence": (
                _serialize_evidence(
                    result.evidence
                )
            ),
        },
        headers={
            "Cache-Control": "no-store",
        },
    )


@router.delete(
    "/imports/{import_id}/evidence/{document_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def desvincular_evidencia_batch_endpoint(
    import_id: UUID,
    document_id: UUID,
    request: Request = None,
    user: UserTenantContext = Depends(
        require_permission(
            Permission.LOTE_UPDATE
        )
    ),
) -> Response:
    """
    Mark an active evidence relationship as unlinked without deleting history.

    This never deletes the Vault document and never deletes the historical link
    row. The audit event and unlinked timestamp remain durable.
    """
    _ensure_write_capabilities(
        user
    )
    service = _new_batch_evidence_service()

    try:
        service.unlink_evidence(
            organization_id=(
                user.organization_id
            ),
            batch_import_id=import_id,
            vault_document_id=document_id,
            actor=(
                build_audit_actor_from_user(
                    user
                )
            ),
            request_context=(
                build_request_audit_context(
                    request
                )
            ),
        )
    except (
        BatchEvidenceAuthorizationError,
        BatchEvidenceValidationError,
        BatchEvidenceNotFoundError,
        BatchEvidenceConflictError,
        BatchEvidencePersistenceError,
    ) as exc:
        _raise_http(
            exc
        )
        raise AssertionError(
            "unreachable"
        )

    return Response(
        status_code=status.HTTP_204_NO_CONTENT,
        headers={
            "Cache-Control": "no-store",
        },
    )
