"""Assurance v1 HTTP surface: one entry point for operational documents."""
from __future__ import annotations

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    HTTPException,
    Request,
    UploadFile,
    status,
)
from fastapi.responses import JSONResponse

from litoral_trace.api.assurance_preflight import (
    assurance_preflight,
    assurance_preflight_reason_catalog,
)
from litoral_trace.api.auth import UserTenantContext
from litoral_trace.assurance.domain import DocumentProcessingStatus
from litoral_trace.assurance.feature_flags import get_assurance_feature_flags
from litoral_trace.assurance.ingestion import (
    AssuranceIngestionError,
    AssuranceIngestionService,
    AssuranceIngestionValidationError,
    validate_incoming_file,
)
from litoral_trace.assurance.processing import (
    AssuranceProcessingError,
    AssuranceProcessingService,
)
from litoral_trace.assurance.reconciliation_service import AssuranceReconciliationService
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.config import get_settings


_MAX_FILES_PER_REQUEST = 20
_READ_CHUNK_BYTES = 1024 * 1024


def _require_document_intelligence_enabled() -> None:
    flags = get_assurance_feature_flags()
    if not flags.assurance_v1 or not flags.document_intelligence:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Assurance v1 no esta habilitado para este entorno.",
        )


async def _read_bounded(upload: UploadFile) -> bytes:
    max_bytes = get_settings().storage.max_upload_bytes
    payload = bytearray()
    while True:
        chunk = await upload.read(_READ_CHUNK_BYTES)
        if not chunk:
            break
        payload.extend(chunk)
        if len(payload) > max_bytes:
            raise AssuranceIngestionValidationError(
                "El archivo excede el tamano maximo permitido."
            )
    if not payload:
        raise AssuranceIngestionValidationError("El archivo no puede estar vacio.")
    return bytes(payload)


def _serialize_ingestion(result) -> dict[str, object]:
    return {
        "assurance_document_id": str(result.assurance_public_id),
        "vault_document_id": str(result.vault_public_id),
        "filename": result.filename,
        "content_type": result.content_type,
        "size_bytes": result.size_bytes,
        "sha256": result.sha256,
        "duplicate": result.duplicate,
        "processing_status": result.processing_status,
        "progress_url": (
            f"/api/v1/assurance/documents/{result.assurance_public_id}/progress"
        ),
    }


def _process_and_reconcile(
    *,
    organization_id: int,
    assurance_public_id,
    force_reprocess: bool = False,
) -> str:
    """Process a document and reconcile linked operations after extraction.

    Reconciliation is independently feature-flagged and only consumes fields
    that the extraction pipeline marked as safe for automatic acceptance.
    """
    processing_status = AssuranceProcessingService().process(
        organization_id=organization_id,
        assurance_public_id=assurance_public_id,
        force_reprocess=force_reprocess,
    )
    flags = get_assurance_feature_flags()
    if (
        flags.assurance_v1
        and flags.reconciliation
        and processing_status
        in {
            DocumentProcessingStatus.EXTRACTED.value,
            DocumentProcessingStatus.NEEDS_REVIEW.value,
        }
    ):
        AssuranceReconciliationService().reconcile_document(
            organization_id=organization_id,
            assurance_public_id=assurance_public_id,
        )
    return processing_status


async def ingest_assurance_documents(
    background_tasks: BackgroundTasks,
    request: Request,
    files: list[UploadFile] = File(...),
    user: UserTenantContext = Depends(require_permission(Permission.VAULT_UPLOAD)),
) -> JSONResponse:
    """Accept one or many PDF/XLSX/XLS/CSV files through the same workflow."""
    del request
    _require_document_intelligence_enabled()
    if not files:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Debe adjuntarse al menos un archivo.",
        )
    if len(files) > _MAX_FILES_PER_REQUEST:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Se permiten hasta {_MAX_FILES_PER_REQUEST} archivos por solicitud.",
        )

    buffered: list[tuple[UploadFile, bytes]] = []
    try:
        for upload in files:
            content = await _read_bounded(upload)
            validate_incoming_file(
                filename=upload.filename or "document",
                content_type=upload.content_type or "",
                content=content,
            )
            buffered.append((upload, content))
    except AssuranceIngestionValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from None
    finally:
        for upload in files:
            await upload.close()

    ingestion_service = AssuranceIngestionService()
    accepted: list[dict[str, object]] = []
    duplicates = 0
    for upload, content in buffered:
        try:
            result = ingestion_service.ingest(
                organization_id=user.organization_id,
                created_by_user_id=user.user_id,
                filename=upload.filename or "document",
                content_type=upload.content_type or "",
                content=content,
            )
        except AssuranceIngestionValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(exc),
            ) from None
        except AssuranceIngestionError:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No se pudo almacenar el documento en Evidence Vault.",
            ) from None

        if result.duplicate:
            duplicates += 1
        else:
            background_tasks.add_task(
                _process_and_reconcile,
                organization_id=user.organization_id,
                assurance_public_id=result.assurance_public_id,
                force_reprocess=False,
            )
        accepted.append(_serialize_ingestion(result))

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "organization_id": user.organization_id,
            "accepted_count": len(accepted),
            "duplicate_count": duplicates,
            "documents": accepted,
        },
    )


async def assurance_document_progress(
    assurance_document_id: str,
    user: UserTenantContext = Depends(require_permission(Permission.VAULT_READ)),
) -> JSONResponse:
    _require_document_intelligence_enabled()
    try:
        progress = AssuranceProcessingService().progress(
            organization_id=user.organization_id,
            assurance_public_id=assurance_document_id,
        )
    except (
        ValueError,
        AssuranceIngestionError,
        AssuranceProcessingError,
    ):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Documento Assurance no encontrado.",
        ) from None
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="No se pudo consultar el procesamiento.",
        ) from None
    return JSONResponse(status_code=status.HTTP_200_OK, content=progress)


def build_assurance_router() -> APIRouter:
    """Create a fully populated router without sharing mutable route state."""
    api_router = APIRouter(
        prefix="/api/v1/assurance",
        tags=["Assurance v1"],
    )
    api_router.add_api_route(
        "/documents",
        ingest_assurance_documents,
        methods=["POST"],
        status_code=status.HTTP_202_ACCEPTED,
    )
    api_router.add_api_route(
        "/documents/{assurance_document_id}/progress",
        assurance_document_progress,
        methods=["GET"],
        status_code=status.HTTP_200_OK,
    )
    api_router.add_api_route(
        "/preflight",
        assurance_preflight,
        methods=["POST"],
        status_code=status.HTTP_200_OK,
    )
    api_router.add_api_route(
        "/preflight/reasons",
        assurance_preflight_reason_catalog,
        methods=["GET"],
        status_code=status.HTTP_200_OK,
    )
    return api_router


# Compatibility export for modules that historically import ``router``.
# The application entrypoint deliberately builds its own independent instance.
router = build_assurance_router()
