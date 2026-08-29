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

from litoral_trace.api.assurance_exceptions import (
    assurance_attention_queue,
    assign_assurance_exception,
    resolve_assurance_exception,
)
from litoral_trace.api.assurance_metrics import (
    assurance_metrics,
    assurance_metrics_report,
    set_assurance_metrics_baseline,
)
from litoral_trace.api.assurance_preflight import (
    assurance_preflight,
    assurance_preflight_reason_catalog,
)
from litoral_trace.api.assurance_review import (
    approve_assurance_review_fields,
    assurance_document_review,
    correct_assurance_review_fields,
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
from litoral_trace.assurance.operational_exceptions import (
    AssuranceOperationalExceptionService,
)
from litoral_trace.assurance.pipeline import mark_pipeline_completed
from litoral_trace.assurance.processing import (
    AssuranceProcessingError,
    AssuranceProcessingService,
)
from litoral_trace.assurance.reconciliation_service import AssuranceReconciliationService
from litoral_trace.assurance.suppliers import AssuranceSupplierService
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.config import get_settings
from litoral_trace.web.assurance_attention import render_assurance_attention
from litoral_trace.web.assurance_workspace import render_assurance_workspace


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


def _serialize_ingestion(
    result,
    *,
    reprocess_scheduled: bool = False,
    poll_required: bool = False,
) -> dict[str, object]:
    """Serialize reuse separately from the workspace polling contract.

    ``reused_original`` is immutable provenance: the same Vault evidence was
    found by hash. ``duplicate`` is intentionally an operational UI hint only:
    it is true exclusively when the reused document is already terminal and no
    polling is required. A document that is currently PROCESSING, or that was
    scheduled for a retry, must keep polling even though its original is reused.
    """
    reused_original = bool(result.duplicate)
    return {
        "assurance_document_id": str(result.assurance_public_id),
        "vault_document_id": str(result.vault_public_id),
        "filename": result.filename,
        "content_type": result.content_type,
        "size_bytes": result.size_bytes,
        "sha256": result.sha256,
        "duplicate": reused_original and not poll_required,
        "reused_original": reused_original,
        "reprocess_scheduled": bool(reprocess_scheduled),
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
    """Process, link supplier identity, reconcile and publish one final outcome."""
    processing_status = AssuranceProcessingService().process(
        organization_id=organization_id,
        assurance_public_id=assurance_public_id,
        force_reprocess=force_reprocess,
    )
    if processing_status == DocumentProcessingStatus.FAILED.value:
        return processing_status

    flags = get_assurance_feature_flags()
    pipeline_metadata: dict[str, object] = {
        "supplier_resolution_enabled": bool(flags.assurance_v1 and flags.document_intelligence),
        "supplier_created": False,
        "supplier_enriched": False,
        "supplier_linked": False,
        "supplier_needs_review": False,
        "supplier_resolution_reason": None,
        "supplier_public_id": None,
        "reconciliation_enabled": bool(flags.assurance_v1 and flags.reconciliation),
        "reconciliation_operation_count": 0,
        "reconciliation_finding_count": 0,
        "reconciliation_created_count": 0,
        "reconciliation_refreshed_count": 0,
        "reconciliation_reopened_count": 0,
        "reconciliation_auto_resolved_count": 0,
    }
    try:
        if (
            flags.assurance_v1
            and flags.document_intelligence
            and processing_status
            in {
                DocumentProcessingStatus.EXTRACTED.value,
                DocumentProcessingStatus.NEEDS_REVIEW.value,
            }
        ):
            supplier = AssuranceSupplierService().resolve_document(
                organization_id=organization_id,
                assurance_public_id=assurance_public_id,
            )
            pipeline_metadata.update(
                {
                    "supplier_created": supplier.created,
                    "supplier_enriched": supplier.enriched,
                    "supplier_linked": supplier.linked,
                    "supplier_needs_review": supplier.needs_review,
                    "supplier_resolution_reason": supplier.reason,
                    "supplier_public_id": (
                        str(supplier.supplier_public_id)
                        if supplier.supplier_public_id is not None
                        else None
                    ),
                }
            )

        if (
            flags.assurance_v1
            and flags.reconciliation
            and processing_status
            in {
                DocumentProcessingStatus.EXTRACTED.value,
                DocumentProcessingStatus.NEEDS_REVIEW.value,
            }
        ):
            reconciliation = AssuranceReconciliationService().reconcile_document(
                organization_id=organization_id,
                assurance_public_id=assurance_public_id,
            )
            pipeline_metadata.update(
                {
                    "reconciliation_operation_count": reconciliation.operation_count,
                    "reconciliation_finding_count": reconciliation.finding_count,
                    "reconciliation_created_count": reconciliation.created_count,
                    "reconciliation_refreshed_count": reconciliation.refreshed_count,
                    "reconciliation_reopened_count": reconciliation.reopened_count,
                    "reconciliation_auto_resolved_count": reconciliation.auto_resolved_count,
                }
            )
            if flags.operational_exceptions:
                AssuranceOperationalExceptionService().sync_reconciliation(
                    organization_id=organization_id
                )
        mark_pipeline_completed(
            organization_id=organization_id,
            assurance_public_id=assurance_public_id,
            metadata=pipeline_metadata,
        )
    except Exception as exc:
        try:
            mark_pipeline_completed(
                organization_id=organization_id,
                assurance_public_id=assurance_public_id,
                metadata={
                    **pipeline_metadata,
                    "pipeline_error_code": type(exc).__name__,
                },
            )
        except Exception:
            pass
        raise
    return processing_status


def _process_and_reconcile_safely(
    *,
    organization_id: int,
    assurance_public_id,
    force_reprocess: bool = False,
) -> str:
    """Isolate one background document so its failure cannot stop the batch."""
    try:
        return _process_and_reconcile(
            organization_id=organization_id,
            assurance_public_id=assurance_public_id,
            force_reprocess=force_reprocess,
        )
    except Exception:
        # Processing/pipeline services persist their own sanitized failure state.
        # BackgroundTasks must not receive the exception, otherwise Starlette
        # aborts the remaining queued documents in the same request.
        return DocumentProcessingStatus.FAILED.value


def _duplicate_requires_reprocess(processing_status: str) -> bool:
    """Retry duplicates that never reached a usable processing pass."""
    return str(processing_status or "").strip().upper() in {
        DocumentProcessingStatus.UPLOADED.value,
        DocumentProcessingStatus.FAILED.value,
    }


def _duplicate_processing_decision(
    *,
    organization_id: int,
    result,
) -> tuple[bool, bool]:
    """Return ``(reprocess, poll)`` for one reused Vault document.

    Active PROCESSING work is never duplicated: a repeated upload simply joins
    the existing progress stream. OCR_REQUIRED is special because it represents
    a technically incomplete pass; after the OCR runtime is fixed it is safe to
    force a new extraction over the same immutable original.
    """
    status_value = str(result.processing_status or "").strip().upper()
    if _duplicate_requires_reprocess(status_value):
        return True, True
    if status_value == DocumentProcessingStatus.PROCESSING.value:
        return False, True
    if status_value != DocumentProcessingStatus.NEEDS_REVIEW.value:
        return False, False

    try:
        progress = AssuranceProcessingService().progress(
            organization_id=int(organization_id),
            assurance_public_id=result.assurance_public_id,
        )
    except Exception:
        # Fail closed: do not create a competing processing run if we cannot
        # prove that the previous review state is the recoverable OCR case.
        return False, False

    error_code = str(progress.get("last_error_code") or "").strip().upper()
    if error_code == "OCR_REQUIRED":
        return True, True
    return False, False


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

        force_reprocess = False
        should_schedule = not result.duplicate
        poll_required = not result.duplicate
        if result.duplicate:
            duplicates += 1
            should_schedule, poll_required = _duplicate_processing_decision(
                organization_id=user.organization_id,
                result=result,
            )
            force_reprocess = should_schedule

        if should_schedule:
            background_tasks.add_task(
                _process_and_reconcile_safely,
                organization_id=user.organization_id,
                assurance_public_id=result.assurance_public_id,
                force_reprocess=force_reprocess,
            )
        accepted.append(
            _serialize_ingestion(
                result,
                reprocess_scheduled=bool(result.duplicate and should_schedule),
                poll_required=poll_required,
            )
        )

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
        "/documents/{assurance_document_id}/review",
        assurance_document_review,
        methods=["GET"],
        status_code=status.HTTP_200_OK,
    )
    api_router.add_api_route(
        "/documents/{assurance_document_id}/review/approve",
        approve_assurance_review_fields,
        methods=["POST"],
        status_code=status.HTTP_200_OK,
    )
    api_router.add_api_route(
        "/documents/{assurance_document_id}/review/correct",
        correct_assurance_review_fields,
        methods=["POST"],
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
    api_router.add_api_route(
        "/exceptions",
        assurance_attention_queue,
        methods=["GET"],
        status_code=status.HTTP_200_OK,
    )
    api_router.add_api_route(
        "/exceptions/{exception_id}/assign",
        assign_assurance_exception,
        methods=["POST"],
        status_code=status.HTTP_200_OK,
    )
    api_router.add_api_route(
        "/exceptions/{exception_id}/resolve",
        resolve_assurance_exception,
        methods=["POST"],
        status_code=status.HTTP_200_OK,
    )
    api_router.add_api_route(
        "/metrics",
        assurance_metrics,
        methods=["GET"],
        status_code=status.HTTP_200_OK,
    )
    api_router.add_api_route(
        "/metrics/baseline",
        set_assurance_metrics_baseline,
        methods=["POST"],
        status_code=status.HTTP_200_OK,
    )
    api_router.add_api_route(
        "/metrics/report",
        assurance_metrics_report,
        methods=["GET"],
        status_code=status.HTTP_200_OK,
    )
    api_router.add_api_route(
        "/workspace",
        render_assurance_workspace,
        methods=["GET"],
        include_in_schema=False,
    )
    api_router.add_api_route(
        "/attention",
        render_assurance_attention,
        methods=["GET"],
        include_in_schema=False,
    )
    return api_router


router = build_assurance_router()
