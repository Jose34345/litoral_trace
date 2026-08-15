"""Enterprise XLSX batch API contract for Litoral Trace."""

from __future__ import annotations

import io
from typing import Any
from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
    File,
    Header,
    HTTPException,
    Request,
    UploadFile,
    status,
)
from fastapi.responses import JSONResponse, StreamingResponse

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.services.audit import (
    build_audit_actor_from_user,
    build_request_audit_context,
)
from litoral_trace.services.batch import (
    BATCH_MAX_FILE_BYTES,
    BATCH_XLSX_MEDIA_TYPE,
    BatchExcelValidationError,
    BatchSemanticValidationError,
    BatchValidationResult,
    BatchWorkbook,
    generar_plantilla_excel,
    parsear_excel_lotes,
    validar_filas_lotes,
)
from litoral_trace.services.batch_imports import (
    BatchImportConflictError,
    BatchImportIdempotencyConflictError,
    BatchImportPersistenceError,
    BatchImportResult,
    BatchImportService,
    normalize_idempotency_key,
)
from litoral_trace.services.batch_queries import (
    BatchImportQueryError,
    BatchImportQueryService,
    BatchImportSnapshot,
)


router = APIRouter(
    prefix="/batch",
    tags=["Procesamiento Batch"],
)

BATCH_HTTP_MULTIPART_OVERHEAD_BYTES = 1024 * 1024
BATCH_HTTP_MAX_REQUEST_BYTES = (
    BATCH_MAX_FILE_BYTES
    + BATCH_HTTP_MULTIPART_OVERHEAD_BYTES
)

_SIZE_ERROR_CODES = frozenset(
    {
        "FILE_TOO_LARGE",
        "XLSX_MEMBER_TOO_LARGE",
        "XLSX_EXPANDED_TOO_LARGE",
    }
)

_SCHEMA_ERROR_CODES = frozenset(
    {
        "TOO_MANY_SHEETS",
        "MISSING_REQUIRED_SHEET",
        "TOO_MANY_COLUMNS",
        "TOO_MANY_ROWS",
        "MISSING_HEADER",
        "FORMULA_NOT_ALLOWED",
        "DUPLICATE_HEADERS",
        "INVALID_HEADERS",
        "CELL_ERROR",
        "NO_DATA_ROWS",
    }
)


def _new_batch_import_service() -> BatchImportService:
    return BatchImportService()


def _new_batch_import_query_service() -> BatchImportQueryService:
    return BatchImportQueryService()


def _raise_api_error(
    *,
    status_code: int,
    code: str,
    message: str,
    extra: dict[str, Any] | None = None,
) -> None:
    detail: dict[str, Any] = {
        "code": code,
        "message": message,
    }

    if extra:
        detail.update(extra)

    raise HTTPException(
        status_code=status_code,
        detail=detail,
    )


def _map_structural_error(
    exc: BatchExcelValidationError,
) -> None:
    if exc.code in _SIZE_ERROR_CODES:
        status_code = status.HTTP_413_CONTENT_TOO_LARGE
    elif exc.code in _SCHEMA_ERROR_CODES:
        status_code = status.HTTP_422_UNPROCESSABLE_CONTENT
    else:
        status_code = status.HTTP_400_BAD_REQUEST

    _raise_api_error(
        status_code=status_code,
        code=exc.code,
        message=exc.detail,
    )


def _serialize_validation(
    *,
    workbook: BatchWorkbook,
    result: BatchValidationResult,
    organization_id: int,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []

    for row_result in result.rows:
        data = None

        if row_result.data is not None:
            data = row_result.data.as_lote_payload()

        rows.append(
            {
                "row": row_result.row,
                "valid": row_result.valid,
                "data": data,
                "errors": [
                    {
                        "field": error.field,
                        "code": error.code,
                        "message": error.message,
                    }
                    for error in row_result.errors
                ],
            }
        )

    return {
        "organization_id": organization_id,
        "filename": workbook.filename,
        "sha256": workbook.sha256,
        "sheet_name": workbook.sheet_name,
        "valid": result.valid,
        "total_rows": result.total_rows,
        "valid_rows": result.valid_rows,
        "invalid_rows": result.invalid_rows,
        "rows": rows,
    }


def _serialize_import_result(
    result: BatchImportResult,
) -> dict[str, Any]:
    if result.import_public_id is None:
        raise BatchImportPersistenceError(
            "La importación no produjo identidad persistente."
        )

    return {
        "organization_id": result.organization_id,
        "import_id": str(result.import_public_id),
        "status": "completed",
        "replayed": result.replayed,
        "total_rows": result.total_rows,
        "inserted_rows": result.inserted_rows,
        "lote_ids": list(result.lote_ids),
        "identifiers": list(result.identifiers),
    }


def _serialize_import_snapshot(
    snapshot: BatchImportSnapshot,
) -> dict[str, Any]:
    return {
        "organization_id": snapshot.organization_id,
        "import_id": str(snapshot.public_id),
        "status": snapshot.status,
        "source_filename": snapshot.source_filename,
        "source_sha256": snapshot.source_sha256,
        "total_rows": snapshot.total_rows,
        "inserted_rows": snapshot.inserted_rows,
        "lote_ids": list(snapshot.lote_ids),
        "identifiers": list(snapshot.identifiers),
        "created_at": snapshot.created_at.isoformat(),
        "completed_at": (
            snapshot.completed_at.isoformat()
            if snapshot.completed_at is not None
            else None
        ),
    }


async def _read_bounded_upload(
    file: UploadFile,
    *,
    request: Request | None,
) -> tuple[bytes, str]:
    filename = file.filename or ""

    if request is not None:
        raw_content_length = (
            request.headers.get("content-length")
            or ""
        ).strip()

        if raw_content_length:
            try:
                content_length = int(
                    raw_content_length
                )
            except ValueError:
                content_length = None

            if (
                content_length is not None
                and content_length
                > BATCH_HTTP_MAX_REQUEST_BYTES
            ):
                await file.close()
                _raise_api_error(
                    status_code=(
                        status.HTTP_413_CONTENT_TOO_LARGE
                    ),
                    code="REQUEST_TOO_LARGE",
                    message=(
                        "La solicitud excede el tamaño "
                        "máximo permitido."
                    ),
                )

    try:
        payload = await file.read(
            BATCH_MAX_FILE_BYTES + 1
        )
    finally:
        await file.close()

    if len(payload) > BATCH_MAX_FILE_BYTES:
        _raise_api_error(
            status_code=(
                status.HTTP_413_CONTENT_TOO_LARGE
            ),
            code="FILE_TOO_LARGE",
            message=(
                "El archivo Excel excede el tamaño "
                "máximo permitido."
            ),
        )

    if not payload:
        _raise_api_error(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="EMPTY_FILE",
            message="El archivo Excel está vacío.",
        )

    return payload, filename


async def _parse_upload(
    file: UploadFile,
    *,
    request: Request | None,
) -> BatchWorkbook:
    payload, filename = await _read_bounded_upload(
        file,
        request=request,
    )

    try:
        return parsear_excel_lotes(
            payload,
            filename=filename,
        )
    except BatchExcelValidationError as exc:
        _map_structural_error(exc)

    raise AssertionError(
        "Unreachable structural parsing state."
    )


@router.get(
    "/template",
)
async def descargar_plantilla_excel_endpoint(
    user: UserTenantContext = Depends(
        require_permission(Permission.LOTE_READ)
    ),
) -> StreamingResponse:
    """Download the only supported XLSX import schema."""

    del user

    template_bytes = generar_plantilla_excel()

    return StreamingResponse(
        io.BytesIO(template_bytes),
        media_type=BATCH_XLSX_MEDIA_TYPE,
        headers={
            "Content-Disposition": (
                "attachment; "
                "filename=LitoralTrace_Plantilla_Ingreso.xlsx"
            ),
            "Cache-Control": "no-store",
        },
    )


@router.post(
    "/validate",
)
async def validar_batch_excel_endpoint(
    file: UploadFile = File(...),
    request: Request = None,
    user: UserTenantContext = Depends(
        require_permission(Permission.LOTE_CREATE)
    ),
) -> JSONResponse:
    """
    Parse and validate a workbook without persisting lotes or import identity.
    """

    workbook = await _parse_upload(
        file,
        request=request,
    )
    result = validar_filas_lotes(
        workbook
    )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=_serialize_validation(
            workbook=workbook,
            result=result,
            organization_id=user.organization_id,
        ),
        headers={
            "Cache-Control": "no-store",
        },
    )


@router.post(
    "/import",
)
async def importar_batch_excel_endpoint(
    file: UploadFile = File(...),
    request: Request = None,
    idempotency_key: str | None = Header(
        default=None,
        alias="Idempotency-Key",
    ),
    user: UserTenantContext = Depends(
        require_permission(Permission.LOTE_CREATE)
    ),
) -> JSONResponse:
    """
    Atomically import a validated workbook with persistent idempotency.
    """

    if idempotency_key is None:
        _raise_api_error(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="MISSING_IDEMPOTENCY_KEY",
            message=(
                "La cabecera Idempotency-Key "
                "es obligatoria."
            ),
        )

    try:
        normalized_key = normalize_idempotency_key(
            idempotency_key
        )
    except BatchImportIdempotencyConflictError:
        _raise_api_error(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="INVALID_IDEMPOTENCY_KEY",
            message=(
                "La cabecera Idempotency-Key "
                "no es válida."
            ),
        )

    if normalized_key is None:
        _raise_api_error(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="MISSING_IDEMPOTENCY_KEY",
            message=(
                "La cabecera Idempotency-Key "
                "es obligatoria."
            ),
        )

    workbook = await _parse_upload(
        file,
        request=request,
    )
    validation = validar_filas_lotes(
        workbook
    )

    if not validation.valid:
        preview = _serialize_validation(
            workbook=workbook,
            result=validation,
            organization_id=user.organization_id,
        )
        _raise_api_error(
            status_code=(
                status.HTTP_422_UNPROCESSABLE_CONTENT
            ),
            code="ROW_VALIDATION_FAILED",
            message=(
                "La planilla contiene filas "
                "con errores de validación."
            ),
            extra={
                "validation": preview,
            },
        )

    service = _new_batch_import_service()

    try:
        result = service.import_validated(
            validation,
            organization_id=user.organization_id,
            actor=build_audit_actor_from_user(
                user
            ),
            request_context=(
                build_request_audit_context(
                    request
                )
            ),
            source_filename=workbook.filename,
            source_sha256=workbook.sha256,
            idempotency_key=normalized_key,
        )

        content = _serialize_import_result(
            result
        )

    except BatchSemanticValidationError as exc:
        preview = _serialize_validation(
            workbook=workbook,
            result=exc.result,
            organization_id=user.organization_id,
        )
        _raise_api_error(
            status_code=(
                status.HTTP_422_UNPROCESSABLE_CONTENT
            ),
            code="ROW_VALIDATION_FAILED",
            message=(
                "La planilla contiene filas "
                "con errores de validación."
            ),
            extra={
                "validation": preview,
            },
        )

    except BatchImportConflictError as exc:
        _raise_api_error(
            status_code=status.HTTP_409_CONFLICT,
            code="DUPLICATE_LOTE_IDENTIFIERS",
            message=(
                "Uno o más identificadores de lote "
                "ya existen en la organización."
            ),
            extra={
                "identifiers": list(
                    exc.identifiers
                ),
            },
        )

    except BatchImportIdempotencyConflictError:
        _raise_api_error(
            status_code=status.HTTP_409_CONFLICT,
            code="IDEMPOTENCY_CONFLICT",
            message=(
                "La clave de idempotencia ya fue "
                "utilizada para otro archivo."
            ),
        )

    except BatchImportPersistenceError:
        _raise_api_error(
            status_code=(
                status.HTTP_503_SERVICE_UNAVAILABLE
            ),
            code="BATCH_IMPORT_UNAVAILABLE",
            message=(
                "No fue posible completar "
                "la importación."
            ),
        )

    return JSONResponse(
        status_code=(
            status.HTTP_200_OK
            if result.replayed
            else status.HTTP_201_CREATED
        ),
        content=content,
        headers={
            "Cache-Control": "no-store",
        },
    )


@router.get(
    "/imports/{public_id}",
)
async def obtener_batch_import_endpoint(
    public_id: UUID,
    user: UserTenantContext = Depends(
        require_permission(Permission.LOTE_READ)
    ),
) -> JSONResponse:
    """Return one tenant-scoped persistent batch import result."""

    service = _new_batch_import_query_service()

    try:
        snapshot = service.get_by_public_id(
            organization_id=user.organization_id,
            public_id=public_id,
        )
    except BatchImportQueryError:
        _raise_api_error(
            status_code=(
                status.HTTP_503_SERVICE_UNAVAILABLE
            ),
            code="BATCH_QUERY_UNAVAILABLE",
            message=(
                "No fue posible consultar "
                "la importación."
            ),
        )

    if snapshot is None:
        _raise_api_error(
            status_code=status.HTTP_404_NOT_FOUND,
            code="BATCH_IMPORT_NOT_FOUND",
            message="Importación no encontrada.",
        )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=_serialize_import_snapshot(
            snapshot
        ),
        headers={
            "Cache-Control": "no-store",
        },
    )


@router.post(
    "/upload",
    include_in_schema=False,
)
async def procesar_batch_excel_legacy_endpoint(
    user: UserTenantContext = Depends(
        require_permission(Permission.LOTE_CREATE)
    ),
) -> None:
    """Explicitly retire the unsafe legacy ZIP upload contract."""

    del user

    _raise_api_error(
        status_code=status.HTTP_410_GONE,
        code="LEGACY_BATCH_ENDPOINT_RETIRED",
        message=(
            "El endpoint legacy fue retirado. "
            "Use /api/v1/batch/validate "
            "y /api/v1/batch/import."
        ),
    )