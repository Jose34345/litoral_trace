"""Browser ViewModels and safe HTML mapping for batch XLSX imports."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import secrets
from typing import Any
from uuid import UUID

from fastapi import Request, status
from starlette.datastructures import UploadFile

from litoral_trace.services.batch import (
    BATCH_MAX_FILE_BYTES,
    BATCH_MAX_ROWS,
    BATCH_MAX_SHEETS,
    BatchExcelValidationError,
    BatchValidationResult,
    BatchWorkbook,
)
from litoral_trace.services.batch_upload import (
    BatchUploadEnvelopeError,
    parse_batch_upload_bytes,
    validate_batch_upload_content_length,
)
from litoral_trace.services.batch_evidence import (
    BatchEvidenceView,
)
from litoral_trace.services.batch_queries import (
    BatchImportSnapshot,
)
from litoral_trace.services.batch_imports import (
    BatchImportIdempotencyConflictError,
    BatchImportResult,
    normalize_idempotency_key,
)
from litoral_trace.services.vault import (
    VaultDocumentView,
)
from litoral_trace.db.models.batch_evidence_link import (
    BATCH_EVIDENCE_TYPES,
)


_MIB = 1024 * 1024
_BROWSER_IDEMPOTENCY_BYTES = 24
_EVIDENCE_TYPE_LABELS = {
    "SOURCE_WORKBOOK": "Workbook fuente",
    "SUPPORTING_EVIDENCE": "Evidencia de soporte",
    "COMPLIANCE_EVIDENCE": "Evidencia de cumplimiento",
}
_DETAIL_RESULT_MESSAGES = {
    "linked": (
        "EVIDENCE_LINKED",
        "Evidencia vinculada",
        "La evidencia se vinculó a la importación.",
        "success",
    ),
    "replayed": (
        "EVIDENCE_REPLAYED",
        "Vínculo ya existente",
        "La evidencia ya estaba vinculada; no se creó un duplicado.",
        "info",
    ),
    "unlinked": (
        "EVIDENCE_UNLINKED",
        "Evidencia desvinculada",
        "La relación activa se removió. El documento Vault y el historial se conservan.",
        "success",
    ),
}


@dataclass(frozen=True)
class BatchImportHtmlError(RuntimeError):
    """Safe browser-facing import workflow error."""

    status_code: int
    code: str
    title: str
    message: str


@dataclass(frozen=True)
class BatchImportLimitsView:
    max_file_bytes: int
    max_file_mb: int
    max_rows: int
    max_sheets: int


@dataclass(frozen=True)
class BatchImportFormView:
    method: str
    enctype: str
    validate_action: str
    import_action: str
    file_field_name: str
    idempotency_field_name: str
    file_accept: str
    idempotency_key: str
    requires_reupload: bool


@dataclass(frozen=True)
class BatchImportAlertView:
    code: str
    title: str
    message: str


@dataclass(frozen=True)
class BatchImportPageMessageView:
    code: str
    title: str
    message: str
    level: str


@dataclass(frozen=True)
class BatchValidationRowErrorView:
    row: int
    field: str
    code: str
    message: str


@dataclass(frozen=True)
class BatchValidationView:
    valid: bool
    title: str
    message: str
    filename: str
    sheet_name: str
    total_rows: int
    valid_rows: int
    invalid_rows: int
    row_errors: tuple[BatchValidationRowErrorView, ...]


@dataclass(frozen=True)
class BatchImportResultView:
    code: str
    title: str
    message: str
    replayed: bool
    status: str | None = None
    import_id: str | None = None
    detail_href: str | None = None
    inserted_rows: int | None = None
    source_filename: str | None = None
    duplicate_identifiers: tuple[str, ...] = ()


@dataclass(frozen=True)
class BatchImportWorkspaceView:
    form: BatchImportFormView
    limits: BatchImportLimitsView
    alert: BatchImportAlertView | None = None
    validation: BatchValidationView | None = None
    result: BatchImportResultView | None = None


@dataclass(frozen=True)
class BatchImportDetailView:
    public_id: str
    status: str
    source_filename: str
    total_rows: int
    inserted_rows: int
    identifiers: tuple[str, ...]
    created_at: str
    completed_at: str | None


@dataclass(frozen=True)
class BatchImportEvidenceItemView:
    vault_document_public_id: str
    evidence_type: str
    evidence_type_label: str
    document_filename: str
    document_type: str
    document_content_type: str
    document_size_bytes: int
    document_sha256: str
    linked_at: str
    document_status: str
    document_available: bool
    unlink_action: str
    can_unlink: bool


@dataclass(frozen=True)
class BatchImportEvidenceDocumentChoiceView:
    public_id: str
    filename: str
    document_type: str
    content_type: str
    size_bytes: int


@dataclass(frozen=True)
class BatchImportEvidenceTypeChoiceView:
    value: str
    label: str


@dataclass(frozen=True)
class BatchImportEvidenceLinkFormView:
    action: str
    method: str
    document_field_name: str
    evidence_type_field_name: str
    document_choices: tuple[BatchImportEvidenceDocumentChoiceView, ...]
    evidence_type_choices: tuple[BatchImportEvidenceTypeChoiceView, ...]
    empty_documents_message: str | None = None


@dataclass(frozen=True)
class BatchImportDetailPageView:
    detail: BatchImportDetailView | None
    can_view_evidence: bool
    can_manage_evidence: bool
    evidence_items: tuple[BatchImportEvidenceItemView, ...]
    evidence_form: BatchImportEvidenceLinkFormView | None = None
    page_message: BatchImportPageMessageView | None = None
    evidence_error: BatchImportAlertView | None = None
    not_found: bool = False


def issue_browser_import_idempotency_key() -> str:
    """Issue a browser form key using the same normalization contract."""

    token = normalize_idempotency_key(
        secrets.token_urlsafe(
            _BROWSER_IDEMPOTENCY_BYTES
        )
    )

    if token is None:
        raise AssertionError(
            "Browser idempotency key generation produced no value."
        )

    return token


def normalize_browser_import_idempotency_key(
    value: str | None,
) -> str:
    if value is None or not str(value).strip():
        raise BatchImportHtmlError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="MISSING_IDEMPOTENCY_KEY",
            title="Formulario incompleto",
            message=(
                "La importacion requiere una clave de idempotencia "
                "emitida por el servidor."
            ),
        )

    try:
        normalized = normalize_idempotency_key(
            value
        )
    except BatchImportIdempotencyConflictError as exc:
        raise BatchImportHtmlError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="INVALID_IDEMPOTENCY_KEY",
            title="Formulario invalido",
            message=(
                "La clave de idempotencia del formulario no es valida. "
                "Recarga el workspace e intenta nuevamente."
            ),
        ) from exc

    if normalized is None:
        raise BatchImportHtmlError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="MISSING_IDEMPOTENCY_KEY",
            title="Formulario incompleto",
            message=(
                "La importacion requiere una clave de idempotencia "
                "emitida por el servidor."
            ),
        )

    return normalized


def workspace_limits_view() -> BatchImportLimitsView:
    return BatchImportLimitsView(
        max_file_bytes=BATCH_MAX_FILE_BYTES,
        max_file_mb=(
            BATCH_MAX_FILE_BYTES
            // _MIB
        ),
        max_rows=BATCH_MAX_ROWS,
        max_sheets=BATCH_MAX_SHEETS,
    )


def build_workspace_view(
    *,
    idempotency_key: str,
    requires_reupload: bool = False,
    alert: BatchImportAlertView | None = None,
    validation: BatchValidationView | None = None,
    result: BatchImportResultView | None = None,
) -> BatchImportWorkspaceView:
    return BatchImportWorkspaceView(
        form=BatchImportFormView(
            method="post",
            enctype="multipart/form-data",
            validate_action="/imports/validate",
            import_action="/imports",
            file_field_name="file",
            idempotency_field_name="idempotency_key",
            file_accept=".xlsx",
            idempotency_key=idempotency_key,
            requires_reupload=requires_reupload,
        ),
        limits=workspace_limits_view(),
        alert=alert,
        validation=validation,
        result=result,
    )


async def parse_browser_upload(
    file: UploadFile,
    *,
    request: Request,
) -> BatchWorkbook:
    try:
        validate_batch_upload_content_length(
            request.headers.get(
                "content-length"
            )
        )
    except BatchUploadEnvelopeError as exc:
        await file.close()
        raise structural_error_to_html(
            exc
        ) from exc

    filename = file.filename or ""

    try:
        payload = await file.read(
            BATCH_MAX_FILE_BYTES + 1
        )
    finally:
        await file.close()

    try:
        return parse_batch_upload_bytes(
            payload,
            filename=filename,
        )
    except (
        BatchUploadEnvelopeError,
        BatchExcelValidationError,
    ) as exc:
        raise structural_error_to_html(
            exc
        ) from exc


def structural_error_to_html(
    exc: (
        BatchUploadEnvelopeError
        | BatchExcelValidationError
    ),
) -> BatchImportHtmlError:
    code = str(
        getattr(exc, "code", "")
        or "BATCH_UPLOAD_ERROR"
    )
    message = str(
        getattr(exc, "detail", "")
        or "No fue posible procesar la planilla."
    )

    if code in {
        "REQUEST_TOO_LARGE",
        "FILE_TOO_LARGE",
        "XLSX_MEMBER_TOO_LARGE",
        "XLSX_EXPANDED_TOO_LARGE",
    }:
        title = "Archivo demasiado grande"
        status_code = status.HTTP_413_CONTENT_TOO_LARGE
    elif code in {
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
    }:
        title = "Planilla invalida"
        status_code = status.HTTP_422_UNPROCESSABLE_CONTENT
    else:
        title = "No se pudo leer la planilla"
        status_code = status.HTTP_400_BAD_REQUEST

    return BatchImportHtmlError(
        status_code=status_code,
        code=code,
        title=title,
        message=message,
    )


def present_validation(
    workbook: BatchWorkbook,
    result: BatchValidationResult,
) -> BatchValidationView:
    if result.valid:
        title = "Validacion completada"
        message = (
            "La planilla supero la validacion semantica del servidor. "
            "Volve a seleccionar el mismo archivo antes de importar, "
            "porque el navegador no conserva el archivo tras el POST."
        )
    else:
        title = "Validacion fallida"
        message = (
            "La planilla requiere correcciones antes de cualquier "
            "persistencia. No se importo ninguna fila."
        )

    return BatchValidationView(
        valid=result.valid,
        title=title,
        message=message,
        filename=workbook.filename,
        sheet_name=workbook.sheet_name,
        total_rows=result.total_rows,
        valid_rows=result.valid_rows,
        invalid_rows=result.invalid_rows,
        row_errors=tuple(
            BatchValidationRowErrorView(
                row=error.row,
                field=error.field,
                code=error.code,
                message=error.message,
            )
            for error in result.errors
        ),
    )


def present_import_success(
    workbook: BatchWorkbook,
    result: BatchImportResult,
) -> BatchImportResultView:
    if result.import_public_id is None:
        raise BatchImportHtmlError(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            code="IMPORT_IDENTITY_MISSING",
            title="Importacion incompleta",
            message=(
                "La importacion no devolvio una identidad persistente "
                "valida."
            ),
        )

    if result.replayed:
        title = "Importacion ya registrada"
        message = (
            "La misma clave de idempotencia ya habia completado esta "
            "planilla. Se muestra el resultado persistido sin crear "
            "una segunda importacion."
        )
    else:
        title = "Importacion creada"
        message = (
            "La importacion se persistio de forma atomica y auditable."
        )

    return BatchImportResultView(
        code=(
            "IMPORT_REPLAYED"
            if result.replayed
            else "IMPORT_CREATED"
        ),
        title=title,
        message=message,
        replayed=result.replayed,
        status="completed",
        import_id=str(
            result.import_public_id
        ),
        detail_href=(
            f"/imports/{result.import_public_id}"
        ),
        inserted_rows=result.inserted_rows,
        source_filename=workbook.filename,
    )


def present_import_error(
    *,
    code: str,
    title: str,
    message: str,
    duplicate_identifiers: tuple[str, ...] = (),
    import_id: str | None = None,
    source_filename: str | None = None,
) -> BatchImportResultView:
    return BatchImportResultView(
        code=code,
        title=title,
        message=message,
        replayed=False,
        import_id=import_id,
        detail_href=(
            f"/imports/{import_id}"
            if import_id is not None
            else None
        ),
        source_filename=source_filename,
        duplicate_identifiers=duplicate_identifiers,
    )


def _format_timestamp(
    value: datetime | None,
) -> str | None:
    if value is None:
        return None

    return value.isoformat()


def present_import_detail(
    snapshot: BatchImportSnapshot,
) -> BatchImportDetailView:
    return BatchImportDetailView(
        public_id=str(
            snapshot.public_id
        ),
        status=snapshot.status,
        source_filename=snapshot.source_filename,
        total_rows=snapshot.total_rows,
        inserted_rows=snapshot.inserted_rows,
        identifiers=snapshot.identifiers,
        created_at=_format_timestamp(
            snapshot.created_at
        )
        or "",
        completed_at=_format_timestamp(
            snapshot.completed_at
        ),
    )


def present_import_evidence(
    evidence: BatchEvidenceView,
    *,
    batch_import_public_id: str,
) -> BatchImportEvidenceItemView:
    return BatchImportEvidenceItemView(
        vault_document_public_id=str(
            evidence.vault_document_public_id
        ),
        evidence_type=evidence.evidence_type,
        evidence_type_label=_EVIDENCE_TYPE_LABELS.get(
            evidence.evidence_type,
            evidence.evidence_type,
        ),
        document_filename=evidence.document_filename,
        document_type=evidence.document_type,
        document_content_type=evidence.document_content_type,
        document_size_bytes=evidence.document_size_bytes,
        document_sha256=evidence.document_sha256,
        linked_at=_format_timestamp(
            evidence.linked_at
        )
        or "",
        document_status=evidence.document_status,
        document_available=evidence.document_available,
        unlink_action=(
            f"/imports/{batch_import_public_id}/evidence/"
            f"{evidence.vault_document_public_id}/unlink"
        ),
        can_unlink=evidence.document_available,
    )


def present_evidence_document_choice(
    document: VaultDocumentView,
) -> BatchImportEvidenceDocumentChoiceView:
    return BatchImportEvidenceDocumentChoiceView(
        public_id=str(
            document.public_id
        ),
        filename=document.filename,
        document_type=document.document_type,
        content_type=document.content_type,
        size_bytes=document.size_bytes,
    )


def present_evidence_type_choices() -> tuple[BatchImportEvidenceTypeChoiceView, ...]:
    return tuple(
        BatchImportEvidenceTypeChoiceView(
            value=value,
            label=_EVIDENCE_TYPE_LABELS.get(
                value,
                value,
            ),
        )
        for value in sorted(
            BATCH_EVIDENCE_TYPES
        )
    )


def build_import_evidence_form(
    *,
    batch_import_public_id: str,
    available_documents: tuple[VaultDocumentView, ...],
) -> BatchImportEvidenceLinkFormView:
    return BatchImportEvidenceLinkFormView(
        action=f"/imports/{batch_import_public_id}/evidence",
        method="post",
        document_field_name="document_id",
        evidence_type_field_name="evidence_type",
        document_choices=tuple(
            present_evidence_document_choice(
                document
            )
            for document in available_documents
        ),
        evidence_type_choices=present_evidence_type_choices(),
        empty_documents_message=(
            "No hay documentos Vault disponibles para vincular."
            if not available_documents
            else None
        ),
    )


def present_import_detail_result(
    result_code: str | None,
) -> BatchImportPageMessageView | None:
    if result_code is None:
        return None

    message = _DETAIL_RESULT_MESSAGES.get(
        str(result_code).strip().lower()
    )
    if message is None:
        return None

    code, title, text, level = message
    return BatchImportPageMessageView(
        code=code,
        title=title,
        message=text,
        level=level,
    )


def present_evidence_mutation_error(
    *,
    code: str,
    message: str | None = None,
) -> BatchImportPageMessageView:
    catalog = {
        "INVALID_EVIDENCE_TYPE": (
            "Tipo de evidencia inválido",
            (
                "Seleccioná un tipo de evidencia permitido por el "
                "sistema."
            ),
            "warning",
        ),
        "SOURCE_WORKBOOK_REQUIRES_REMITO_EXCEL": (
            "Documento incompatible",
            (
                "SOURCE_WORKBOOK sólo admite documentos Vault del tipo "
                "REMITO_EXCEL."
            ),
            "warning",
        ),
        "SOURCE_WORKBOOK_HASH_MISMATCH": (
            "Workbook no coincide",
            (
                "El documento SOURCE_WORKBOOK debe coincidir exactamente "
                "con el SHA-256 de la planilla importada."
            ),
            "warning",
        ),
        "SOURCE_WORKBOOK_ALREADY_LINKED": (
            "Workbook fuente ya vinculado",
            (
                "La importación ya posee una evidencia "
                "SOURCE_WORKBOOK activa."
            ),
            "warning",
        ),
        "EVIDENCE_TYPE_CONFLICT": (
            "Conflicto de evidencia",
            (
                "Ese documento ya está vinculado a la importación con "
                "otro tipo de evidencia."
            ),
            "warning",
        ),
        "VAULT_DOCUMENT_NOT_FOUND": (
            "Documento no disponible",
            (
                "El documento solicitado no existe o no está disponible "
                "para la organización actual."
            ),
            "warning",
        ),
        "BATCH_IMPORT_NOT_FOUND": (
            "Importación no disponible",
            (
                "La importación solicitada no existe o no está disponible "
                "para la organización actual."
            ),
            "warning",
        ),
        "EVIDENCE_NOT_FOUND": (
            "Evidencia no disponible",
            (
                "El recurso solicitado no existe o no está disponible "
                "para la organización actual."
            ),
            "warning",
        ),
        "EVIDENCE_LINK_NOT_FOUND": (
            "Vínculo no disponible",
            (
                "La relación activa solicitada no existe o no está "
                "disponible para la organización actual."
            ),
            "warning",
        ),
        "BATCH_EVIDENCE_UNAVAILABLE": (
            "Operación no disponible",
            (
                "No fue posible completar la operación de evidencia en "
                "este momento."
            ),
            "danger",
        ),
    }
    title, fallback_message, level = catalog.get(
        code,
        (
            "Operación no disponible",
            (
                "No fue posible completar la operación de evidencia en "
                "este momento."
            ),
            "danger",
        ),
    )
    return BatchImportPageMessageView(
        code=code,
        title=title,
        message=message or fallback_message,
        level=level,
    )


def present_import_detail_page(
    *,
    snapshot: BatchImportSnapshot | None,
    can_view_evidence: bool,
    can_manage_evidence: bool = False,
    evidence: tuple[BatchEvidenceView, ...] = (),
    available_documents: tuple[VaultDocumentView, ...] = (),
    page_message: BatchImportPageMessageView | None = None,
    evidence_error: BatchImportAlertView | None = None,
    not_found: bool = False,
) -> BatchImportDetailPageView:
    detail = (
        present_import_detail(snapshot)
        if snapshot is not None
        else None
    )
    return BatchImportDetailPageView(
        detail=detail,
        can_view_evidence=can_view_evidence,
        can_manage_evidence=can_manage_evidence,
        evidence_items=tuple(
            present_import_evidence(
                item,
                batch_import_public_id=(
                    detail.public_id
                    if detail is not None
                    else ""
                ),
            )
            for item in evidence
        ),
        evidence_form=(
            build_import_evidence_form(
                batch_import_public_id=detail.public_id,
                available_documents=available_documents,
            )
            if detail is not None and can_manage_evidence
            else None
        ),
        page_message=page_message,
        evidence_error=evidence_error,
        not_found=not_found,
    )


def workspace_context(
    view: BatchImportWorkspaceView,
) -> dict[str, Any]:
    return {
        "batch_import_view": view,
    }
