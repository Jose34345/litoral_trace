"""Neutral batch upload envelope validation for secure XLSX ingestion."""
from __future__ import annotations

from dataclasses import dataclass

from litoral_trace.services.batch import (
    BATCH_MAX_FILE_BYTES,
    BatchWorkbook,
    parsear_excel_lotes,
)


BATCH_HTTP_MULTIPART_OVERHEAD_BYTES = 1024 * 1024
BATCH_HTTP_MAX_REQUEST_BYTES = (
    BATCH_MAX_FILE_BYTES
    + BATCH_HTTP_MULTIPART_OVERHEAD_BYTES
)


@dataclass(frozen=True)
class BatchUploadEnvelopeError(ValueError):
    """Transport-neutral upload envelope validation error."""

    code: str
    detail: str

    def __str__(self) -> str:
        return self.detail


def validate_batch_upload_content_length(
    raw_content_length: str | None,
) -> None:
    normalized = str(
        raw_content_length or ""
    ).strip()

    if not normalized:
        return

    try:
        content_length = int(
            normalized
        )
    except ValueError:
        return

    if content_length > BATCH_HTTP_MAX_REQUEST_BYTES:
        raise BatchUploadEnvelopeError(
            code="REQUEST_TOO_LARGE",
            detail=(
                "La solicitud excede el tamaño máximo permitido."
            ),
        )


def parse_batch_upload_bytes(
    payload: bytes,
    *,
    filename: str,
) -> BatchWorkbook:
    if len(payload) > BATCH_MAX_FILE_BYTES:
        raise BatchUploadEnvelopeError(
            code="FILE_TOO_LARGE",
            detail=(
                "El archivo Excel excede el tamaño máximo permitido."
            ),
        )

    if not payload:
        raise BatchUploadEnvelopeError(
            code="EMPTY_FILE",
            detail="El archivo Excel está vacío.",
        )

    return parsear_excel_lotes(
        payload,
        filename=filename,
    )
