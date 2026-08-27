"""Zero-friction document ingestion primitives.

This module performs cheap deterministic checks before any OCR/LLM work. The
original bytes are fingerprinted immediately so downstream storage and
processing can be idempotent.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from io import BytesIO
from pathlib import Path
import zipfile


DEFAULT_MAX_FILE_BYTES = 25 * 1024 * 1024
ALLOWED_EXTENSIONS = {".pdf", ".xlsx", ".xls", ".csv"}


class DocumentValidationError(ValueError):
    """Rejected upload before persistence/extraction."""


@dataclass(frozen=True, slots=True)
class ValidatedDocumentUpload:
    filename: str
    extension: str
    detected_mime_type: str
    file_size_bytes: int
    sha256: str
    content: bytes


def compute_sha256(content: bytes) -> str:
    """Return canonical lowercase SHA-256 for original file bytes."""
    return hashlib.sha256(content).hexdigest()


def _detect_xlsx(content: bytes) -> bool:
    if not content.startswith(b"PK"):
        return False
    try:
        with zipfile.ZipFile(BytesIO(content)) as archive:
            names = set(archive.namelist())
    except (zipfile.BadZipFile, OSError):
        return False
    return "[Content_Types].xml" in names and any(name.startswith("xl/") for name in names)


def _looks_like_csv(content: bytes) -> bool:
    if b"\x00" in content:
        return False
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = content.decode(encoding)
        except UnicodeDecodeError:
            continue
        lines = [line for line in text.splitlines() if line.strip()]
        if not lines:
            return False
        sample = "\n".join(lines[:5])
        return any(separator in sample for separator in (",", ";", "\t"))
    return False


def detect_mime_type(filename: str, content: bytes) -> str:
    """Detect the supported file type from content, not browser headers alone."""
    extension = Path(filename).suffix.lower()

    if extension == ".pdf" and content.startswith(b"%PDF-"):
        return "application/pdf"
    if extension == ".xlsx" and _detect_xlsx(content):
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if extension == ".xls" and content.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return "application/vnd.ms-excel"
    if extension == ".csv" and _looks_like_csv(content):
        return "text/csv"

    raise DocumentValidationError(
        f"El contenido no coincide con un archivo {extension or 'admitido'} válido."
    )


def validate_document_upload(
    *,
    filename: str,
    content: bytes,
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES,
) -> ValidatedDocumentUpload:
    """Validate extension, size, corruption signature and compute integrity hash."""
    safe_name = Path(filename).name.strip()
    if not safe_name:
        raise DocumentValidationError("El archivo debe tener un nombre válido.")

    extension = Path(safe_name).suffix.lower()
    if extension not in ALLOWED_EXTENSIONS:
        raise DocumentValidationError(
            "Formato no admitido. Use PDF, XLSX, XLS o CSV."
        )
    if not content:
        raise DocumentValidationError("El archivo está vacío.")
    if len(content) > max_file_bytes:
        raise DocumentValidationError(
            f"El archivo supera el máximo permitido de {max_file_bytes} bytes."
        )

    detected_mime_type = detect_mime_type(safe_name, content)
    return ValidatedDocumentUpload(
        filename=safe_name,
        extension=extension,
        detected_mime_type=detected_mime_type,
        file_size_bytes=len(content),
        sha256=compute_sha256(content),
        content=content,
    )
