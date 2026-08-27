from io import BytesIO

from openpyxl import Workbook

from litoral_trace.services.document_ingestion import (
    DocumentValidationError,
    compute_sha256,
    validate_document_upload,
)


def _xlsx_bytes() -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["lote", "cantidad"])
    sheet.append(["LOT-442", 80])
    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def test_pdf_is_validated_and_fingerprinted():
    content = b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF"

    upload = validate_document_upload(filename="factura.pdf", content=content)

    assert upload.detected_mime_type == "application/pdf"
    assert upload.sha256 == compute_sha256(content)
    assert len(upload.sha256) == 64


def test_xlsx_is_detected_from_real_container():
    upload = validate_document_upload(filename="stock.xlsx", content=_xlsx_bytes())

    assert upload.detected_mime_type.endswith("spreadsheetml.sheet")
    assert upload.file_size_bytes > 0


def test_csv_accepts_semicolon_delimiter():
    upload = validate_document_upload(
        filename="operaciones.csv",
        content="lote;cantidad\nLOT-442;80\n".encode("utf-8"),
    )

    assert upload.detected_mime_type == "text/csv"


def test_rejects_extension_spoofing():
    try:
        validate_document_upload(filename="falso.pdf", content=b"not a pdf")
    except DocumentValidationError as exc:
        assert "no coincide" in str(exc)
    else:
        raise AssertionError("Spoofed PDF must be rejected")


def test_rejects_unsupported_extension():
    try:
        validate_document_upload(filename="imagen.jpg", content=b"jpeg")
    except DocumentValidationError as exc:
        assert "Formato no admitido" in str(exc)
    else:
        raise AssertionError("Unsupported extension must be rejected")


def test_rejects_oversized_file():
    try:
        validate_document_upload(
            filename="data.csv",
            content=b"a,b\n1,2\n",
            max_file_bytes=2,
        )
    except DocumentValidationError as exc:
        assert "supera" in str(exc)
    else:
        raise AssertionError("Oversized upload must be rejected")
