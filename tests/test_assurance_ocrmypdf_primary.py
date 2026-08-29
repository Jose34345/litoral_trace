from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace

from litoral_trace.assurance import parsers


def test_ocrmypdf_is_primary_layer_with_safe_pdf_options(monkeypatch):
    calls: dict[str, object] = {}

    def fake_ocr(input_file, output_file, **kwargs):
        calls["input_exists"] = Path(input_file).exists()
        calls["kwargs"] = dict(kwargs)
        Path(output_file).write_bytes(b"%PDF-1.4\n% derived test output\n%%EOF")
        return 0

    monkeypatch.setitem(sys.modules, "ocrmypdf", SimpleNamespace(ocr=fake_ocr))
    monkeypatch.setattr(
        parsers,
        "_extract_pdf_text_pages",
        lambda content: ("INVOICE NUMBER 6875", 1, 1),
    )
    monkeypatch.setenv("LT_ASSURANCE_OCR_TIMEOUT_SECONDS", "60")

    text, metadata = parsers._ocr_scanned_pdf_with_ocrmypdf(
        b"%PDF-1.4\n% image-only input\n%%EOF",
        page_count=1,
    )

    assert text == "INVOICE NUMBER 6875"
    assert metadata["ocr_engine"] == "ocrmypdf"
    assert metadata["ocr_applied"] is True
    assert metadata["ocr_timeout_seconds"] == 60
    assert metadata["ocr_pages_processed"] == 1
    assert calls["input_exists"] is True

    kwargs = calls["kwargs"]
    assert kwargs["language"] == ["spa", "eng"]
    assert kwargs["output_type"] == "pdf"
    assert kwargs["mode"] == "skip"
    assert kwargs["jobs"] == 1
    assert kwargs["use_threads"] is True
    assert kwargs["optimize"] == 0
    assert kwargs["tesseract_timeout"] == 60.0
    assert kwargs["progress_bar"] is False


def test_successful_ocrmypdf_does_not_call_legacy_fallback(monkeypatch):
    monkeypatch.setattr(
        parsers,
        "_ocr_scanned_pdf_with_ocrmypdf",
        lambda content, page_count: (
            "INVOICE 6875 Concepts Marketing Group",
            {
                "ocr_attempted": True,
                "ocr_applied": True,
                "ocr_engine": "ocrmypdf",
            },
        ),
    )

    def unexpected_fallback(*args, **kwargs):
        raise AssertionError("legacy Tesseract fallback must not run after OCRmyPDF succeeds")

    monkeypatch.setattr(
        parsers,
        "_ocr_scanned_pdf_with_tesseract",
        unexpected_fallback,
    )

    text, metadata = parsers._ocr_scanned_pdf(b"pdf", page_count=1)

    assert "INVOICE 6875" in text
    assert metadata["ocr_engine"] == "ocrmypdf"
    assert metadata["ocr_fallback_used"] is False


def test_ocrmypdf_failure_falls_back_without_losing_primary_diagnostics(monkeypatch):
    monkeypatch.setattr(
        parsers,
        "_ocr_scanned_pdf_with_ocrmypdf",
        lambda content, page_count: (
            "",
            {
                "ocr_attempted": True,
                "ocr_applied": False,
                "ocr_engine": "ocrmypdf",
                "ocr_engine_version": "17.10.0",
                "ocr_error_code": "OCRMY_PDF_EXECUTION_FAILED",
                "ocr_error_type": "RuntimeError",
            },
        ),
    )
    monkeypatch.setattr(
        parsers,
        "_ocr_scanned_pdf_with_tesseract",
        lambda content, page_count: (
            "FACTURA 0001-00001234",
            {
                "ocr_attempted": True,
                "ocr_applied": True,
                "ocr_engine": "tesseract",
                "ocr_pages_processed": 1,
            },
        ),
    )

    text, metadata = parsers._ocr_scanned_pdf(b"pdf", page_count=1)

    assert text == "FACTURA 0001-00001234"
    assert metadata["ocr_applied"] is True
    assert metadata["ocr_engine"] == "tesseract_fallback"
    assert metadata["ocr_fallback_used"] is True
    assert metadata["ocr_primary_engine"] == "ocrmypdf"
    assert metadata["ocr_primary_engine_version"] == "17.10.0"
    assert metadata["ocr_primary_error_code"] == "OCRMY_PDF_EXECUTION_FAILED"
    assert metadata["ocr_primary_error_type"] == "RuntimeError"
