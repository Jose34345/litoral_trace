from __future__ import annotations

from io import BytesIO
import shutil
import sys
from types import SimpleNamespace

from PIL import Image, ImageDraw, ImageFont
import pytest

from litoral_trace.assurance import parsers


def _image_only_pdf(text: str = "INVOICE 6875 LACEY") -> bytes:
    image = Image.new("L", (600, 120), 255)
    draw = ImageDraw.Draw(image)
    draw.text((20, 45), text, font=ImageFont.load_default(), fill=0)
    image = image.resize((3600, 720), Image.Resampling.NEAREST).convert("RGB")
    buffer = BytesIO()
    image.save(buffer, format="PDF", resolution=150.0)
    return buffer.getvalue()


def test_parse_pdf_falls_back_to_pdfium_page_count_when_pypdf_inspection_crashes(monkeypatch):
    """A pypdf inspection failure must not prevent OCR of an otherwise readable scan."""
    content = b"%PDF-1.4\nsynthetic image-only source\n%%EOF"
    monkeypatch.setattr(
        parsers,
        "_extract_pdf_text_pages",
        lambda _content: (_ for _ in ()).throw(RuntimeError("pypdf layout crash")),
    )
    monkeypatch.setattr(parsers, "_pdf_page_count_with_pdfium", lambda _content: 1)
    monkeypatch.setattr(
        parsers,
        "_ocr_scanned_pdf",
        lambda _content, page_count: (
            "INVOICE NUMBER 6875",
            {
                "ocr_attempted": True,
                "ocr_applied": True,
                "ocr_engine": "tesseract_fallback",
                "ocr_pages_processed": page_count,
            },
        ),
    )

    parsed = parsers.parse_pdf(content)

    assert parsed.text == "INVOICE NUMBER 6875"
    assert parsed.ocr_required is False
    assert parsed.metadata["page_count"] == 1
    assert parsed.metadata["pages_with_text"] == 0
    assert parsed.metadata["text_extraction_fallback"] == "pdfium_page_count"
    assert parsed.metadata["text_extraction_error_type"] == "RuntimeError"


def test_tesseract_fallback_reports_missing_binary_before_render(monkeypatch):
    """Missing native Tesseract must be a deterministic diagnostic, not a generic crash."""
    monkeypatch.setattr(shutil, "which", lambda _name: None)

    class UnexpectedDocument:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("PDF rendering must not start without the OCR binary")

    monkeypatch.setitem(sys.modules, "pypdfium2", SimpleNamespace(PdfDocument=UnexpectedDocument))
    monkeypatch.setitem(
        sys.modules,
        "pytesseract",
        SimpleNamespace(image_to_string=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("OCR must not run"))),
    )

    text, metadata = parsers._ocr_scanned_pdf_with_tesseract(
        b"%PDF-1.4\nscan\n%%EOF",
        page_count=1,
    )

    assert text == ""
    assert metadata["ocr_applied"] is False
    assert metadata["ocr_error_code"] == "OCR_TESSERACT_BINARY_UNAVAILABLE"
    assert metadata["ocr_binary"] == "tesseract"


@pytest.mark.skipif(shutil.which("tesseract") is None, reason="native Tesseract unavailable")
def test_image_only_pdf_runtime_smoke_extracts_text_with_tesseract():
    """CI/runtime smoke: raster-only PDF -> PDFium render -> native Tesseract text."""
    content = _image_only_pdf()

    text, metadata = parsers._ocr_scanned_pdf_with_tesseract(content, page_count=1)

    canonical = " ".join(text.upper().split())
    assert "INVOICE" in canonical
    assert "6875" in canonical
    assert metadata["ocr_applied"] is True
    assert metadata["ocr_pages_processed"] == 1
    assert metadata["ocr_pages_with_text"] == 1
