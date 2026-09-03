"""One-pass layout extraction that retains PDF coordinates and table relationships."""
from __future__ import annotations

from io import BytesIO
from pathlib import PurePath

from .domain import BoundingBox, LayoutBlock, LayoutStructureType, ParsedLayout
from .errors import LaceyEngineError


def _text(value: object) -> str:
    return " ".join(str(value or "").split())


def _bbox(item: dict) -> BoundingBox:
    return BoundingBox(float(item["x0"]), float(item["top"]), float(item["x1"]), float(item["bottom"]))


def _ocr_blocks(content: bytes, page_numbers: set[int] | None = None) -> list[LayoutBlock]:
    """Coordinate-preserving OCR fallback for image-only shipment documents."""
    try:
        import pypdfium2 as pdfium
        import pytesseract
        from pytesseract import Output
    except ImportError as exc:  # pragma: no cover - optional dependency gate
        raise LaceyEngineError("PDF has no digital text and OCR dependencies are unavailable") from exc
    document = pdfium.PdfDocument(content)
    blocks: list[LayoutBlock] = []
    try:
        for page_index in range(len(document)):
            if page_numbers is not None and page_index + 1 not in page_numbers:
                continue
            page = document[page_index]
            bitmap = page.render(scale=2)
            image = bitmap.to_pil().convert("L")
            data = pytesseract.image_to_data(image, lang="eng", output_type=Output.DICT, config="--psm 6")
            lines: dict[tuple[int, int, int], list[int]] = {}
            for index, word in enumerate(data["text"]):
                if _text(word):
                    lines.setdefault((data["block_num"][index], data["par_num"][index], data["line_num"][index]), []).append(index)
            for number, indexes in enumerate(lines.values(), start=1):
                text = _text(" ".join(data["text"][index] for index in indexes))
                blocks.append(LayoutBlock(
                    f"ocr-p{page_index + 1}-l{number}", page_index + 1,
                    BoundingBox(min(float(data["left"][index]) for index in indexes),
                                min(float(data["top"][index]) for index in indexes),
                                max(float(data["left"][index] + data["width"][index]) for index in indexes),
                                max(float(data["top"][index] + data["height"][index]) for index in indexes)),
                    text, "OCR_LINE"
                ))
            image.close()
            bitmap.close()
            page.close()
    finally:
        document.close()
    return blocks


def _pdf_layout(content: bytes) -> ParsedLayout:
    try:
        import pdfplumber
    except ImportError as exc:  # pragma: no cover - runtime dependency
        raise LaceyEngineError("pdfplumber is required to read PDF documents") from exc
    blocks: list[LayoutBlock] = []
    page_count = 0
    try:
        with pdfplumber.open(BytesIO(content)) as pdf:
            page_count = len(pdf.pages)
            for page_number, page in enumerate(pdf.pages, start=1):
                page_start = len(blocks)
                # Text lines preserve label/value relationships even when a PDF has
                # no detectable ruled table.  They also give every extraction a
                # highlightable page coordinate.
                line_blocks = []
                extract_lines = getattr(page, "extract_text_lines", None)
                if callable(extract_lines):
                    line_blocks = extract_lines(layout=True, strip=True) or []
                for line_number, line in enumerate(line_blocks, start=1):
                    text = _text(line.get("text"))
                    if text:
                        blocks.append(LayoutBlock(
                            f"p{page_number}-l{line_number}", page_number, _bbox(line), text, "TEXT_LINE"
                        ))
                words = page.extract_words(use_text_flow=True, keep_blank_chars=False) or []
                if not line_blocks:
                    # Older pdfplumber releases do not offer extract_text_lines.
                    # Reconstruct visual lines from words, retaining their union
                    # bbox rather than falling back to coordinate-free text.
                    grouped: list[list[dict]] = []
                    for word in sorted(words, key=lambda item: (float(item["top"]), float(item["x0"]))):
                        if not grouped or abs(float(word["top"]) - float(grouped[-1][0]["top"])) > 2:
                            grouped.append([word])
                        else:
                            grouped[-1].append(word)
                    for line_number, line_words in enumerate(grouped, start=1):
                        text = _text(" ".join(str(word.get("text") or "") for word in line_words))
                        if text:
                            blocks.append(LayoutBlock(
                                f"p{page_number}-l{line_number}", page_number,
                                BoundingBox(min(float(word["x0"]) for word in line_words),
                                            min(float(word["top"]) for word in line_words),
                                            max(float(word["x1"]) for word in line_words),
                                            max(float(word["bottom"]) for word in line_words)),
                                text, "TEXT_LINE"
                            ))
                for word_number, word in enumerate(words, start=1):
                    text = _text(word.get("text"))
                    if text:
                        blocks.append(LayoutBlock(f"p{page_number}-w{word_number}", page_number, _bbox(word), text, "WORD"))
                for table_number, table in enumerate(page.find_tables() or [], start=1):
                    table_id = f"p{page_number}-t{table_number}"
                    rows = table.extract() or []
                    # A consistently two-column table is semantically key/value.
                    is_key_value = bool(rows) and all(len(row or []) == 2 for row in rows if row)
                    for row_index, row in enumerate(rows):
                        cells = row or []
                        if is_key_value and len(cells) == 2:
                            key, value = _text(cells[0]), _text(cells[1])
                            if key or value:
                                blocks.append(LayoutBlock(
                                    f"{table_id}-r{row_index}", page_number, None,
                                    f"{key}: {value}", "TABLE_ROW", LayoutStructureType.KEY_VALUE_TABLE,
                                    table_id, row_index, None, key, key, value,
                                ))
                # Mixed PDFs need OCR only for pages without digital layout.
                if len(blocks) == page_start:
                    blocks.extend(_ocr_blocks(content, {page_number}))
    except Exception as exc:
        raise LaceyEngineError("Unable to read PDF layout") from exc
    if not blocks:
        raise LaceyEngineError("PDF contains no usable text after OCR")
    return ParsedLayout(tuple(blocks), page_count)


def parse_layout(filename: str, content: bytes) -> ParsedLayout:
    if PurePath(filename).suffix.lower() != ".pdf":
        raise LaceyEngineError("Gate 1 currently supports PDF documents only")
    if not content.startswith(b"%PDF-"):
        raise LaceyEngineError("Input is not a valid PDF")
    return _pdf_layout(content)


def layout_from_key_value_rows(rows: list[tuple[str, str]]) -> ParsedLayout:
    """Small deterministic test seam for vertical key/value table parsing."""
    return ParsedLayout(tuple(
        LayoutBlock(f"t1-r{i}", 1, None, f"{key}: {value}", "TABLE_ROW",
                    LayoutStructureType.KEY_VALUE_TABLE, "t1", i, None, key, key, value)
        for i, (key, value) in enumerate(rows)
    ), 1)
