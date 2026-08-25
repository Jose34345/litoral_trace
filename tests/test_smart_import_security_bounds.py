from __future__ import annotations

from io import BytesIO

from openpyxl import Workbook
import pytest

from litoral_trace.services.batch import BATCH_COLUMNAS
from litoral_trace.services.smart_import import SmartImportEngine
from litoral_trace.services.smart_import.canonicalize import (
    SMART_MAX_SOURCE_ROW_SPAN,
    SmartCanonicalizationError,
    canonicalize_workbook,
    default_confirmed_mapping,
)
from litoral_trace.services.smart_import.matcher import (
    SMART_SAMPLE_TEXT_MAX_CHARS,
    map_source_column,
)
from litoral_trace.services.smart_import.normalize import (
    SMART_HEADER_TEXT_MAX_CHARS,
    normalize_header,
)


def _serialize(workbook: Workbook) -> bytes:
    buffer = BytesIO()
    workbook.save(buffer)
    workbook.close()
    return buffer.getvalue()


def test_header_normalization_caps_untrusted_text_before_fuzzy_work() -> None:
    raw = "A" * (SMART_HEADER_TEXT_MAX_CHARS + 10_000)

    normalized = normalize_header(raw)

    assert normalized == "a" * SMART_HEADER_TEXT_MAX_CHARS
    assert len(normalized) == SMART_HEADER_TEXT_MAX_CHARS


def test_mapper_retains_only_bounded_header_and_sample_text() -> None:
    mapping = map_source_column(
        "H" * (SMART_HEADER_TEXT_MAX_CHARS + 10_000),
        ["S" * (SMART_SAMPLE_TEXT_MAX_CHARS + 50_000)],
        source_index=0,
    )

    assert len(mapping.source_column) == SMART_HEADER_TEXT_MAX_CHARS
    assert len(mapping.sample_values) == 1
    assert isinstance(mapping.sample_values[0], str)
    assert len(mapping.sample_values[0]) == SMART_SAMPLE_TEXT_MAX_CHARS


def test_canonicalizer_rejects_sparse_pathological_source_range_before_iteration() -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Datos"
    sheet.append(BATCH_COLUMNAS)
    sheet.append(
        [
            "R-001",
            "P-001",
            "Pino",
            10.0,
            -27.4,
            -58.8,
            20.0,
            10.0,
        ]
    )

    # A single residual cell can make XLSX metadata declare a huge used range.
    # Keep it outside mapped columns so the test proves the range is rejected
    # before Smart Import performs a long row scan.
    sheet.cell(
        row=SMART_MAX_SOURCE_ROW_SPAN + 2,
        column=len(BATCH_COLUMNAS) + 1,
        value="residual-format-range",
    )
    payload = _serialize(workbook)

    analysis = SmartImportEngine().analyze(payload, filename="sparse-range.xlsx")
    candidate = analysis.best_candidate
    assert candidate is not None
    mappings = default_confirmed_mapping(candidate)
    assert {item.canonical_field for item in mappings} == set(BATCH_COLUMNAS)

    with pytest.raises(SmartCanonicalizationError) as exc_info:
        canonicalize_workbook(
            payload,
            filename="sparse-range.xlsx",
            candidate=candidate,
            mappings=mappings,
        )

    assert exc_info.value.code == "SMART_SOURCE_RANGE_TOO_LARGE"
