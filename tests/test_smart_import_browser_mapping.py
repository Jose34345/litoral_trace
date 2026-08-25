from __future__ import annotations

from io import BytesIO

import pytest
from openpyxl import Workbook

from litoral_trace.services.batch import BATCH_COLUMNAS
from litoral_trace.services.smart_import import SmartImportEngine, SmartImportError
from litoral_trace.web.smart_import import (
    SMART_CONFIRM_FIELD,
    SMART_HEADER_FINGERPRINT_FIELD,
    SMART_HEADER_ROW_FIELD,
    SMART_MAPPING_FIELD_PREFIX,
    SMART_SHEET_FIELD,
    _candidate_fingerprint,
    _explicit_mapping,
    _mapping_is_complete,
    _select_candidate,
)


_BASE_HEADERS = [
    "Fecha",
    "Rodal",
    "Productor",
    "Especie",
    "Sup. ha",
    "LAT",
    "LONG",
    "Tn recibidas",
    "Stock exportable",
    "Patente",
]
_BASE_ROW = [
    "2026-08-24",
    "R-1",
    "P-1",
    "Pino",
    10.0,
    -27.4,
    -58.8,
    20.0,
    12.0,
    "AB123CD",
]


def _analysis_candidate(
    *,
    headers: list[str] | None = None,
    row: list[object] | None = None,
):
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Movimientos"
    sheet.append(["Reporte"])
    sheet.append(headers or list(_BASE_HEADERS))
    sheet.append(row or list(_BASE_ROW))
    buffer = BytesIO()
    workbook.save(buffer)
    workbook.close()

    analysis = SmartImportEngine().analyze(
        buffer.getvalue(),
        filename="cliente.xlsx",
    )
    candidate = analysis.best_candidate
    assert candidate is not None
    return analysis, candidate


def _candidate():
    return _analysis_candidate()


def _confirmed_form(candidate) -> dict[str, str]:
    form = {
        SMART_CONFIRM_FIELD: "1",
        SMART_SHEET_FIELD: candidate.sheet_name,
        SMART_HEADER_ROW_FIELD: str(candidate.header_row),
        SMART_HEADER_FINGERPRINT_FIELD: _candidate_fingerprint(candidate),
    }
    for column in candidate.mappings:
        target = column.decision.canonical_field
        if target in BATCH_COLUMNAS:
            form[f"{SMART_MAPPING_FIELD_PREFIX}{target}"] = str(column.source_index)
    return form


def test_browser_mapping_requires_all_eight_targets_to_be_complete() -> None:
    _, candidate = _candidate()
    form = _confirmed_form(candidate)
    mapping, confirmed = _explicit_mapping(form, candidate)

    assert confirmed is True
    assert mapping is not None
    assert _mapping_is_complete(mapping) is True


def test_browser_mapping_rejects_one_source_column_reused_for_two_targets() -> None:
    _, candidate = _candidate()
    form = _confirmed_form(candidate)
    first_index = form[f"{SMART_MAPPING_FIELD_PREFIX}Identificador_Lote"]
    form[f"{SMART_MAPPING_FIELD_PREFIX}ID_Proveedor"] = first_index

    with pytest.raises(SmartImportError) as exc_info:
        _explicit_mapping(form, candidate)

    assert exc_info.value.code == "SMART_DUPLICATE_SOURCE_MAPPING"


def test_candidate_confirmation_is_bound_to_sheet_and_header_row() -> None:
    analysis, candidate = _candidate()
    form = {
        SMART_CONFIRM_FIELD: "1",
        SMART_SHEET_FIELD: candidate.sheet_name,
        SMART_HEADER_ROW_FIELD: str(candidate.header_row + 1),
        SMART_HEADER_FINGERPRINT_FIELD: _candidate_fingerprint(candidate),
    }

    with pytest.raises(SmartImportError) as exc_info:
        _select_candidate(analysis, form)

    assert exc_info.value.code == "SMART_CANDIDATE_CHANGED"


def test_confirmed_candidate_requires_header_fingerprint() -> None:
    analysis, candidate = _candidate()
    form = {
        SMART_CONFIRM_FIELD: "1",
        SMART_SHEET_FIELD: candidate.sheet_name,
        SMART_HEADER_ROW_FIELD: str(candidate.header_row),
    }

    with pytest.raises(SmartImportError) as exc_info:
        _select_candidate(analysis, form)

    assert exc_info.value.code == "SMART_CANDIDATE_CHANGED"


def test_confirmed_candidate_rejects_tampered_header_fingerprint() -> None:
    analysis, candidate = _candidate()
    form = {
        SMART_CONFIRM_FIELD: "1",
        SMART_SHEET_FIELD: candidate.sheet_name,
        SMART_HEADER_ROW_FIELD: str(candidate.header_row),
        SMART_HEADER_FINGERPRINT_FIELD: "0" * 64,
    }

    with pytest.raises(SmartImportError) as exc_info:
        _select_candidate(analysis, form)

    assert exc_info.value.code == "SMART_CANDIDATE_CHANGED"


def test_stale_confirmation_rejects_reordered_columns_even_with_same_headers() -> None:
    _, original = _candidate()
    stale_fingerprint = _candidate_fingerprint(original)

    reordered_headers = list(_BASE_HEADERS)
    reordered_row = list(_BASE_ROW)
    reordered_headers[1], reordered_headers[2] = reordered_headers[2], reordered_headers[1]
    reordered_row[1], reordered_row[2] = reordered_row[2], reordered_row[1]
    analysis, reordered = _analysis_candidate(
        headers=reordered_headers,
        row=reordered_row,
    )

    assert _candidate_fingerprint(reordered) != stale_fingerprint
    form = {
        SMART_CONFIRM_FIELD: "1",
        SMART_SHEET_FIELD: reordered.sheet_name,
        SMART_HEADER_ROW_FIELD: str(reordered.header_row),
        SMART_HEADER_FINGERPRINT_FIELD: stale_fingerprint,
    }

    with pytest.raises(SmartImportError) as exc_info:
        _select_candidate(analysis, form)

    assert exc_info.value.code == "SMART_CANDIDATE_CHANGED"
