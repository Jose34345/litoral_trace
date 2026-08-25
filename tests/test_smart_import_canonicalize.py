from __future__ import annotations

from io import BytesIO

from openpyxl import Workbook
import pytest

from litoral_trace.services.batch import BATCH_COLUMNAS, validar_filas_lotes
from litoral_trace.services.smart_import import SmartImportEngine
from litoral_trace.services.smart_import.canonicalize import (
    SmartCanonicalizationError,
    canonicalize_workbook,
    default_confirmed_mapping,
)


def _build_workbook(*, formula_in_extra: bool = False, formula_in_mapped: bool = False) -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Movimientos"
    sheet.append(["FORESTAL XYZ"])
    sheet.append([])
    sheet.append(
        [
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
    )
    sheet.append(
        [
            "2026-08-01",
            "R-001",
            "P-001",
            "Pino",
            "=5+5" if formula_in_mapped else 10.0,
            -27.4,
            -58.8,
            25.0,
            12.0,
            "=CONCAT(\"AB\",\"123CD\")" if formula_in_extra else "AB123CD",
        ]
    )
    sheet.append(
        [
            "2026-08-02",
            "R-002",
            "P-002",
            "Eucalipto",
            8.0,
            -27.5,
            -58.9,
            20.0,
            10.0,
            "AC456EF",
        ]
    )

    buffer = BytesIO()
    workbook.save(buffer)
    workbook.close()
    return buffer.getvalue()


def test_canonicalizer_projects_only_confirmed_columns_into_existing_schema() -> None:
    payload = _build_workbook()
    analysis = SmartImportEngine().analyze(payload, filename="cliente.xlsx")
    candidate = analysis.best_candidate
    assert candidate is not None

    workbook = canonicalize_workbook(
        payload,
        filename="cliente.xlsx",
        candidate=candidate,
        mappings=default_confirmed_mapping(candidate),
    )

    assert list(workbook.dataframe.columns) == BATCH_COLUMNAS
    assert workbook.row_count == 2
    assert workbook.source_row_numbers == (4, 5)
    assert workbook.dataframe.iloc[0]["Identificador_Lote"] == "R-001"
    assert "Patente" not in workbook.dataframe.columns

    validation = validar_filas_lotes(workbook)
    assert validation.valid is True
    assert validation.invalid_rows == 0


def test_canonicalizer_ignores_formula_in_unmapped_extra_column() -> None:
    payload = _build_workbook(formula_in_extra=True)
    analysis = SmartImportEngine().analyze(payload, filename="cliente.xlsx")
    candidate = analysis.best_candidate
    assert candidate is not None

    workbook = canonicalize_workbook(
        payload,
        filename="cliente.xlsx",
        candidate=candidate,
        mappings=default_confirmed_mapping(candidate),
    )

    assert validar_filas_lotes(workbook).valid is True


def test_canonicalizer_fails_closed_on_formula_in_mapped_column() -> None:
    payload = _build_workbook(formula_in_mapped=True)
    analysis = SmartImportEngine().analyze(payload, filename="cliente.xlsx")
    candidate = analysis.best_candidate
    assert candidate is not None

    with pytest.raises(SmartCanonicalizationError) as exc_info:
        canonicalize_workbook(
            payload,
            filename="cliente.xlsx",
            candidate=candidate,
            mappings=default_confirmed_mapping(candidate),
        )

    # Smart Import preserves the strict batch parser's public security contract:
    # formulas in any mapped business field are rejected, never evaluated.
    assert exc_info.value.code == "FORMULA_NOT_ALLOWED"
