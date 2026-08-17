from __future__ import annotations

import io

from openpyxl import Workbook
import pandas as pd
import pytest

from litoral_trace.services.batch import (
    BATCH_COLUMNAS,
    BATCH_MAX_TEXT_LENGTH,
    BATCH_SHEET_NAME,
    BatchExcelValidationError,
    BatchSemanticValidationError,
    parsear_excel_lotes,
    procesar_lote_masivo,
    validar_dataframe_lotes,
    validar_filas_lotes,
)


def _row(
    *,
    identificador: object = "RODAL-001",
    proveedor: object = "30-12345678-9",
    producto: object = "Madera Aserrada (Pino)",
    hectareas: object = 50.0,
    latitud: object = -27.45,
    longitud: object = -58.90,
    volumen_ingresado: object = 100.0,
    volumen_exportar: object = 45.0,
) -> dict[str, object]:
    return {
        "Identificador_Lote": identificador,
        "ID_Proveedor": proveedor,
        "Producto_Forestal": producto,
        "Hectareas": hectareas,
        "Latitud": latitud,
        "Longitud": longitud,
        "Volumen_Ingresado_Ton": volumen_ingresado,
        "Volumen_Exportar_Ton": volumen_exportar,
    }


def _df(*rows: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(
        list(rows),
        columns=BATCH_COLUMNAS,
    )


def _workbook_bytes(
    rows: list[list[object | None]],
) -> bytes:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = BATCH_SHEET_NAME
    worksheet.append(BATCH_COLUMNAS)

    for row_values in rows:
        worksheet.append(row_values)

    buffer = io.BytesIO()
    workbook.save(buffer)
    workbook.close()
    return buffer.getvalue()


def _row_values(
    *,
    identificador: object = "RODAL-001",
    proveedor: object = "30-12345678-9",
    producto: object = "Madera Aserrada (Pino)",
    hectareas: object = 50.0,
    latitud: object = -27.45,
    longitud: object = -58.90,
    volumen_ingresado: object = 100.0,
    volumen_exportar: object = 45.0,
) -> list[object]:
    return [
        identificador,
        proveedor,
        producto,
        hectareas,
        latitud,
        longitud,
        volumen_ingresado,
        volumen_exportar,
    ]


def _codes(result) -> set[tuple[int, str, str]]:
    return {
        (
            error.row,
            error.field,
            error.code,
        )
        for error in result.errors
    }


def test_valid_row_is_normalized_into_canonical_payload():
    result = validar_dataframe_lotes(
        _df(
            _row(
                identificador="  RODAL   NORTE 01 ",
                proveedor="  30-12345678-9 ",
                producto=" Madera Aserrada (Pino) ",
            )
        )
    )

    assert result.valid is True
    assert result.total_rows == 1
    assert result.valid_rows == 1
    assert result.invalid_rows == 0

    canonical = result.canonical_rows[0]

    assert canonical.source_row == 2
    assert canonical.identificador == "RODAL NORTE 01"
    assert canonical.productor_id == "30-12345678-9"
    assert canonical.producto_forestal == "Madera Aserrada (Pino)"
    assert canonical.as_lote_payload()["latitud"] == -27.45


def test_semantic_unicode_and_boundary_inputs_are_handled_as_intended():
    exact_text = "Á" * BATCH_MAX_TEXT_LENGTH
    result = validar_dataframe_lotes(
        _df(
            _row(
                identificador="Ｒｏｄａｌ　Ｎｏｒｔｅ　０１",
                proveedor=exact_text,
                producto="Madera Aserrada (Ñandú)",
                hectareas=1.0,
                latitud=-90.0,
                longitud=-180.0,
            ),
            _row(
                identificador="Rodal Sur",
                proveedor="30-12345678-8",
                producto="Madera Aserrada (Pino)",
                hectareas=1.0,
                latitud=90.0,
                longitud=180.0,
            ),
        )
    )

    assert result.valid is True
    assert result.invalid_rows == 0
    assert result.canonical_rows[0].identificador == "Rodal Norte 01"
    assert result.canonical_rows[0].productor_id == exact_text
    assert result.canonical_rows[0].producto_forestal == "Madera Aserrada (Ñandú)"
    assert result.canonical_rows[0].latitud == -90.0
    assert result.canonical_rows[0].longitud == -180.0
    assert result.canonical_rows[1].latitud == 90.0
    assert result.canonical_rows[1].longitud == 180.0


def test_secure_parser_preserves_original_excel_row_numbers_across_blank_rows():
    payload = _workbook_bytes(
        [
            _row_values(identificador="RODAL-001"),
            [None] * len(BATCH_COLUMNAS),
            _row_values(identificador="RODAL-002"),
        ]
    )

    workbook = parsear_excel_lotes(
        payload,
        filename="batch.xlsx",
    )

    assert workbook.source_row_numbers == (2, 4)

    result = validar_filas_lotes(workbook)

    assert result.valid is True
    assert [
        item.source_row
        for item in result.canonical_rows
    ] == [2, 4]


def test_required_text_fields_report_field_level_errors():
    result = validar_dataframe_lotes(
        _df(
            _row(
                identificador="",
                proveedor=None,
                producto="   ",
            )
        )
    )

    assert result.valid is False
    assert _codes(result) == {
        (2, "Identificador_Lote", "REQUIRED"),
        (2, "ID_Proveedor", "REQUIRED"),
        (2, "Producto_Forestal", "REQUIRED"),
    }


def test_required_numeric_missing_reports_required():
    result = validar_dataframe_lotes(
        _df(
            _row(
                hectareas=None,
            )
        )
    )

    assert (
        2,
        "Hectareas",
        "REQUIRED",
    ) in _codes(result)


def test_text_length_limit_matches_lote_model_contract():
    result = validar_dataframe_lotes(
        _df(
            _row(
                identificador="X" * 101,
            )
        )
    )

    assert (
        2,
        "Identificador_Lote",
        "STRING_TOO_LONG",
    ) in _codes(result)


def test_control_characters_are_rejected_in_text_fields():
    result = validar_dataframe_lotes(
        _df(
            _row(
                proveedor="30-123\n45678-9",
            )
        )
    )

    assert (
        2,
        "ID_Proveedor",
        "CONTROL_CHARACTERS",
    ) in _codes(result)


def test_text_fields_reject_non_string_types():
    result = validar_dataframe_lotes(
        _df(
            _row(
                proveedor=30123456789,
            )
        )
    )

    assert (
        2,
        "ID_Proveedor",
        "INVALID_TYPE",
    ) in _codes(result)


@pytest.mark.parametrize(
    "value",
    [
        "50",
        True,
    ],
)
def test_numeric_fields_do_not_accept_text_or_booleans(value):
    result = validar_dataframe_lotes(
        _df(
            _row(
                hectareas=value,
            )
        )
    )

    assert (
        2,
        "Hectareas",
        "INVALID_TYPE",
    ) in _codes(result)


@pytest.mark.parametrize(
    "value",
    [
        float("nan"),
        float("inf"),
    ],
)
def test_nan_and_infinity_are_rejected(value):
    result = validar_dataframe_lotes(
        _df(
            _row(
                volumen_ingresado=value,
            )
        )
    )

    assert (
        2,
        "Volumen_Ingresado_Ton",
        "NOT_FINITE",
    ) in _codes(result)


def test_hectares_must_be_strictly_positive():
    result = validar_dataframe_lotes(
        _df(
            _row(
                hectareas=0.0,
            )
        )
    )

    assert (
        2,
        "Hectareas",
        "MUST_BE_POSITIVE",
    ) in _codes(result)


@pytest.mark.parametrize(
    ("field", "kwargs"),
    [
        (
            "Latitud",
            {"latitud": 91.0},
        ),
        (
            "Longitud",
            {"longitud": -181.0},
        ),
    ],
)
def test_coordinates_must_be_inside_wgs84_ranges(
    field,
    kwargs,
):
    result = validar_dataframe_lotes(
        _df(
            _row(**kwargs)
        )
    )

    assert (
        2,
        field,
        "OUT_OF_RANGE",
    ) in _codes(result)


@pytest.mark.parametrize(
    ("field", "kwargs"),
    [
        (
            "Volumen_Ingresado_Ton",
            {"volumen_ingresado": -1.0},
        ),
        (
            "Volumen_Exportar_Ton",
            {"volumen_exportar": -1.0},
        ),
    ],
)
def test_volumes_must_be_non_negative(
    field,
    kwargs,
):
    result = validar_dataframe_lotes(
        _df(
            _row(**kwargs)
        )
    )

    assert (
        2,
        field,
        "MUST_BE_NON_NEGATIVE",
    ) in _codes(result)


def test_export_volume_cannot_exceed_input_volume():
    result = validar_dataframe_lotes(
        _df(
            _row(
                volumen_ingresado=100.0,
                volumen_exportar=101.0,
            )
        )
    )

    assert (
        2,
        "Volumen_Exportar_Ton",
        "EXPORT_EXCEEDS_INPUT",
    ) in _codes(result)


def test_duplicate_identifiers_are_rejected_case_insensitively_for_all_rows():
    result = validar_dataframe_lotes(
        _df(
            _row(
                identificador="Rodal Norte",
            ),
            _row(
                identificador="ＲＯＤＡＬ　ＮＯＲＴＥ",
                proveedor="30-22222222-2",
            ),
        )
    )

    assert result.valid is False
    assert result.invalid_rows == 2

    duplicate_errors = [
        error
        for error in result.errors
        if error.code == "DUPLICATE_IN_FILE"
    ]

    assert [error.row for error in duplicate_errors] == [2, 3]
    assert result.canonical_rows == ()


def test_multiple_errors_from_same_row_are_aggregated():
    result = validar_dataframe_lotes(
        _df(
            _row(
                identificador="",
                hectareas=-10.0,
                latitud=999.0,
                volumen_exportar=-5.0,
            )
        )
    )

    row_result = result.rows[0]

    assert row_result.valid is False
    assert row_result.data is None
    assert len(row_result.errors) >= 4


def test_legacy_batch_processor_still_generates_artifact_for_valid_rows():
    summary, zip_bytes = procesar_lote_masivo(
        _df(
            _row(
                identificador="RODAL-APTO-01",
                volumen_ingresado=100.0,
                volumen_exportar=45.0,
            ),
            _row(
                identificador="RODAL-EXCESO-02",
                proveedor="30-22222222-2",
                volumen_ingresado=100.0,
                volumen_exportar=90.0,
            ),
        )
    )

    assert len(summary) == 2
    assert len(zip_bytes) > 0


def test_invalid_batch_processor_fails_closed_without_silent_defaults():
    invalid = _df(
        _row(
            latitud="latitud-invalida",
        )
    )

    with pytest.raises(
        BatchSemanticValidationError
    ) as exc_info:
        procesar_lote_masivo(invalid)

    result = exc_info.value.result

    assert result.valid is False
    assert result.rows[0].data is None
    assert (
        2,
        "Latitud",
        "INVALID_TYPE",
    ) in _codes(result)

    assert "latitud-invalida" not in str(exc_info.value)


def test_dataframe_schema_mismatch_is_a_structural_error():
    invalid_df = pd.DataFrame(
        [{"Identificador_Lote": "RODAL-1"}]
    )

    with pytest.raises(
        BatchExcelValidationError
    ) as exc_info:
        validar_dataframe_lotes(
            invalid_df
        )

    assert exc_info.value.code == "INVALID_HEADERS"
