from __future__ import annotations

from io import BytesIO

from openpyxl import Workbook

from litoral_trace.services.smart_import import MappingStatus, SmartImportEngine
from litoral_trace.services.smart_import.matcher import map_source_column
from litoral_trace.services.smart_import.normalize import normalize_header


def _xlsx_bytes(builder) -> bytes:
    workbook = Workbook()
    builder(workbook)
    buffer = BytesIO()
    workbook.save(buffer)
    workbook.close()
    return buffer.getvalue()


def test_normalize_header_is_case_accent_and_punctuation_insensitive() -> None:
    assert normalize_header("  SUPERFÍCIE (Ha) ") == "superficie ha"
    assert normalize_header("Nro. Rodal") == "numero rodal"


def test_mapper_recognizes_common_business_aliases() -> None:
    mapping = map_source_column(
        "Sup. (ha)",
        [12.5, 20.0, 4.75],
        source_index=3,
    )

    assert mapping.decision.canonical_field == "Hectareas"
    assert mapping.decision.status in {MappingStatus.AUTO, MappingStatus.CONFIRM}
    assert mapping.decision.confidence >= 0.78


def test_engine_discovers_nonstandard_sheet_header_and_extra_columns() -> None:
    def build(workbook: Workbook) -> None:
        default = workbook.active
        default.title = "Resumen"
        default.append(["FORESTAL XYZ SA"])
        default.append(["Reporte agosto 2026"])

        sheet = workbook.create_sheet("Recepciones Agosto")
        sheet.append(["FORESTAL XYZ SA"])
        sheet.append(["Control de planta"])
        sheet.append([])
        sheet.append(
            [
                "Fecha",
                "Rodal",
                "Productor",
                "Especie",
                "Sup. (ha)",
                "LAT",
                "LONG",
                "Tn recibidas",
                "Stock exportable",
                "Patente",
                "Chofer",
                "Observaciones",
            ]
        )
        sheet.append(
            [
                "2026-08-01",
                "R-001",
                "Forestal Perez",
                "Pinus taeda",
                34.5,
                -27.42,
                -58.81,
                80.0,
                40.0,
                "AB123CD",
                "Juan Perez",
                "OK",
            ]
        )
        sheet.append(
            [
                "2026-08-02",
                "R-002",
                "Forestal Gomez",
                "Eucalyptus grandis",
                44.0,
                -27.50,
                -58.90,
                90.0,
                55.0,
                "AC456EF",
                "Ana Gomez",
                "OK",
            ]
        )

    analysis = SmartImportEngine().analyze(
        _xlsx_bytes(build),
        filename="operaciones_agosto.xlsx",
    )

    best = analysis.best_candidate
    assert best is not None
    assert best.sheet_name == "Recepciones Agosto"
    assert best.header_row == 4
    assert best.estimated_columns == 12

    mapped = {
        mapping.decision.canonical_field
        for mapping in best.mappings
        if mapping.decision.canonical_field is not None
    }
    assert {
        "Identificador_Lote",
        "ID_Proveedor",
        "Producto_Forestal",
        "Hectareas",
        "Latitud",
        "Longitud",
        "Volumen_Ingresado_Ton",
        "Volumen_Exportar_Ton",
    }.issubset(mapped)

    ignored = {
        mapping.source_column
        for mapping in best.mappings
        if mapping.decision.status == MappingStatus.IGNORED
    }
    assert {"Fecha", "Patente", "Chofer", "Observaciones"}.issubset(ignored)


def test_engine_keeps_official_template_as_fast_compatible_candidate() -> None:
    def build(workbook: Workbook) -> None:
        sheet = workbook.active
        sheet.title = "Plantilla_LitoralTrace"
        sheet.append(
            [
                "Identificador_Lote",
                "ID_Proveedor",
                "Producto_Forestal",
                "Hectareas",
                "Latitud",
                "Longitud",
                "Volumen_Ingresado_Ton",
                "Volumen_Exportar_Ton",
            ]
        )
        sheet.append(
            [
                "R-001",
                "CUIT-30123456789",
                "Madera Aserrada (Eucalipto)",
                120.0,
                -27.50,
                -58.90,
                500.0,
                200.0,
            ]
        )

    analysis = SmartImportEngine().analyze(
        _xlsx_bytes(build),
        filename="plantilla.xlsx",
    )
    best = analysis.best_candidate

    assert best is not None
    assert best.sheet_name == "Plantilla_LitoralTrace"
    assert best.header_row == 1
    assert best.missing_required_fields == ()
    assert len(best.mapped_fields) == 8


def test_engine_does_not_invent_missing_required_fields() -> None:
    def build(workbook: Workbook) -> None:
        sheet = workbook.active
        sheet.title = "Datos"
        sheet.append(["Rodal", "Productor", "Especie", "Sup. ha", "Tn recibidas"])
        sheet.append(["R-1", "P-1", "Pino", 10, 22])

    analysis = SmartImportEngine().analyze(
        _xlsx_bytes(build),
        filename="incompleto.xlsx",
    )
    best = analysis.best_candidate

    assert best is not None
    assert "Latitud" in best.missing_required_fields
    assert "Longitud" in best.missing_required_fields
    assert "Volumen_Exportar_Ton" in best.missing_required_fields
