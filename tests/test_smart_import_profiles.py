from __future__ import annotations

from io import BytesIO

from openpyxl import Workbook

from litoral_trace.services.batch import BATCH_COLUMNAS
from litoral_trace.services.smart_import import SmartImportEngine, default_confirmed_mapping
from litoral_trace.services.smart_import.profiles import (
    candidate_header_signature,
    header_fingerprint,
    profile_mapping_payload,
    resolve_profile_mapping,
)


def _workbook_bytes(headers: list[str]) -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Recepciones"
    sheet.append(["Reporte interno"])
    sheet.append(headers)

    values = {
        "Rodal": "R-001",
        "Productor": "P-001",
        "Especie": "Pino",
        "Sup. ha": 12.0,
        "LAT": -27.4,
        "LONG": -58.8,
        "Tn recibidas": 30.0,
        "Stock exportable": 18.0,
        "Fecha": "2026-08-24",
        "Patente": "AB123CD",
        "Observaciones": "OK",
    }
    sheet.append([values.get(header, "extra") for header in headers])

    buffer = BytesIO()
    workbook.save(buffer)
    workbook.close()
    return buffer.getvalue()


def _candidate(headers: list[str]):
    analysis = SmartImportEngine().analyze(
        _workbook_bytes(headers),
        filename="cliente.xlsx",
    )
    candidate = analysis.best_candidate
    assert candidate is not None
    return candidate


def test_header_fingerprint_is_stable_when_columns_reorder() -> None:
    first = _candidate(
        [
            "Rodal",
            "Productor",
            "Especie",
            "Sup. ha",
            "LAT",
            "LONG",
            "Tn recibidas",
            "Stock exportable",
            "Fecha",
        ]
    )
    reordered = _candidate(
        [
            "Fecha",
            "Stock exportable",
            "LONG",
            "Rodal",
            "LAT",
            "Especie",
            "Tn recibidas",
            "Productor",
            "Sup. ha",
        ]
    )

    assert header_fingerprint(candidate_header_signature(first)) == header_fingerprint(
        candidate_header_signature(reordered)
    )


def test_remembered_mapping_resolves_by_header_not_saved_column_index() -> None:
    original = _candidate(
        [
            "Rodal",
            "Productor",
            "Especie",
            "Sup. ha",
            "LAT",
            "LONG",
            "Tn recibidas",
            "Stock exportable",
        ]
    )
    saved = profile_mapping_payload(
        original,
        default_confirmed_mapping(original),
    )

    reordered = _candidate(
        [
            "Stock exportable",
            "LAT",
            "Rodal",
            "LONG",
            "Productor",
            "Tn recibidas",
            "Especie",
            "Sup. ha",
            "Observaciones",
        ]
    )
    resolved, missing = resolve_profile_mapping(saved, reordered)

    assert missing == ()
    assert {item.canonical_field for item in resolved} == set(BATCH_COLUMNAS)
    assert {
        item.canonical_field: item.source_column for item in resolved
    }["Identificador_Lote"] == "Rodal"
    assert {
        item.canonical_field: item.source_index for item in resolved
    }["Identificador_Lote"] == 2


def test_profile_resolution_blocks_when_required_source_header_disappears() -> None:
    original = _candidate(
        [
            "Rodal",
            "Productor",
            "Especie",
            "Sup. ha",
            "LAT",
            "LONG",
            "Tn recibidas",
            "Stock exportable",
        ]
    )
    saved = profile_mapping_payload(
        original,
        default_confirmed_mapping(original),
    )

    changed = _candidate(
        [
            "Rodal",
            "Productor",
            "Especie",
            "Sup. ha",
            "LAT",
            "LONG",
            "Tn recibidas",
            "Observaciones",
        ]
    )
    resolved, missing = resolve_profile_mapping(saved, changed)

    assert len(resolved) == 7
    assert "stock exportable" in missing
