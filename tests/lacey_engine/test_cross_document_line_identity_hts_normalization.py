from __future__ import annotations

from litoral_trace.us_lacey.cross_document_line_identity import (
    reconcile_cross_document_line_identity,
)


def _row(
    *,
    document_id: int,
    field: str,
    value: str,
    line_key: str | None = None,
    component_key: str | None = None,
    row_index: int,
) -> dict:
    source_block = {
        "table_id": "p1-t2",
        "row_index": row_index,
        "key_text": "HTS" if field == "hts_code" else "Description",
        "table_header": "HTS" if field == "hts_code" else "Description",
        "value_text": value,
        "text": value,
    }
    return {
        "candidate_id": f"{document_id}:{field}:{row_index}:{value}",
        "document_id": str(document_id),
        "field_key": field,
        "normalized_value": value,
        "candidate_score": 90.0,
        "source_authority": 20.0,
        "line_key": line_key,
        "component_key": component_key,
        "candidate": {
            "score": 90.0,
            "raw": {
                "field_key": field,
                "normalized_value": value,
                "source_block": source_block,
            },
            "provenance": {"source_text": value},
        },
    }


def _field(key: str, rows: list[dict]) -> dict:
    return {
        "field_key": key,
        "state": "SUPPORTED_MULTIPLE",
        "values": [],
        "supporting_evidence": rows,
    }


def test_equivalent_hts_formatting_collapses_cross_document_product_lines() -> None:
    invoice_1 = "183:p1-t2:row:1"
    invoice_2 = "183:p1-t2:row:2"
    entry_1 = "188:p1-t2:row:1"
    entry_2 = "188:p1-t2:row:2"
    pinus = "taxon:pinus:taeda"
    eucalyptus = "taxon:eucalyptus:grandis"

    payload = {
        "canonical_fields": {
            "hts_code": _field(
                "hts_code",
                [
                    _row(
                        document_id=183,
                        field="hts_code",
                        value="4407.11.019 0",
                        line_key=invoice_1,
                        row_index=1,
                    ),
                    _row(
                        document_id=183,
                        field="hts_code",
                        value="4407.99.019 0",
                        line_key=invoice_2,
                        row_index=2,
                    ),
                    _row(
                        document_id=188,
                        field="hts_code",
                        value="4407.11.0190",
                        line_key=entry_1,
                        row_index=1,
                    ),
                    _row(
                        document_id=188,
                        field="hts_code",
                        value="4407.99.0190",
                        line_key=entry_2,
                        row_index=2,
                    ),
                ],
            ),
            "genus": _field(
                "genus",
                [
                    _row(document_id=183, field="genus", value="Pinus", component_key=pinus, row_index=1),
                    _row(document_id=183, field="genus", value="Eucalyptus", component_key=eucalyptus, row_index=2),
                    _row(document_id=188, field="genus", value="Pinus", component_key=pinus, row_index=1),
                    _row(document_id=188, field="genus", value="Eucalyptus", component_key=eucalyptus, row_index=2),
                ],
            ),
        }
    }

    reconciled = reconcile_cross_document_line_identity(payload)
    rows = reconciled["canonical_fields"]["hts_code"]["supporting_evidence"]

    assert {row["line_key"] for row in rows} == {invoice_1, invoice_2}
    assert [row["line_key"] for row in rows].count(invoice_1) == 2
    assert [row["line_key"] for row in rows].count(invoice_2) == 2
