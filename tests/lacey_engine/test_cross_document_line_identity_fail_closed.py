from __future__ import annotations

from litoral_trace.us_lacey.cross_document_line_identity import (
    reconcile_cross_document_line_identity,
)


def _row(*, document: int, row: int, field: str, value: str, component: str | None = None):
    line_key = f"{document}:p1-t1:row:{row}"
    return {
        "candidate_id": f"{document}:{row}:{field}:{value}",
        "document_id": str(document),
        "field_key": field,
        "normalized_value": value,
        "source_authority": 20.0,
        "candidate_score": 90.0,
        "line_key": line_key if field == "hts_code" else None,
        "component_key": component,
        "candidate": {
            "score": 90.0,
            "raw": {
                "field_key": field,
                "normalized_value": value,
                "source_block": {
                    "table_id": "p1-t1",
                    "row_index": row,
                    "key_text": "Description" if field in {"genus", "species"} else field,
                    "table_header": "Description" if field in {"genus", "species"} else field,
                    "value_text": "Pinus taeda boards" if field in {"genus", "species"} else value,
                    "text": "Description: Pinus taeda boards" if field in {"genus", "species"} else value,
                    "page": 1,
                },
            },
            "provenance": {"page": 1, "source_text": value},
        },
    }


def test_same_document_duplicate_signature_is_not_collapsed_across_documents():
    taxon = "taxon:pinus:taeda"
    hts_rows = [
        _row(document=10, row=1, field="hts_code", value="4407110190"),
        _row(document=10, row=2, field="hts_code", value="4407110190"),
        _row(document=20, row=1, field="hts_code", value="4407110190"),
    ]
    genus_rows = [
        _row(document=10, row=1, field="genus", value="Pinus", component=taxon),
        _row(document=10, row=2, field="genus", value="Pinus", component=taxon),
        _row(document=20, row=1, field="genus", value="Pinus", component=taxon),
    ]
    species_rows = [
        _row(document=10, row=1, field="species", value="taeda", component=taxon),
        _row(document=10, row=2, field="species", value="taeda", component=taxon),
        _row(document=20, row=1, field="species", value="taeda", component=taxon),
    ]
    payload = {
        "canonical_fields": {
            "description": {"field_key": "description", "state": "MISSING", "values": [], "supporting_evidence": []},
            "hts_code": {"field_key": "hts_code", "state": "SUPPORTED_MULTIPLE", "values": [], "supporting_evidence": hts_rows},
            "genus": {"field_key": "genus", "state": "SUPPORTED_MULTIPLE", "values": [], "supporting_evidence": genus_rows},
            "species": {"field_key": "species", "state": "SUPPORTED_MULTIPLE", "values": [], "supporting_evidence": species_rows},
        },
        "issues": [],
    }

    reconciled = reconcile_cross_document_line_identity(payload)

    assert [row["line_key"] for row in reconciled["canonical_fields"]["hts_code"]["supporting_evidence"]] == [
        "10:p1-t1:row:1",
        "10:p1-t1:row:2",
        "20:p1-t1:row:1",
    ]
    assert reconciled["canonical_fields"]["description"]["state"] == "MISSING"
