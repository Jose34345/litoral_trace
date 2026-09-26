from __future__ import annotations

from litoral_trace.us_lacey.canonical_shipment_truth import (
    CanonicalTruthState,
    build_canonical_shipment_truth,
)


def _evidence(
    *,
    field: str,
    value: str,
    document_id: int,
    line_key: str | None = None,
    component_key: str | None = None,
    text: str,
    table_id: str | None = None,
    row_index: int | None = None,
    key_text: str | None = None,
    value_text: str | None = None,
    authority: float = 20.0,
    score: float = 90.0,
) -> dict:
    block_id = (
        f"{table_id}-r{row_index}-c2"
        if table_id is not None and row_index is not None
        else f"free:{document_id}:{field}:{value}"
    )
    source_block = {
        "block_id": block_id,
        "table_id": table_id,
        "row_index": row_index,
        "key_text": key_text,
        "table_header": key_text,
        "value_text": value_text,
        "text": text,
        "page": 1,
    }
    return {
        "candidate_id": f"{document_id}:{field}:{line_key or component_key}:{value}:{block_id}",
        "document_id": str(document_id),
        "field_key": field,
        "normalized_value": value,
        "candidate_score": score,
        "source_authority": authority,
        "scope": "MERCHANDISE_LINE" if line_key else "PLANT_COMPONENT",
        "line_key": line_key,
        "component_key": component_key,
        "quantity_semantic_type": "PLANT_MATERIAL_QUANTITY" if field == "plant_quantity" else "OTHER",
        "candidate": {
            "score": score,
            "raw": {
                "field_key": field,
                "normalized_value": value,
                "evidence_class": "EXPLICIT",
                "source_block": source_block,
            },
            "provenance": {
                "page": 1,
                "block_id": block_id,
                "source_text": text,
                "evidence_class": "EXPLICIT",
            },
        },
    }


def _field(field: str, state: str, rows: list[dict]) -> dict:
    values = [] if state in {"MISSING", "REVIEW_REQUIRED"} else [
        {"value": value, "evidence_ids": []}
        for value in dict.fromkeys(row["normalized_value"] for row in rows)
    ]
    return {
        "field_key": field,
        "state": state,
        "values": values,
        "supporting_evidence": rows,
    }


def _payload() -> dict:
    invoice_1 = "176:p1-t2:row:1"
    invoice_2 = "176:p1-t2:row:2"
    entry_1 = "181:p1-t2:row:1"
    entry_2 = "181:p1-t2:row:2"
    pinus = "taxon:pinus:taeda"
    eucalyptus = "taxon:eucalyptus:grandis"

    hts = [
        _evidence(field="hts_code", value="4407110190", document_id=176, line_key=invoice_1, text="HTS: 4407.11.0190", table_id="p1-t2", row_index=1),
        _evidence(field="hts_code", value="4407990190", document_id=176, line_key=invoice_2, text="HTS: 4407.99.0190", table_id="p1-t2", row_index=2),
        _evidence(field="hts_code", value="4407110190", document_id=181, line_key=entry_1, text="HTS: 4407.11.0190", table_id="p1-t2", row_index=1),
        _evidence(field="hts_code", value="4407990190", document_id=181, line_key=entry_2, text="HTS: 4407.99.0190", table_id="p1-t2", row_index=2),
    ]
    entered = [
        _evidence(field="entered_value", value="18300", document_id=181, line_key=entry_1, text="Entered Value: USD 18,300.00", table_id="p1-t2", row_index=1),
        _evidence(field="entered_value", value="12640", document_id=181, line_key=entry_2, text="Entered Value: USD 12,640.00", table_id="p1-t2", row_index=2),
    ]

    genus = []
    species = []
    for document_id, row_1, row_2 in (
        (176, invoice_1, invoice_2),
        (181, entry_1, entry_2),
    ):
        genus.extend(
            [
                _evidence(
                    field="genus",
                    value="Pinus",
                    document_id=document_id,
                    component_key=pinus,
                    text="Description: Pinus taeda KD sawn boards",
                    table_id="p1-t2",
                    row_index=1,
                    key_text="Description",
                    value_text="Pinus taeda KD sawn boards",
                ),
                _evidence(
                    field="genus",
                    value="Eucalyptus",
                    document_id=document_id,
                    component_key=eucalyptus,
                    text="Description: Eucalyptus grandis KD sawn boards",
                    table_id="p1-t2",
                    row_index=2,
                    key_text="Description",
                    value_text="Eucalyptus grandis KD sawn boards",
                ),
            ]
        )
        species.extend(
            [
                _evidence(
                    field="species",
                    value="taeda",
                    document_id=document_id,
                    component_key=pinus,
                    text="Description: Pinus taeda KD sawn boards",
                    table_id="p1-t2",
                    row_index=1,
                    key_text="Description",
                    value_text="Pinus taeda KD sawn boards",
                ),
                _evidence(
                    field="species",
                    value="grandis",
                    document_id=document_id,
                    component_key=eucalyptus,
                    text="Description: Eucalyptus grandis KD sawn boards",
                    table_id="p1-t2",
                    row_index=2,
                    key_text="Description",
                    value_text="Eucalyptus grandis KD sawn boards",
                ),
            ]
        )

    harvest = [
        _evidence(field="country_of_harvest", value="Brasil", document_id=180, component_key=pinus, text="Pinus taeda - pais de colheita Brasil", authority=5.0),
        _evidence(field="country_of_harvest", value="Brasil", document_id=180, component_key=eucalyptus, text="Eucalyptus grandis - pais de colheita Brasil", authority=5.0),
    ]
    quantities = [
        _evidence(field="plant_quantity", value="30.000", document_id=181, component_key=entry_1, text="Qty: 30.000 m3", table_id="p1-t2", row_index=1),
        _evidence(field="plant_quantity", value="16.000", document_id=181, component_key=entry_2, text="Qty: 16.000 m3", table_id="p1-t2", row_index=2),
    ]
    units = [
        _evidence(field="metric_unit", value="m3", document_id=181, component_key=entry_1, text="Qty: 30.000 m3", table_id="p1-t2", row_index=1),
        _evidence(field="metric_unit", value="m3", document_id=181, component_key=entry_2, text="Qty: 16.000 m3", table_id="p1-t2", row_index=2),
    ]

    return {
        "schema_version": "lacey_shipment_resolution_v1",
        "engine_version": "lacey-engine-2.3.0",
        "canonical_fields": {
            "description": _field("description", "MISSING", []),
            "hts_code": _field("hts_code", "SUPPORTED_MULTIPLE", hts),
            "entered_value": _field("entered_value", "SUPPORTED_MULTIPLE", entered),
            "genus": _field("genus", "SUPPORTED_MULTIPLE", genus),
            "species": _field("species", "SUPPORTED_MULTIPLE", species),
            "country_of_harvest": _field("country_of_harvest", "REVIEW_REQUIRED", harvest),
            "plant_quantity": _field("plant_quantity", "SUPPORTED_MULTIPLE", quantities),
            "metric_unit": _field("metric_unit", "SUPPORTED_MULTIPLE", units),
        },
        "issues": [
            {
                "field_key": "country_of_harvest",
                "issue_type": "LOW_AUTHORITY_ONLY",
                "requires_human_review": True,
            }
        ],
    }


def test_cross_document_rows_collapse_to_two_canonical_product_lines():
    truth = build_canonical_shipment_truth(_payload())

    assert len(truth.plant_lines) == 2
    assert truth.unresolved_component_keys == ()

    pinus, eucalyptus = truth.plant_lines
    assert pinus.ordinal_hint == 1
    assert pinus.taxon_key == "taxon:pinus:taeda"
    assert pinus.fields["hts_code"].values == ("4407110190",)
    assert pinus.fields["entered_value"].values == ("18300",)
    assert pinus.fields["genus"].values == ("Pinus",)
    assert pinus.fields["species"].values == ("taeda",)
    assert pinus.fields["plant_quantity"].values == ("30.000",)
    assert pinus.fields["metric_unit"].values == ("m3",)
    assert pinus.fields["country_of_harvest"].values == ("Brasil",)
    assert pinus.fields["country_of_harvest"].state is CanonicalTruthState.REVIEW_REQUIRED
    # Cross-document rows share one canonical line identity while retaining
    # independent source-document provenance on every evidence item.
    assert {row.line_key for row in pinus.fields["hts_code"].evidence} == {
        "176:p1-t2:row:1"
    }
    assert {row.document_id for row in pinus.fields["hts_code"].evidence} == {"176", "181"}
    assert len(pinus.fields["hts_code"].evidence) == 2

    assert eucalyptus.ordinal_hint == 2
    assert eucalyptus.taxon_key == "taxon:eucalyptus:grandis"
    assert eucalyptus.fields["hts_code"].values == ("4407990190",)
    assert eucalyptus.fields["entered_value"].values == ("12640",)
    assert eucalyptus.fields["genus"].values == ("Eucalyptus",)
    assert eucalyptus.fields["species"].values == ("grandis",)
    assert eucalyptus.fields["plant_quantity"].values == ("16.000",)
    assert eucalyptus.fields["metric_unit"].values == ("m3",)
    assert eucalyptus.fields["country_of_harvest"].values == ("Brasil",)
    assert eucalyptus.fields["country_of_harvest"].state is CanonicalTruthState.REVIEW_REQUIRED
    assert {row.document_id for row in eucalyptus.fields["hts_code"].evidence} == {"176", "181"}
    assert len(eucalyptus.fields["hts_code"].evidence) == 2

    description = truth.shipment_fields["merchandise_description"]
    assert description.values == (
        "Pinus taeda KD sawn boards; Eucalyptus grandis KD sawn boards",
    )

def _description_rows_for_authority_regression(*, entry_authority: float = 30.0) -> list[dict]:
    return [
        _evidence(
            field="description",
            value="Tablas secas / KD boards - Pinus taeda",
            document_id=176,
            line_key="176:p1-t2:row:1",
            text="Description: Tablas secas / KD boards - Pinus taeda",
            table_id="p1-t2",
            row_index=1,
            key_text="Description",
            value_text="Tablas secas / KD boards - Pinus taeda",
            authority=40.0,
        ),
        _evidence(
            field="description",
            value="Tablas secas / KD boards - Eucalyptus grandis",
            document_id=176,
            line_key="176:p1-t2:row:2",
            text="Description: Tablas secas / KD boards - Eucalyptus grandis",
            table_id="p1-t2",
            row_index=2,
            key_text="Description",
            value_text="Tablas secas / KD boards - Eucalyptus grandis",
            authority=40.0,
        ),
        _evidence(
            field="description",
            value="Pinus taeda KD sawn boards",
            document_id=181,
            line_key="181:p1-t2:row:1",
            text="Description: Pinus taeda KD sawn boards",
            table_id="p1-t2",
            row_index=1,
            key_text="Description",
            value_text="Pinus taeda KD sawn boards",
            authority=entry_authority,
        ),
        _evidence(
            field="description",
            value="Eucalyptus grandis KD sawn boards",
            document_id=181,
            line_key="181:p1-t2:row:2",
            text="Description: Eucalyptus grandis KD sawn boards",
            table_id="p1-t2",
            row_index=2,
            key_text="Description",
            value_text="Eucalyptus grandis KD sawn boards",
            authority=entry_authority,
        ),
    ]


def test_cross_document_description_uses_unique_highest_authority_wording_per_line():
    payload = _payload()
    payload["canonical_fields"]["description"] = _field(
        "description",
        "SUPPORTED_MULTIPLE",
        _description_rows_for_authority_regression(),
    )

    truth = build_canonical_shipment_truth(payload)

    pinus, eucalyptus = truth.plant_lines
    assert pinus.fields["merchandise_description"].values == (
        "Tablas secas / KD boards - Pinus taeda",
    )
    assert eucalyptus.fields["merchandise_description"].values == (
        "Tablas secas / KD boards - Eucalyptus grandis",
    )
    assert {row.document_id for row in pinus.fields["merchandise_description"].evidence} == {"176"}
    assert {row.document_id for row in eucalyptus.fields["merchandise_description"].evidence} == {"176"}
    assert truth.shipment_fields["merchandise_description"].values == (
        "Tablas secas / KD boards - Pinus taeda; "
        "Tablas secas / KD boards - Eucalyptus grandis",
    )


def test_cross_document_description_equal_authority_disagreement_fails_closed():
    payload = _payload()
    payload["canonical_fields"]["description"] = _field(
        "description",
        "SUPPORTED_MULTIPLE",
        _description_rows_for_authority_regression(entry_authority=40.0),
    )

    truth = build_canonical_shipment_truth(payload)

    assert all(
        line.fields["merchandise_description"].state is CanonicalTruthState.CONFLICT
        for line in truth.plant_lines
    )
    assert "merchandise_description" not in truth.shipment_fields

