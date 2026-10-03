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
    text: str,
    line_key: str | None = None,
    component_key: str | None = None,
    table_id: str | None = None,
    row_index: int | None = None,
    key_text: str | None = None,
    value_text: str | None = None,
    scope: str | None = None,
) -> dict:
    block_id = (
        f"{table_id}-r{row_index}-{field}"
        if table_id is not None and row_index is not None
        else f"free:{document_id}:{field}:{value}"
    )
    return {
        "candidate_id": f"{document_id}:{field}:{line_key or component_key}:{value}:{block_id}",
        "document_id": str(document_id),
        "field_key": field,
        "normalized_value": value,
        "candidate_score": 95.0,
        "source_authority": 20.0,
        "scope": scope or ("MERCHANDISE_LINE" if line_key else "PLANT_COMPONENT"),
        "line_key": line_key,
        "component_key": component_key,
        "quantity_semantic_type": (
            "PLANT_MATERIAL_QUANTITY" if field == "plant_quantity" else "OTHER"
        ),
        "candidate": {
            "score": 95.0,
            "raw": {
                "field_key": field,
                "normalized_value": value,
                "evidence_class": "EXPLICIT",
                "source_block": {
                    "block_id": block_id,
                    "table_id": table_id,
                    "row_index": row_index,
                    "key_text": key_text,
                    "table_header": key_text,
                    "value_text": value_text,
                    "text": text,
                    "page": 1,
                },
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
    values = (
        []
        if state in {"MISSING", "REVIEW_REQUIRED"}
        else [
            {"value": value, "evidence_ids": []}
            for value in dict.fromkeys(row["normalized_value"] for row in rows)
        ]
    )
    return {
        "field_key": field,
        "state": state,
        "values": values,
        "supporting_evidence": rows,
    }


def _payload(fields: dict[str, dict], *, issues: list[dict] | None = None) -> dict:
    return {
        "schema_version": "lacey_shipment_resolution_v1",
        "engine_version": "p1-02-test",
        "canonical_fields": fields,
        "issues": issues or [],
    }


def test_same_hts_two_taxa_do_not_bind_orphan_quantities_without_sku_or_taxon_signature() -> None:
    """Case A: HTS alone is never enough to assign quantitative evidence."""
    line_a = "10:invoice:row:1"
    line_b = "10:invoice:row:2"
    pinus = "taxon:pinus:taeda"
    eucalyptus = "taxon:eucalyptus:grandis"

    payload = _payload(
        {
            "description": _field(
                "description",
                "SUPPORTED_MULTIPLE",
                [
                    _evidence(
                        field="description",
                        value="Pinus taeda KD boards",
                        document_id=10,
                        text="Pinus taeda KD boards",
                        line_key=line_a,
                    ),
                    _evidence(
                        field="description",
                        value="Eucalyptus grandis KD boards",
                        document_id=10,
                        text="Eucalyptus grandis KD boards",
                        line_key=line_b,
                    ),
                ],
            ),
            "hts_code": _field(
                "hts_code",
                "SUPPORTED_MULTIPLE",
                [
                    _evidence(
                        field="hts_code",
                        value="4407110190",
                        document_id=10,
                        text="4407.11.0190",
                        line_key=line_a,
                    ),
                    _evidence(
                        field="hts_code",
                        value="4407110190",
                        document_id=10,
                        text="4407.11.0190",
                        line_key=line_b,
                    ),
                ],
            ),
            "genus": _field(
                "genus",
                "SUPPORTED_MULTIPLE",
                [
                    _evidence(
                        field="genus",
                        value="Pinus",
                        document_id=20,
                        text="Pinus taeda",
                        component_key=pinus,
                    ),
                    _evidence(
                        field="genus",
                        value="Eucalyptus",
                        document_id=20,
                        text="Eucalyptus grandis",
                        component_key=eucalyptus,
                    ),
                ],
            ),
            "species": _field(
                "species",
                "SUPPORTED_MULTIPLE",
                [
                    _evidence(
                        field="species",
                        value="Pinus taeda",
                        document_id=20,
                        text="Pinus taeda",
                        component_key=pinus,
                    ),
                    _evidence(
                        field="species",
                        value="Eucalyptus grandis",
                        document_id=20,
                        text="Eucalyptus grandis",
                        component_key=eucalyptus,
                    ),
                ],
            ),
            "plant_quantity": _field(
                "plant_quantity",
                "SUPPORTED_MULTIPLE",
                [
                    _evidence(
                        field="plant_quantity",
                        value="30",
                        document_id=30,
                        text="HTS 4407.11.0190 quantity 30 m3",
                        component_key="orphan:quantity:1",
                    ),
                    _evidence(
                        field="plant_quantity",
                        value="16",
                        document_id=30,
                        text="HTS 4407.11.0190 quantity 16 m3",
                        component_key="orphan:quantity:2",
                    ),
                ],
            ),
            "metric_unit": _field(
                "metric_unit",
                "SUPPORTED_MULTIPLE",
                [
                    _evidence(
                        field="metric_unit",
                        value="m3",
                        document_id=30,
                        text="HTS 4407.11.0190 quantity 30 m3",
                        component_key="orphan:quantity:1",
                    ),
                    _evidence(
                        field="metric_unit",
                        value="m3",
                        document_id=30,
                        text="HTS 4407.11.0190 quantity 16 m3",
                        component_key="orphan:quantity:2",
                    ),
                ],
            ),
        }
    )

    truth = build_canonical_shipment_truth(payload)

    assert len(truth.plant_lines) == 2
    assert {line.taxon_key for line in truth.plant_lines} == {pinus, eucalyptus}
    assert all("plant_quantity" not in line.fields for line in truth.plant_lines)
    assert all("metric_unit" not in line.fields for line in truth.plant_lines)


def test_explicit_sku_binds_quantity_and_unit_without_table_row_sidecar() -> None:
    sku = "SKU:PT-38"
    pinus = "taxon:pinus:taeda"
    payload = _payload(
        {
            "description": _field(
                "description",
                "SUPPORTED",
                [
                    _evidence(
                        field="description",
                        value="Pinus taeda KD boards",
                        document_id=10,
                        text="SKU PT-38 Pinus taeda KD boards",
                        line_key=sku,
                    )
                ],
            ),
            "hts_code": _field(
                "hts_code",
                "SUPPORTED",
                [
                    _evidence(
                        field="hts_code",
                        value="4407110190",
                        document_id=10,
                        text="SKU PT-38 HTS 4407.11.0190",
                        line_key=sku,
                    )
                ],
            ),
            "genus": _field(
                "genus",
                "SUPPORTED",
                [
                    _evidence(
                        field="genus",
                        value="Pinus",
                        document_id=20,
                        text="Pinus taeda",
                        component_key=pinus,
                    )
                ],
            ),
            "species": _field(
                "species",
                "SUPPORTED",
                [
                    _evidence(
                        field="species",
                        value="Pinus taeda",
                        document_id=20,
                        text="Pinus taeda",
                        component_key=pinus,
                    )
                ],
            ),
            "plant_quantity": _field(
                "plant_quantity",
                "SUPPORTED",
                [
                    _evidence(
                        field="plant_quantity",
                        value="30",
                        document_id=30,
                        text="SKU PT-38 Plant Quantity 30 m3",
                        component_key=sku,
                    )
                ],
            ),
            "metric_unit": _field(
                "metric_unit",
                "SUPPORTED",
                [
                    _evidence(
                        field="metric_unit",
                        value="m3",
                        document_id=30,
                        text="SKU PT-38 Plant Quantity 30 m3",
                        component_key=sku,
                    )
                ],
            ),
        }
    )

    truth = build_canonical_shipment_truth(payload)

    assert len(truth.plant_lines) == 1
    line = truth.plant_lines[0]
    assert line.entity_key == sku
    assert line.fields["plant_quantity"].values == ("30",)
    assert line.fields["metric_unit"].values == ("m3",)


def test_exact_hts_taxon_signature_binds_quantity_and_unit() -> None:
    invoice_line = "10:invoice:row:1"
    declaration_line = "20:botanical:row:7"
    pinus = "taxon:pinus:taeda"

    payload = _payload(
        {
            "description": _field(
                "description",
                "SUPPORTED",
                [
                    _evidence(
                        field="description",
                        value="Pinus taeda KD boards",
                        document_id=10,
                        text="Pinus taeda KD boards",
                        line_key=invoice_line,
                        table_id="invoice",
                        row_index=1,
                    )
                ],
            ),
            "hts_code": _field(
                "hts_code",
                "SUPPORTED_MULTIPLE",
                [
                    _evidence(
                        field="hts_code",
                        value="4407110190",
                        document_id=10,
                        text="4407.11.0190",
                        line_key=invoice_line,
                        table_id="invoice",
                        row_index=1,
                    ),
                    _evidence(
                        field="hts_code",
                        value="4407110190",
                        document_id=20,
                        text="4407.11.0190",
                        line_key=declaration_line,
                        table_id="botanical",
                        row_index=7,
                    ),
                ],
            ),
            "genus": _field(
                "genus",
                "SUPPORTED",
                [
                    _evidence(
                        field="genus",
                        value="Pinus",
                        document_id=20,
                        text="Pinus taeda 4407.11.0190",
                        component_key=pinus,
                        table_id="botanical",
                        row_index=7,
                    )
                ],
            ),
            "species": _field(
                "species",
                "SUPPORTED",
                [
                    _evidence(
                        field="species",
                        value="Pinus taeda",
                        document_id=20,
                        text="Pinus taeda 4407.11.0190",
                        component_key=pinus,
                        table_id="botanical",
                        row_index=7,
                    )
                ],
            ),
            "plant_quantity": _field(
                "plant_quantity",
                "SUPPORTED",
                [
                    _evidence(
                        field="plant_quantity",
                        value="30",
                        document_id=20,
                        text="Pinus taeda 4407.11.0190 30 m3",
                        component_key="local:quantity",
                        table_id="botanical",
                        row_index=7,
                    )
                ],
            ),
            "metric_unit": _field(
                "metric_unit",
                "SUPPORTED",
                [
                    _evidence(
                        field="metric_unit",
                        value="m3",
                        document_id=20,
                        text="Pinus taeda 4407.11.0190 30 m3",
                        component_key="local:quantity",
                        table_id="botanical",
                        row_index=7,
                    )
                ],
            ),
        }
    )

    truth = build_canonical_shipment_truth(payload)

    assert len(truth.plant_lines) == 1
    line = truth.plant_lines[0]
    assert line.taxon_key == pinus
    assert line.fields["plant_quantity"].values == ("30",)
    assert line.fields["metric_unit"].values == ("m3",)


def test_single_declarable_line_uses_unique_shipment_total_as_review_required_entered_value() -> None:
    """Case B: shipment total can seed one line, but never as auto-supported truth."""
    line_key = "SKU:ONLY-1"
    taxon = "taxon:eucalyptus:grandis"
    total = _evidence(
        field="shipment_total_entered_value",
        value="19800",
        document_id=40,
        text="Shipment Total Entered Value USD 19,800.00",
        scope="SHIPMENT",
    )

    payload = _payload(
        {
            "description": _field(
                "description",
                "SUPPORTED",
                [
                    _evidence(
                        field="description",
                        value="Eucalyptus grandis KD boards",
                        document_id=10,
                        text="SKU ONLY-1 Eucalyptus grandis KD boards",
                        line_key=line_key,
                    )
                ],
            ),
            "hts_code": _field(
                "hts_code",
                "SUPPORTED",
                [
                    _evidence(
                        field="hts_code",
                        value="4407990190",
                        document_id=10,
                        text="SKU ONLY-1 HTS 4407.99.0190",
                        line_key=line_key,
                    )
                ],
            ),
            "genus": _field(
                "genus",
                "SUPPORTED",
                [
                    _evidence(
                        field="genus",
                        value="Eucalyptus",
                        document_id=20,
                        text="Eucalyptus grandis",
                        component_key=taxon,
                    )
                ],
            ),
            "species": _field(
                "species",
                "SUPPORTED",
                [
                    _evidence(
                        field="species",
                        value="Eucalyptus grandis",
                        document_id=20,
                        text="Eucalyptus grandis",
                        component_key=taxon,
                    )
                ],
            ),
            "shipment_total_entered_value": _field(
                "shipment_total_entered_value",
                "SUPPORTED",
                [total],
            ),
        }
    )

    truth = build_canonical_shipment_truth(payload)

    assert len(truth.plant_lines) == 1
    line = truth.plant_lines[0]
    entered = line.fields["entered_value"]
    assert entered.values == ("19800",)
    assert entered.state is CanonicalTruthState.REVIEW_REQUIRED
    assert entered.evidence[0].field_key == "shipment_total_entered_value"
    assert entered.evidence[0].normalized_value == "19800"

def test_unique_document_line_number_binds_quantity_and_unit() -> None:
    """Explicit LINE:n is valid only when it identifies one row in its source document."""
    line_key = "LINE:7"
    taxon = "taxon:pinus:taeda"
    payload = _payload(
        {
            "description": _field(
                "description",
                "SUPPORTED",
                [
                    _evidence(
                        field="description",
                        value="Pinus taeda KD boards",
                        document_id=10,
                        text="Line 7 Pinus taeda KD boards",
                        line_key=line_key,
                    )
                ],
            ),
            "hts_code": _field(
                "hts_code",
                "SUPPORTED",
                [
                    _evidence(
                        field="hts_code",
                        value="4407110190",
                        document_id=10,
                        text="Line 7 HTS 4407.11.0190",
                        line_key=line_key,
                    )
                ],
            ),
            "genus": _field(
                "genus",
                "SUPPORTED",
                [
                    _evidence(
                        field="genus",
                        value="Pinus",
                        document_id=10,
                        text="Line 7 Pinus taeda",
                        component_key=taxon,
                    )
                ],
            ),
            "species": _field(
                "species",
                "SUPPORTED",
                [
                    _evidence(
                        field="species",
                        value="Pinus taeda",
                        document_id=10,
                        text="Line 7 Pinus taeda",
                        component_key=taxon,
                    )
                ],
            ),
            "plant_quantity": _field(
                "plant_quantity",
                "SUPPORTED",
                [
                    _evidence(
                        field="plant_quantity",
                        value="30",
                        document_id=10,
                        text="Line 7 Plant Quantity 30 m3",
                        component_key=line_key,
                    )
                ],
            ),
            "metric_unit": _field(
                "metric_unit",
                "SUPPORTED",
                [
                    _evidence(
                        field="metric_unit",
                        value="m3",
                        document_id=10,
                        text="Line 7 Plant Quantity 30 m3",
                        component_key=line_key,
                    )
                ],
            ),
        }
    )

    truth = build_canonical_shipment_truth(payload)

    assert len(truth.plant_lines) == 1
    line = truth.plant_lines[0]
    assert line.entity_key == line_key
    assert line.fields["plant_quantity"].values == ("30",)
    assert line.fields["metric_unit"].values == ("m3",)


def test_unique_shipment_total_is_not_allocated_across_multiple_declarable_lines() -> None:
    """One shipment total cannot be guessed across two declaration lines."""
    pinus_line = "SKU:PT-38"
    eucalyptus_line = "SKU:EG-22"
    pinus = "taxon:pinus:taeda"
    eucalyptus = "taxon:eucalyptus:grandis"
    total = _evidence(
        field="shipment_total_entered_value",
        value="30940",
        document_id=40,
        text="Shipment Total Entered Value USD 30,940.00",
        scope="SHIPMENT",
    )
    payload = _payload(
        {
            "description": _field(
                "description",
                "SUPPORTED_MULTIPLE",
                [
                    _evidence(
                        field="description",
                        value="Pinus taeda KD boards",
                        document_id=10,
                        text="SKU PT-38 Pinus taeda KD boards",
                        line_key=pinus_line,
                    ),
                    _evidence(
                        field="description",
                        value="Eucalyptus grandis KD boards",
                        document_id=10,
                        text="SKU EG-22 Eucalyptus grandis KD boards",
                        line_key=eucalyptus_line,
                    ),
                ],
            ),
            "hts_code": _field(
                "hts_code",
                "SUPPORTED_MULTIPLE",
                [
                    _evidence(
                        field="hts_code",
                        value="4407110190",
                        document_id=10,
                        text="SKU PT-38 HTS 4407.11.0190",
                        line_key=pinus_line,
                    ),
                    _evidence(
                        field="hts_code",
                        value="4407990190",
                        document_id=10,
                        text="SKU EG-22 HTS 4407.99.0190",
                        line_key=eucalyptus_line,
                    ),
                ],
            ),
            "genus": _field(
                "genus",
                "SUPPORTED_MULTIPLE",
                [
                    _evidence(
                        field="genus",
                        value="Pinus",
                        document_id=20,
                        text="Pinus taeda",
                        component_key=pinus,
                    ),
                    _evidence(
                        field="genus",
                        value="Eucalyptus",
                        document_id=20,
                        text="Eucalyptus grandis",
                        component_key=eucalyptus,
                    ),
                ],
            ),
            "species": _field(
                "species",
                "SUPPORTED_MULTIPLE",
                [
                    _evidence(
                        field="species",
                        value="Pinus taeda",
                        document_id=20,
                        text="Pinus taeda",
                        component_key=pinus,
                    ),
                    _evidence(
                        field="species",
                        value="Eucalyptus grandis",
                        document_id=20,
                        text="Eucalyptus grandis",
                        component_key=eucalyptus,
                    ),
                ],
            ),
            "shipment_total_entered_value": _field(
                "shipment_total_entered_value",
                "SUPPORTED",
                [total],
            ),
        }
    )

    truth = build_canonical_shipment_truth(payload)

    assert len(truth.plant_lines) == 2
    assert all("entered_value" not in line.fields for line in truth.plant_lines)


def test_multiple_shipment_totals_do_not_seed_single_line_entered_value() -> None:
    """A one-line shipment still fails closed when the shipment total is not unique."""
    line_key = "SKU:ONLY-1"
    taxon = "taxon:eucalyptus:grandis"
    totals = [
        _evidence(
            field="shipment_total_entered_value",
            value="19800",
            document_id=40,
            text="Shipment Total Entered Value USD 19,800.00",
            scope="SHIPMENT",
        ),
        _evidence(
            field="shipment_total_entered_value",
            value="19900",
            document_id=41,
            text="Shipment Total Entered Value USD 19,900.00",
            scope="SHIPMENT",
        ),
    ]
    payload = _payload(
        {
            "description": _field(
                "description",
                "SUPPORTED",
                [
                    _evidence(
                        field="description",
                        value="Eucalyptus grandis KD boards",
                        document_id=10,
                        text="SKU ONLY-1 Eucalyptus grandis KD boards",
                        line_key=line_key,
                    )
                ],
            ),
            "hts_code": _field(
                "hts_code",
                "SUPPORTED",
                [
                    _evidence(
                        field="hts_code",
                        value="4407990190",
                        document_id=10,
                        text="SKU ONLY-1 HTS 4407.99.0190",
                        line_key=line_key,
                    )
                ],
            ),
            "genus": _field(
                "genus",
                "SUPPORTED",
                [
                    _evidence(
                        field="genus",
                        value="Eucalyptus",
                        document_id=20,
                        text="Eucalyptus grandis",
                        component_key=taxon,
                    )
                ],
            ),
            "species": _field(
                "species",
                "SUPPORTED",
                [
                    _evidence(
                        field="species",
                        value="Eucalyptus grandis",
                        document_id=20,
                        text="Eucalyptus grandis",
                        component_key=taxon,
                    )
                ],
            ),
            "shipment_total_entered_value": _field(
                "shipment_total_entered_value",
                "SUPPORTED_MULTIPLE",
                totals,
            ),
        }
    )

    truth = build_canonical_shipment_truth(payload)

    assert len(truth.plant_lines) == 1
    assert "entered_value" not in truth.plant_lines[0].fields

def test_explicit_component_sku_maps_taxon_without_taxonomic_commercial_description() -> None:
    """Explicit SKU+taxon evidence must not depend on taxonomy words in description."""
    line_key = "SKU:RUB-CB-32"
    taxon = "taxon:hevea:brasiliensis"
    payload = _payload(
        {
            "description": _field(
                "description",
                "SUPPORTED",
                [
                    _evidence(
                        field="description",
                        value="Rubberwood kitchen cutting boards with juice groove",
                        document_id=10,
                        text="SKU RUB-CB-32 Rubberwood kitchen cutting boards with juice groove",
                        line_key=line_key,
                    )
                ],
            ),
            "hts_code": _field(
                "hts_code",
                "SUPPORTED",
                [
                    _evidence(
                        field="hts_code",
                        value="4419908000",
                        document_id=10,
                        text="SKU RUB-CB-32 HTS 4419.90.8000",
                        line_key=line_key,
                    )
                ],
            ),
            "species": _field(
                "species",
                "SUPPORTED",
                [
                    _evidence(
                        field="species",
                        value="Hevea brasiliensis",
                        document_id=20,
                        text="SKU RUB-CB-32 Hevea brasiliensis",
                        line_key=line_key,
                        component_key=taxon,
                    )
                ],
            ),
            "plant_quantity": _field(
                "plant_quantity",
                "SUPPORTED",
                [
                    _evidence(
                        field="plant_quantity",
                        value="510",
                        document_id=20,
                        text="SKU RUB-CB-32 Plant Quantity 510 KG",
                        line_key=line_key,
                        component_key=taxon,
                    )
                ],
            ),
            "metric_unit": _field(
                "metric_unit",
                "SUPPORTED",
                [
                    _evidence(
                        field="metric_unit",
                        value="KG",
                        document_id=20,
                        text="SKU RUB-CB-32 Plant Quantity 510 KG",
                        line_key=line_key,
                        component_key=taxon,
                    )
                ],
            ),
        }
    )

    truth = build_canonical_shipment_truth(payload)

    assert len(truth.plant_lines) == 1
    line = truth.plant_lines[0]
    assert line.entity_key == line_key
    assert line.taxon_key == taxon
    assert line.fields["species"].values == ("Hevea brasiliensis",)
    assert line.fields["plant_quantity"].values == ("510",)
    assert line.fields["metric_unit"].values == ("KG",)

