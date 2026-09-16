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
    authority: float = 20.0,
    score: float = 95.0,
):
    return {
        "candidate_id": f"{document_id}:{field}:{line_key or component_key}:{value}",
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
            },
            "provenance": {
                "page": 1,
                "source_text": text,
                "evidence_class": "EXPLICIT",
            },
        },
    }


def _field(field: str, state: str, rows: list[dict]):
    values = [] if state == "REVIEW_REQUIRED" else [
        {"value": value, "evidence_ids": []}
        for value in dict.fromkeys(row["normalized_value"] for row in rows)
    ]
    return {
        "field_key": field,
        "state": state,
        "values": values,
        "supporting_evidence": rows,
    }


def _golden_payload() -> dict:
    line_1 = "6:table:2:row:1"
    line_2 = "6:table:2:row:2"
    pinus = "taxon:pinus:taeda"
    eucalyptus = "taxon:eucalyptus:grandis"

    descriptions = [
        _evidence(
            field="description",
            value="Pinus taeda KD sawn boards",
            document_id=6,
            text="Line 1 4407.11.0190 Pinus taeda KD sawn boards USD 18,300.00",
            line_key=line_1,
        ),
        _evidence(
            field="description",
            value="Eucalyptus grandis KD sawn boards",
            document_id=6,
            text="Line 2 4407.99.0190 Eucalyptus grandis KD sawn boards USD 12,640.00",
            line_key=line_2,
        ),
    ]
    hts = [
        _evidence(field="hts_code", value="4407110190", document_id=6, text="4407.11.0190", line_key=line_1),
        _evidence(field="hts_code", value="4407990190", document_id=6, text="4407.99.0190", line_key=line_2),
    ]
    entered = [
        _evidence(field="entered_value", value="18300", document_id=6, text="USD 18,300.00", line_key=line_1),
        _evidence(field="entered_value", value="12640", document_id=6, text="USD 12,640.00", line_key=line_2),
    ]
    genus = [
        _evidence(field="genus", value="Pinus", document_id=4, text="Pinus taeda", component_key=pinus),
        _evidence(field="genus", value="Eucalyptus", document_id=4, text="Eucalyptus grandis", component_key=eucalyptus),
    ]
    species = [
        _evidence(field="species", value="Pinus taeda", document_id=4, text="Pinus taeda", component_key=pinus),
        _evidence(field="species", value="Eucalyptus grandis", document_id=4, text="Eucalyptus grandis", component_key=eucalyptus),
    ]
    harvest = [
        _evidence(
            field="country_of_harvest",
            value="Brazil",
            document_id=5,
            text="Pinus taeda - Pais de colheita: Brasil",
            component_key=pinus,
            authority=5.0,
        ),
        _evidence(
            field="country_of_harvest",
            value="Brazil",
            document_id=5,
            text="Eucalyptus grandis - Pais de colheita: Brasil",
            component_key=eucalyptus,
            authority=5.0,
        ),
    ]
    quantity = [
        _evidence(field="plant_quantity", value="30", document_id=4, text="Pinus taeda 30.000 m3", component_key=pinus),
        _evidence(field="plant_quantity", value="16", document_id=4, text="Eucalyptus grandis 16.000 m3", component_key=eucalyptus),
    ]
    units = [
        _evidence(field="metric_unit", value="m3", document_id=4, text="Pinus taeda 30.000 m3", component_key=pinus),
        _evidence(field="metric_unit", value="m3", document_id=4, text="Eucalyptus grandis 16.000 m3", component_key=eucalyptus),
    ]

    return {
        "schema_version": "lacey_shipment_resolution_v1",
        "engine_version": "fixture",
        "canonical_fields": {
            "description": _field("description", "SUPPORTED_MULTIPLE", descriptions),
            "hts_code": _field("hts_code", "SUPPORTED_MULTIPLE", hts),
            "entered_value": _field("entered_value", "SUPPORTED_MULTIPLE", entered),
            "genus": _field("genus", "SUPPORTED_MULTIPLE", genus),
            "species": _field("species", "SUPPORTED_MULTIPLE", species),
            "country_of_harvest": _field("country_of_harvest", "REVIEW_REQUIRED", harvest),
            "plant_quantity": _field("plant_quantity", "SUPPORTED_MULTIPLE", quantity),
            "metric_unit": _field("metric_unit", "SUPPORTED_MULTIPLE", units),
        },
        "issues": [
            {
                "issue_id": "low-authority:country_of_harvest",
                "field_key": "country_of_harvest",
                "scope": "PLANT_COMPONENT",
                "severity": "MEDIUM",
                "issue_type": "LOW_AUTHORITY_ONLY",
                "candidate_ids": [row["candidate_id"] for row in harvest],
                "document_ids": ["5"],
                "requires_human_review": True,
            }
        ],
    }


def test_golden_shipment_truth_has_two_isolated_complete_lines():
    truth = build_canonical_shipment_truth(_golden_payload())

    assert len(truth.plant_lines) == 2
    first, second = truth.plant_lines

    assert first.ordinal_hint == 1
    assert first.taxon_key == "taxon:pinus:taeda"
    assert first.fields["hts_code"].values == ("4407110190",)
    assert first.fields["merchandise_description"].values == ("Pinus taeda KD sawn boards",)
    assert first.fields["entered_value"].values == ("18300",)
    assert first.fields["genus"].values == ("Pinus",)
    assert first.fields["species"].values == ("Pinus taeda",)
    assert first.fields["plant_quantity"].values == ("30",)
    assert first.fields["metric_unit"].values == ("m3",)

    assert second.ordinal_hint == 2
    assert second.taxon_key == "taxon:eucalyptus:grandis"
    assert second.fields["hts_code"].values == ("4407990190",)
    assert second.fields["merchandise_description"].values == ("Eucalyptus grandis KD sawn boards",)
    assert second.fields["entered_value"].values == ("12640",)
    assert second.fields["genus"].values == ("Eucalyptus",)
    assert second.fields["species"].values == ("Eucalyptus grandis",)
    assert second.fields["plant_quantity"].values == ("16",)
    assert second.fields["metric_unit"].values == ("m3",)

    assert {row.normalized_value for row in first.fields["species"].evidence} == {"Pinus taeda"}
    assert {row.normalized_value for row in second.fields["species"].evidence} == {"Eucalyptus grandis"}
    assert first.fields["hts_code"].state is CanonicalTruthState.SUPPORTED
    assert second.fields["hts_code"].state is CanonicalTruthState.SUPPORTED

    description = truth.shipment_fields["merchandise_description"]
    assert description.state is CanonicalTruthState.SUPPORTED_MULTIPLE
    assert description.values == (
        "Pinus taeda KD sawn boards; Eucalyptus grandis KD sawn boards",
    )
    assert {row.line_key for row in description.evidence} == {
        "6:table:2:row:1",
        "6:table:2:row:2",
    }


def test_low_authority_harvest_country_is_visible_review_not_missing():
    truth = build_canonical_shipment_truth(_golden_payload())

    for line in truth.plant_lines:
        country = line.fields["country_of_harvest"]
        assert country.state is CanonicalTruthState.REVIEW_REQUIRED
        assert country.values == ("Brazil",)
        assert len(country.evidence) == 1
        assert country.evidence[0].source_authority == 5.0


def test_ambiguous_components_fail_closed_instead_of_leaking_across_lines():
    payload = _golden_payload()
    description_rows = payload["canonical_fields"]["description"]["supporting_evidence"]
    description_rows[0]["normalized_value"] = "KD sawn boards grade A"
    description_rows[0]["candidate"]["raw"]["normalized_value"] = "KD sawn boards grade A"
    description_rows[0]["candidate"]["provenance"]["source_text"] = "Line 1 KD sawn boards grade A"
    description_rows[1]["normalized_value"] = "KD sawn boards grade B"
    description_rows[1]["candidate"]["raw"]["normalized_value"] = "KD sawn boards grade B"
    description_rows[1]["candidate"]["provenance"]["source_text"] = "Line 2 KD sawn boards grade B"

    truth = build_canonical_shipment_truth(payload)

    assert len(truth.plant_lines) == 2
    assert truth.unresolved_component_keys == (
        "taxon:eucalyptus:grandis",
        "taxon:pinus:taeda",
    )
    assert "species" not in truth.plant_lines[0].fields
    assert "species" not in truth.plant_lines[1].fields
