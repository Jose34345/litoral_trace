from __future__ import annotations

from litoral_trace.lacey_engine.domain import EvidenceClass, FieldStatus
from litoral_trace.lacey_engine.layout_parser import layout_from_key_value_rows
from litoral_trace.lacey_engine.pipeline import _extract, process_document


def test_explicit_ppq_labels_cover_customer_metadata_and_plant_fields(monkeypatch):
    layout = layout_from_key_value_rows(
        [
            ("Importer Name", "Acme Imports LLC"),
            ("Importer Address", "100 Main St, Miami, FL 33101"),
            ("Consignee Name", "Acme Warehouse LLC"),
            ("Consignee Address", "200 Port Ave, Miami, FL 33132"),
            ("Entry Number", "123-4567890-1"),
            ("Manufacturer Identification Code", "CNABC1234SHA"),
            ("Merchandise Description", "Pine furniture components"),
            ("HTS Number", "9403.60.8081"),
            ("Entered Value", "USD 12,500.00"),
            ("Article / Component", "Chair frame"),
            ("Genus", "Pinus"),
            ("Species", "radiata"),
            ("Country of Harvest", "New Zealand"),
            ("Quantity of Plant Material", "1,250 kg"),
            ("Percent Recycled", "0%"),
        ]
    )
    monkeypatch.setattr("litoral_trace.lacey_engine.pipeline.parse_layout", lambda *_args, **_kwargs: layout)
    resolution = process_document(filename="ppq-explicit.pdf", content=b"unused")

    expected = {
        "importer_name": "Acme Imports LLC",
        "importer_address": "100 Main St, Miami, FL 33101",
        "consignee_name": "Acme Warehouse LLC",
        "consignee_address": "200 Port Ave, Miami, FL 33132",
        "filing_entry_reference": "123-4567890-1",
        "manufacturer_id": "CNABC1234SHA",
        "description": "Pine furniture components",
        "hts_code": "9403.60.8081",
        "entered_value": "12500.00",
        "article_component": "Chair frame",
        "genus": "Pinus",
        "species": "radiata",
        "country_of_harvest": "New Zealand",
        "plant_quantity": "1250",
        "metric_unit": "kg",
        "percent_recycled": "0",
    }
    for field_key, value in expected.items():
        field = resolution.field(field_key)
        assert field.status is FieldStatus.MATCHED
        assert field.effective_value == value
        assert field.winning_candidate is not None
        assert field.winning_candidate.raw.evidence_class is not EvidenceClass.INFERRED


def test_weight_and_origin_context_do_not_become_lacey_facts():
    layout = layout_from_key_value_rows(
        [
            ("Gross Weight", "12,500 kg"),
            ("Net Weight", "12,000 kg"),
            ("Country of Origin", "Chile"),
            ("Exporter Address", "Santiago, Chile"),
            ("Port of Lading", "San Antonio, Chile"),
        ]
    )
    extracted = _extract(layout)
    assert extracted["plant_quantity"] == []
    assert extracted["metric_unit"] == []
    assert extracted["country_of_harvest"] == []


def test_generic_identifier_labels_do_not_create_entry_mid_or_hts_candidates():
    layout = layout_from_key_value_rows(
        [
            ("Reference", "123-4567890-1"),
            ("Supplier Code", "CNABC1234SHA"),
            ("Product Code", "9403608081"),
        ]
    )
    extracted = _extract(layout)
    assert extracted["filing_entry_reference"] == []
    assert extracted["manufacturer_id"] == []
    assert extracted["hts_code"] == []


def test_generic_component_label_outside_table_does_not_create_plant_component():
    layout = layout_from_key_value_rows([("Component", "Chair frame")])
    assert _extract(layout)["article_component"] == []
