"""Shipment 2: exact SKU can be bound BEFORE botanical memory is consulted.

Source-label arrangements match the original synthetic six-PDF packet:
invoice slash-form SKU row, packing list SKU table header separated from value,
bill-of-lading cargo SKU, and entry worksheet's two-line Manufacturer SKU.
No species/genus is available in these second-shipment source documents.
"""
from __future__ import annotations

from copy import deepcopy

from litoral_trace.us_lacey.single_sku_recovery import (
    _explicit_document_skus, bind_single_sku_evidence,
)


def _doc(identity: str, kind: str, *lines: str) -> dict:
    return {
        "document_id": identity,
        "resolution": {
            "document_type": kind,
            "layout": {"blocks": [
                {"block_type": "TEXT_LINE", "text": line} for line in lines
            ]},
        },
    }


def _row(field: str, value: str):
    return {
        "document_id": "invoice", "field_key": field,
        "normalized_value": value, "line_key": None, "component_key": None,
    }


def _field(field: str, value: str | None):
    return {
        "state": "SUPPORTED" if value else "REVIEW_REQUIRED",
        "values": [{"value": value}] if value else [],
        "supporting_evidence": [_row(field, value)] if value else [],
    }


def golden_shipment_2():
    documents = [
        _doc("invoice", "COMMERCIAL_INVOICE",
             "COMMERCIAL INVOICE",
             "LINE / SKU DESCRIPTION / HTS QTY UNIT VALUE EXTENDED VALUE",
             "1 / BAM-COAST-04 Bamboo coaster set, 4 900 SET USD 12.50 USD 11,250.00",
             "Supplier / manufacturer LONGSHENG BAMBOO HOUSEWARES CO., LTD.",
             "Product / SKU", "Bamboo coaster set (4 pieces) / BAM-COAST-04"),
        _doc("packing", "PACKING_LIST",
             "PACKING LIST", "SKU", "Cartons", "Quantity", "Net Weight",
             "Description", "BAM-COAST-04", "LSBH-BATCH-26B",
             "Plant-material 288.0 KG"),
        _doc("bill", "BILL_OF_LADING",
             "HOUSE BILL OF LADING", "LSBH-B31405 / SKU BAM-COAST-04"),
        _doc("supplier", "SUPPLIER_DECLARATION",
             "SUPPLIER ORDER CONFIRMATION", "MANUFACTURER", "SKU",
             "DESCRIPTION", "ORDER QTY", "BAM-COAST-04"),
        _doc("entry", "UNKNOWN",
             "ENTRY PREPARATION WORKSHEET", "Manufacturer SKU", "BAM-COAST-04",
             "Scientific genus TO BE FILLED FROM REVIEWED SUPPLIER MEMORY",
             "Scientific species TO BE FILLED FROM REVIEWED SUPPLIER MEMORY",
             "Country of harvest TO BE CONFIRMED FROM VALID SUPPLIER EVIDENCE"),
        _doc("arrival", "ARRIVAL_NOTICE",
             "ARRIVAL NOTICE", "Merchandise / SKU",
             "Bamboo coaster sets / BAM-COAST-04"),
    ]
    fields = {
        "hts_code": _field("hts_code", "4419199010"),
        "article_component": _field("article_component", "SOLID BAMBOO COASTERS"),
        "plant_quantity": _field("plant_quantity", "288.0"),
        "metric_unit": _field("metric_unit", "KG"),
        "genus": _field("genus", None),
        "species": _field("species", None),
        "country_of_harvest": _field("country_of_harvest", None),
    }
    return documents, fields


def test_exact_sku_labels_survive_realistic_pdf_line_breaks():
    docs, _ = golden_shipment_2()
    assert all(_explicit_document_skus(
        tuple(block["text"] for block in doc["resolution"]["layout"]["blocks"])
    ) == frozenset({"BAM-COAST-04"}) for doc in docs)


def test_shipment_2_commercial_line_binding_precedes_species_memory():
    docs, fields = golden_shipment_2()
    assert bind_single_sku_evidence(fields, docs)
    assert fields["hts_code"]["supporting_evidence"][0]["line_key"] == "sku:bam-coast-04"
    assert fields["article_component"]["supporting_evidence"][0]["line_key"] == "sku:bam-coast-04"
    assert fields["genus"]["state"] == "REVIEW_REQUIRED"
    assert fields["species"]["state"] == "REVIEW_REQUIRED"
    assert fields["country_of_harvest"]["state"] == "REVIEW_REQUIRED"
    assert fields["genus"]["values"] == []
    assert fields["article_component"]["supporting_evidence"][0]["component_key"] is None


def test_multiple_commercial_lines_or_conflicting_labeled_skus_block_identity():
    docs, fields = golden_shipment_2()
    conflicting = deepcopy(docs)
    conflicting[3]["resolution"]["layout"]["blocks"].append({
        "block_type": "TEXT_LINE", "text": "Manufacturer SKU OTHER-TRAY-99",
    })
    assert not bind_single_sku_evidence(deepcopy(fields), conflicting)
    multi = deepcopy(docs)
    multi[0]["resolution"]["layout"]["blocks"].append({
        "block_type": "TEXT_LINE",
        "text": "2 / BAM-COAST-05 Other product 100 SET USD 1,250",
    })
    assert not bind_single_sku_evidence(deepcopy(fields), multi)
