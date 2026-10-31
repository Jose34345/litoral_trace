from __future__ import annotations

from litoral_trace.lacey_engine.domain import DocumentSection, DocumentType
from litoral_trace.lacey_engine.layout_parser import layout_from_matrix_rows
from litoral_trace.lacey_engine.pipeline import _extract, _resolve_logical_document
from litoral_trace.lacey_engine.shipment import (
    ReconciliationState,
    ShipmentDocumentInput,
    process_shipment,
)


def _values(found, field: str) -> list[str]:
    return [candidate.normalized_value for candidate in found[field]]


def test_golden_customs_headers_extract_htsus_and_entered_value_usd():
    layout = layout_from_matrix_rows(
        ["Line", "HTSUS", "Commercial Description", "Entered Value USD", "Origin"],
        [
            ["1", "4419.90.9000", "Acacia solid-wood serving trays", "18,900.00", "VN"],
            ["2", "4419.90.8000", "Rubberwood kitchen cutting boards", "17,760.00", "VN"],
            ["3", "4421.99.9880", "Teak salad-server pairs", "11,200.00", "VN"],
        ],
    )

    found = _extract(layout)

    assert _values(found, "hts_code") == [
        "4419.90.9000",
        "4419.90.8000",
        "4421.99.9880",
    ]
    assert _values(found, "entered_value") == ["18900.00", "17760.00", "11200.00"]


def test_golden_botanical_headers_extract_plant_qty_unit_and_harvest():
    layout = layout_from_matrix_rows(
        [
            "Line",
            "SKU",
            "HTSUS",
            "Article / Component",
            "Genus",
            "Species",
            "Harvest",
            "Qty",
            "Unit",
            "Line Value",
        ],
        [
            [
                "1",
                "ACT-TRAY-18",
                "4419.90.9000",
                "Serving tray / solid wood",
                "Acacia",
                "mangium",
                "Vietnam",
                "315",
                "KG",
                "$18,900.00",
            ],
            [
                "2",
                "RUB-CB-32",
                "4419.90.8000",
                "Cutting board / solid wood",
                "Hevea",
                "brasiliensis",
                "Vietnam",
                "510",
                "KG",
                "$17,760.00",
            ],
            [
                "3",
                "TEK-SRV-04",
                "4421.99.9880",
                "Salad servers / solid wood",
                "Tectona",
                "grandis",
                "Vietnam",
                "145",
                "KG",
                "$11,200.00",
            ],
        ],
    )

    found = _extract(layout)

    assert _values(found, "hts_code") == [
        "4419.90.9000",
        "4419.90.8000",
        "4421.99.9880",
    ]
    assert _values(found, "plant_quantity") == ["315", "510", "145"]
    assert {candidate.label for candidate in found["plant_quantity"]} == {"Plant Quantity"}
    assert [
        (candidate.source_block.table_id, candidate.source_block.row_index)
        for candidate in found["plant_quantity"]
    ] == [
        (candidate.source_block.table_id, candidate.source_block.row_index)
        for candidate in found["metric_unit"]
    ]
    assert _values(found, "metric_unit") == ["KG", "KG", "KG"]
    assert _values(found, "country_of_harvest") == ["Vietnam", "Vietnam", "Vietnam"]
    assert _values(found, "entered_value") == ["18900.00", "17760.00", "11200.00"]


def test_golden_noise_guards_trim_mid_and_reject_structural_party_and_description():
    layout = layout_from_matrix_rows(
        ["Manufacturer ID", "Consignee", "Description"],
        [
            [
                "VNMKHOM123HCM INVOICE MHW",
                "Notify Party / Importer",
                "Gross Wt.",
            ]
        ],
    )

    found = _extract(layout)

    assert _values(found, "manufacturer_id") == ["VNMKHOM123HCM"]
    assert _values(found, "consignee_name") == []
    assert _values(found, "description") == []


def test_golden_botanical_quantities_survive_shipment_semantic_gate_by_row():
    layout = layout_from_matrix_rows(
        ["SKU", "Article / Component", "Genus", "Species", "Country of Harvest", "Plant Qty", "Unit"],
        [
            ["ACT-TRAY-18", "Serving tray / solid wood", "Acacia", "mangium", "Vietnam", "315", "KG"],
            ["RUB-CB-32", "Cutting board / solid wood", "Hevea", "brasiliensis", "Vietnam", "510", "KG"],
            ["TEK-SRV-04", "Salad servers / solid wood", "Tectona", "grandis", "Vietnam", "145", "KG"],
        ],
    )
    section = DocumentSection(
        section_id="logical-004",
        page_start=1,
        page_end=1,
        document_type=DocumentType.SPECIES_DECLARATION,
        confidence=1.0,
        block_ids=tuple(block.block_id for block in layout.blocks),
    )
    resolution = _resolve_logical_document(
        filename="golden-botanical.pdf",
        layout=layout,
        section=section,
    )

    shipment = process_shipment(
        documents=[
            ShipmentDocumentInput(
                document_id="5:logical-004",
                filename="golden-botanical.pdf",
                resolution=resolution,
            )
        ]
    )

    quantity = shipment.canonical_fields["plant_quantity"]
    unit = shipment.canonical_fields["metric_unit"]

    assert quantity.state is ReconciliationState.SUPPORTED_MULTIPLE
    assert {value.value for value in quantity.values} == {"315", "510", "145"}
    assert {
        evidence.component_key for evidence in quantity.supporting_evidence
    } == {
        "5:logical-004:p1-t1:row:1",
        "5:logical-004:p1-t1:row:2",
        "5:logical-004:p1-t1:row:3",
    }
    assert {
        evidence.component_key for evidence in quantity.supporting_evidence
    } == {
        evidence.component_key for evidence in unit.supporting_evidence
    }
