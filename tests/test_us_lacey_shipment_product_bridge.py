from __future__ import annotations

from litoral_trace.us_lacey.shipment_product_bridge import (
    BRIDGE_SCHEMA_VERSION,
    build_shipment_product_bridge,
)
from litoral_trace.us_lacey.specialized_projection import (
    derived_line_reference_for_identity,
)


def _payload():
    return {
        "schema_version": "product-intelligence-snapshot-v1",
        "sources": [
            {
                "document_id": "doc-1",
                "filename": "bom.pdf",
                "assurance_document_id": 7,
                "tables": [
                    {
                        "name": "page_1_table_1",
                        "source": {"page": 1, "locator": "pdf:page:1;table:1"},
                        "compositions": [
                            {
                                "sku": "CHAIR-001",
                                "product_name": "Chair",
                                "components": [
                                    {
                                        "component_key": "CHAIR-001::LEG",
                                        "description_raw": "Front leg",
                                        "quantity": "4",
                                        "material": {
                                            "name_raw": "Rubberwood",
                                            "name_normalized": "rubberwood",
                                            "taxonomy": {
                                                "status": "EXACT",
                                                "candidates": [
                                                    {
                                                        "scientific_name": "Hevea brasiliensis",
                                                        "genus": "Hevea",
                                                        "species_epithet": "brasiliensis",
                                                    }
                                                ],
                                            },
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
    }


def test_bridge_links_specialized_generated_line_reference_from_sku_identity():
    generated = derived_line_reference_for_identity("SKU:CHAIR-001")

    bridge = build_shipment_product_bridge(
        _payload(),
        line_references=(generated,),
    )

    assert bridge["schema_version"] == BRIDGE_SCHEMA_VERSION
    assert bridge["composition_count"] == 1
    assert bridge["linked_count"] == 1
    assert bridge["review_count"] == 0
    link = bridge["links"][0]
    assert link["status"] == "LINKED"
    assert link["line_item_key"] == "SKU:CHAIR-001"
    assert link["shipment_line_reference"] == generated
    assert link["product"]["sku"] == "CHAIR-001"
    assert link["product"]["components"][0]["material"]["taxonomy"]["candidates"][0]["scientific_name"] == "Hevea brasiliensis"
    assert link["source"]["filename"] == "bom.pdf"
    assert link["source"]["table_name"] == "page_1_table_1"


def test_bridge_links_manual_line_reference_equal_to_sku():
    bridge = build_shipment_product_bridge(
        _payload(),
        line_references=("CHAIR-001",),
    )

    assert bridge["links"][0]["status"] == "LINKED"
    assert bridge["links"][0]["shipment_line_reference"] == "CHAIR-001"


def test_bridge_fails_closed_when_multiple_existing_lines_match_same_sku():
    generated = derived_line_reference_for_identity("SKU:CHAIR-001")

    bridge = build_shipment_product_bridge(
        _payload(),
        line_references=("CHAIR-001", generated),
    )

    link = bridge["links"][0]
    assert link["status"] == "AMBIGUOUS_REVIEW"
    assert link["shipment_line_reference"] is None
    assert set(link["candidate_line_references"]) == {"CHAIR-001", generated}
    assert bridge["linked_count"] == 0
    assert bridge["review_count"] == 1


def test_bridge_never_guesses_unmatched_sku_from_product_description():
    bridge = build_shipment_product_bridge(
        _payload(),
        line_references=("Chair", "LINE-1"),
    )

    link = bridge["links"][0]
    assert link["status"] == "UNLINKED_REVIEW"
    assert link["shipment_line_reference"] is None
    assert link["candidate_line_references"] == []
    assert bridge["review_count"] == 1
