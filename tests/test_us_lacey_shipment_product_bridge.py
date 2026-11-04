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
                        "source": {
                            "page": 1,
                            "locator": "pdf:page:1;table:1",
                        },
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


def _explicit(line_reference: str):
    return {
        "line_reference": line_reference,
        "sku": "CHAIR-001",
        "product_key": "SKU:CHAIR-001",
        "link_method": "EXACT_SKU",
        "supplier_public_id": "11111111-1111-1111-1111-111111111111",
        "supplier_product_public_id": "22222222-2222-2222-2222-222222222222",
    }


def test_bridge_links_only_from_explicit_product_identity_relation():
    generated = derived_line_reference_for_identity("SKU:CHAIR-001")

    bridge = build_shipment_product_bridge(
        _payload(),
        line_references=(generated,),
        explicit_links=(_explicit(generated),),
    )

    assert bridge["schema_version"] == BRIDGE_SCHEMA_VERSION
    assert bridge["composition_count"] == 1
    assert bridge["linked_count"] == 1
    assert bridge["review_count"] == 0
    link = bridge["links"][0]
    assert link["status"] == "LINKED"
    assert link["line_item_key"] == "SKU:CHAIR-001"
    assert link["shipment_line_reference"] == generated
    assert link["link_method"] == "EXACT_SKU"
    assert link["product"]["sku"] == "CHAIR-001"
    assert (
        link["product"]["components"][0]["material"]["taxonomy"]
        ["candidates"][0]["scientific_name"]
        == "Hevea brasiliensis"
    )
    assert link["source"]["filename"] == "bom.pdf"
    assert link["source"]["table_name"] == "page_1_table_1"


def test_literal_line_reference_equal_to_sku_is_not_binding_evidence():
    bridge = build_shipment_product_bridge(
        _payload(),
        line_references=("CHAIR-001",),
    )

    link = bridge["links"][0]
    assert link["status"] == "UNLINKED_REVIEW"
    assert link["shipment_line_reference"] is None
    assert link["candidate_line_references"] == []


def test_bridge_fails_closed_when_two_explicit_links_claim_same_sku():
    generated = derived_line_reference_for_identity("SKU:CHAIR-001")

    bridge = build_shipment_product_bridge(
        _payload(),
        line_references=("LINE-1", generated),
        explicit_links=(
            _explicit("LINE-1"),
            _explicit(generated),
        ),
    )

    link = bridge["links"][0]
    assert link["status"] == "AMBIGUOUS_REVIEW"
    assert link["shipment_line_reference"] is None
    assert set(link["candidate_line_references"]) == {
        "LINE-1",
        generated,
    }
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
