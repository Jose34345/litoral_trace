from __future__ import annotations

from litoral_trace.us_lacey.shipment_product_bridge import (
    BRIDGE_SCHEMA_VERSION,
    CanonicalLineFieldInput,
    CanonicalPlantLineInput,
    build_shipment_product_bridge,
    materialized_line_reference,
)


def _pi_payload(*, sku: str = "CHAIR-001", description: str = "Chair") -> dict:
    return {
        "schema_version": "product-intelligence-snapshot-v2",
        "sources": [
            {
                "document_id": "doc-bom-1",
                "filename": "bom.xlsx",
                "tables": [
                    {
                        "name": "BOM",
                        "source": {"sheet": "BOM", "locator": "sheet:BOM"},
                        "compositions": [
                            {
                                "sku": sku,
                                "product_name": description,
                                "components": [
                                    {
                                        "component_key": "LEG-1",
                                        "description_raw": "Front leg",
                                        "quantity": "4",
                                        "source": {
                                            "document_id": "doc-bom-1",
                                            "sheet": "BOM",
                                            "row": 2,
                                        },
                                        "material": {
                                            "name_raw": "Rubberwood",
                                            "name_normalized": "rubberwood",
                                            "mass": {
                                                "raw_value": "0.5",
                                                "raw_unit": "kg",
                                                "kilograms": "0.5",
                                            },
                                            "source": {
                                                "document_id": "doc-bom-1",
                                                "sheet": "BOM",
                                                "row": 2,
                                            },
                                            "taxonomy": {
                                                "status": "AMBIGUOUS",
                                                "review_required": True,
                                                "candidates": [],
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


def _line(reference: str, *, ordinal: int = 1, description: str = "Chair") -> CanonicalPlantLineInput:
    return CanonicalPlantLineInput(
        line_reference=reference,
        ordinal=ordinal,
        fields=(
            CanonicalLineFieldInput(
                field_key="merchandise_description",
                value=description,
                status="MATCHED",
                source_assurance_document_id=10,
                source_page=1,
                source_locator="invoice:row:1",
            ),
            CanonicalLineFieldInput(
                field_key="hts_code",
                value="9403.60.8081",
                status="FOUND",
                source_assurance_document_id=10,
                source_page=1,
                source_locator="invoice:row:1",
            ),
        ),
    )


def test_bridge_binds_direct_sku_line_reference_and_carries_both_sides():
    bridge = build_shipment_product_bridge(
        canonical_lines=(_line("CHAIR-001"),),
        product_intelligence_payload=_pi_payload(),
    )

    assert bridge["schema_version"] == BRIDGE_SCHEMA_VERSION
    assert bridge["status"] == "READY"
    assert bridge["summary"] == {
        "canonical_line_count": 1,
        "product_sku_count": 1,
        "bound_line_count": 1,
        "unbound_line_count": 0,
        "unbound_product_count": 0,
        "ambiguous_binding_count": 0,
    }
    linked = bridge["lines"][0]
    assert linked["binding"] == {
        "status": "BOUND",
        "method": "DIRECT_SKU_LINE_REFERENCE",
        "sku": "CHAIR-001",
    }
    assert linked["shipment_facts"]["hts_code"]["value"] == "9403.60.8081"
    assert linked["product"]["sku"] == "CHAIR-001"
    assert linked["product"]["compositions"][0]["components"][0]["material"]["name_raw"] == "Rubberwood"


def test_bridge_binds_existing_specialized_materialized_sku_reference():
    generated = materialized_line_reference("SKU:CHAIR-001")

    bridge = build_shipment_product_bridge(
        canonical_lines=(_line(generated),),
        product_intelligence_payload=_pi_payload(),
    )

    assert bridge["lines"][0]["binding"] == {
        "status": "BOUND",
        "method": "SPECIALIZED_SKU_MATERIALIZATION",
        "sku": "CHAIR-001",
    }


def test_bridge_never_binds_on_description_similarity_alone():
    bridge = build_shipment_product_bridge(
        canonical_lines=(_line("LINE-1", description="Chair"),),
        product_intelligence_payload=_pi_payload(description="Chair"),
    )

    assert bridge["status"] == "PARTIAL"
    assert bridge["summary"]["bound_line_count"] == 0
    assert bridge["lines"][0]["binding"]["status"] == "UNBOUND"
    assert bridge["unbound_products"][0]["sku"] == "CHAIR-001"


def test_bridge_fails_closed_when_two_canonical_lines_claim_same_sku():
    bridge = build_shipment_product_bridge(
        canonical_lines=(
            _line("CHAIR-001", ordinal=1),
            _line(materialized_line_reference("SKU:CHAIR-001"), ordinal=2),
        ),
        product_intelligence_payload=_pi_payload(),
    )

    assert bridge["status"] == "PARTIAL"
    assert bridge["summary"]["bound_line_count"] == 0
    assert bridge["summary"]["ambiguous_binding_count"] == 2
    assert {item["binding"]["status"] for item in bridge["lines"]} == {"AMBIGUOUS"}
    assert bridge["unbound_products"][0]["sku"] == "CHAIR-001"


def test_bridge_is_not_applicable_without_explicit_product_compositions():
    bridge = build_shipment_product_bridge(
        canonical_lines=(_line("CHAIR-001"),),
        product_intelligence_payload={"sources": []},
    )

    assert bridge["status"] == "NOT_APPLICABLE"
    assert bridge["summary"]["product_sku_count"] == 0
    assert bridge["summary"]["bound_line_count"] == 0
