from __future__ import annotations

from copy import deepcopy

from litoral_trace.us_lacey.product_intelligence_snapshot import (
    ProductIntelligenceDocumentInput,
    analyze_product_intelligence_documents,
    enrich_product_intelligence_taxonomy,
)


def _source(payload: bytes) -> ProductIntelligenceDocumentInput:
    return ProductIntelligenceDocumentInput(
        operation_document_id=111,
        assurance_document_id=11,
        document_id="assurance-11",
        filename="bom.csv",
        source_sha256="a" * 64,
        content=payload,
    )


def _materials(result):
    table = result.payload["sources"][0]["tables"][0]
    components = table["compositions"][0]["components"]
    return {component["material"]["name_raw"]: component["material"] for component in components}


def test_product_intelligence_attaches_noncanonical_taxonomy_without_losing_provenance():
    payload = (
        b"SKU,Product,Component,Material,Qty,Weight,UOM\n"
        b"CHAIR-001,Chair,Leg,Rubberwood,4,0.5,kg\n"
        b"CHAIR-001,Chair,Seat,Hevea wood,1,1,kg\n"
        b"CHAIR-001,Chair,Panel,Plywood,1,2,kg\n"
    )

    result = analyze_product_intelligence_documents((_source(payload),))

    assert result.status == "READY"
    materials = _materials(result)

    rubberwood = materials["Rubberwood"]
    assert rubberwood["taxonomy"]["status"] == "REVIEW_REQUIRED"
    assert rubberwood["taxonomy"]["review_required"] is True
    assert rubberwood["taxonomy"]["candidates"][0]["scientific_name"] == "Hevea brasiliensis"
    assert rubberwood["taxonomy"]["candidates"][0]["match_kind"] == "COMMERCIAL_ALIAS"
    assert rubberwood["source"]["document_id"] == "assurance-11"
    assert rubberwood["source"]["row"] == 2

    hevea_wood = materials["Hevea wood"]
    assert hevea_wood["taxonomy"]["status"] == "REVIEW_REQUIRED"
    assert hevea_wood["taxonomy"]["candidates"][0]["scientific_name"] == "Hevea"
    assert hevea_wood["taxonomy"]["candidates"][0]["rank"] == "GENUS"
    assert hevea_wood["taxonomy"]["candidates"][0]["species_epithet"] is None
    assert all(
        candidate["scientific_name"] != "Hevea brasiliensis"
        for candidate in hevea_wood["taxonomy"]["candidates"]
    )
    assert hevea_wood["source"]["row"] == 3

    plywood = materials["Plywood"]
    assert plywood["taxonomy"]["status"] == "NO_MATCH"
    assert plywood["taxonomy"]["review_required"] is True
    assert plywood["taxonomy"]["candidates"] == []
    assert plywood["source"]["row"] == 4


def test_taxonomy_enrichment_does_not_change_bom_readiness():
    payload = (
        b"SKU,Product,Component,Material,Qty,Weight,UOM\n"
        b"SKU-1,Product,Body,Unknown mystery wood,1,1,kg\n"
    )

    result = analyze_product_intelligence_documents((_source(payload),))

    assert result.status == "READY"
    material = next(iter(_materials(result).values()))
    assert material["taxonomy"]["status"] == "NO_MATCH"
    assert material["taxonomy"]["review_required"] is True


def test_legacy_snapshot_payload_is_enriched_on_read_without_rewriting_source_payload():
    legacy_payload = {
        "schema_version": "product-intelligence-snapshot-v1",
        "sources": [
            {
                "filename": "legacy-bom.csv",
                "tables": [
                    {
                        "compositions": [
                            {
                                "sku": "CHAIR-OLD",
                                "components": [
                                    {
                                        "component_key": "leg",
                                        "material": {
                                            "name_raw": "Hevea wood",
                                            "name_normalized": "hevea wood",
                                            "source": {
                                                "document_id": "legacy-assurance-1",
                                                "sheet": "BOM",
                                                "row": 7,
                                            },
                                        },
                                    }
                                ],
                            }
                        ]
                    }
                ],
            }
        ],
    }
    original = deepcopy(legacy_payload)

    enriched = enrich_product_intelligence_taxonomy(legacy_payload)

    material = enriched["sources"][0]["tables"][0]["compositions"][0]["components"][0]["material"]
    assert material["taxonomy"]["status"] == "REVIEW_REQUIRED"
    assert material["taxonomy"]["candidates"][0]["scientific_name"] == "Hevea"
    assert material["taxonomy"]["candidates"][0]["rank"] == "GENUS"
    assert material["source"] == original["sources"][0]["tables"][0]["compositions"][0]["components"][0]["material"]["source"]
    assert legacy_payload == original


def test_read_compatibility_keeps_existing_taxonomy_instead_of_reinterpreting_it():
    payload = {
        "sources": [
            {
                "tables": [
                    {
                        "compositions": [
                            {
                                "components": [
                                    {
                                        "material": {
                                            "name_raw": "Rubberwood",
                                            "taxonomy": {"status": "HISTORICAL_TEST_VALUE"},
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }

    enriched = enrich_product_intelligence_taxonomy(payload)

    material = enriched["sources"][0]["tables"][0]["compositions"][0]["components"][0]["material"]
    assert material["taxonomy"] == {"status": "HISTORICAL_TEST_VALUE"}
