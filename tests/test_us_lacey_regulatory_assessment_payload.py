from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from litoral_trace.us_lacey.regulatory_assessment_snapshot import (
    build_regulatory_assessment_payload,
)


def _field(
    name: str,
    value: str,
    *,
    line_reference: str = "LT-LINE-1",
    reviewed: bool = True,
):
    return SimpleNamespace(
        merchandise_line_reference=line_reference,
        field_name=name,
        field_scope="PLANT_LINE",
        normalized_value=value,
        original_value=value,
        human_value=value if reviewed else None,
        reviewed_at=datetime.now(timezone.utc) if reviewed else None,
        validation_status="VALID",
        field_status="MATCHED",
        source_assurance_document_id=11,
        source_page=3,
        source_locator=f"entry:{line_reference}:{name}",
    )


def _pi_payload(material: str = "plywood") -> dict:
    return {
        "schema_version": "product-intelligence-snapshot-v2",
        "shipment_product_bridge": {
            "schema_version": "shipment-product-bridge-v1",
            "links": [
                {
                    "status": "LINKED",
                    "line_item_key": "SKU:CHAIR-001",
                    "shipment_line_reference": "LT-LINE-1",
                    "product": {
                        "sku": "CHAIR-001",
                        "product_name": "Chair",
                        "components": [
                            {
                                "component_key": "CHAIR-001:row:2",
                                "description_raw": "Seat",
                                "quantity": "1",
                                "material": {
                                    "name_raw": material,
                                    "name_normalized": material.casefold(),
                                    "mass": {"kilograms": "1.2"},
                                    "source": {
                                        "document_id": "doc-1",
                                        "sheet": "BOM",
                                        "row": 2,
                                    },
                                },
                                "source": {
                                    "document_id": "doc-1",
                                    "sheet": "BOM",
                                    "row": 2,
                                },
                            }
                        ],
                    },
                }
            ],
        },
    }


def test_payload_evaluates_plant_line_without_bom_and_never_returns_zero_assessments():
    payload = build_regulatory_assessment_payload(
        product_intelligence_payload={},
        source_set={"revision_id": 9, "generation": 2, "fingerprint": "f" * 64},
        operation_fields=(
            _field("hts_code", "4407990190"),
            _field("article_component", "Sawn wood"),
        ),
        plant_line_references=("LT-LINE-1",),
    )

    assert payload["schema_version"] == "regulatory-assessment-snapshot-v3"
    assert payload["ruleset_version"] == "us-lacey-regulatory-rules-v3"
    assert payload["summary"]["subject_count"] == 1
    assert payload["summary"]["assessment_count"] == 4

    by_rule = {
        item["rule_id"]: item
        for item in payload["assessments"]
    }
    assert by_rule["HTS_APPLICABILITY"]["status"] == "PASS"
    assert by_rule["DE_MINIMIS"]["status"] == "INDETERMINATE"
    assert by_rule["SPECIAL_COMPOSITE"]["status"] == "INDETERMINATE"
    assert by_rule["SPECIAL_RECYCLED"]["status"] == "NOT_APPLICABLE"
    assert all(item["subject_ref"] == "LT-LINE-1" for item in payload["assessments"])
    assert payload["regulatory_subjects"][0]["has_product_enrichment"] is False


def test_payload_can_use_exact_linked_bom_enrichment_to_reject_plywood_composite():
    payload = build_regulatory_assessment_payload(
        product_intelligence_payload=_pi_payload("plywood"),
        source_set={"revision_id": 9, "generation": 2, "fingerprint": "f" * 64},
        operation_fields=(
            _field("hts_code", "9401692010"),
            _field("article_component", "Seat"),
        ),
        plant_line_references=("LT-LINE-1",),
    )

    special = next(
        item
        for item in payload["assessments"]
        if item["rule_id"] == "SPECIAL_COMPOSITE"
    )
    assert special["status"] == "NOT_APPLICABLE"
    assert special["reason_codes"] == ["THIN_SOLID_PLIES_NOT_SPECIAL_COMPOSITE"]
    assert special["subject_ref"] == "LT-LINE-1"
    assert payload["regulatory_subjects"][0]["has_product_enrichment"] is True


def test_payload_keeps_mdf_special_composite_indeterminate_without_due_care():
    payload = build_regulatory_assessment_payload(
        product_intelligence_payload=_pi_payload("MDF"),
        source_set={"revision_id": 9, "generation": 2, "fingerprint": "f" * 64},
        operation_fields=(
            _field("hts_code", "9401692010"),
            _field("article_component", "Seat"),
        ),
        plant_line_references=("LT-LINE-1",),
    )

    special = next(
        item
        for item in payload["assessments"]
        if item["rule_id"] == "SPECIAL_COMPOSITE"
    )
    assert special["status"] == "INDETERMINATE"
    assert special["review_required"] is True
