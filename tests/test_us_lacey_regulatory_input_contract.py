from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from litoral_trace.us_lacey.regulatory_input_contract import (
    INPUT_CONTRACT_SCHEMA_VERSION,
    InputStatus,
    build_regulatory_input_contract,
)


def _pi_payload(*, line_reference="LT-LINE-1"):
    return {
        "schema_version": "product-intelligence-snapshot-v2",
        "shipment_product_bridge": {
            "schema_version": "shipment-product-bridge-v1",
            "links": [
                {
                    "status": "LINKED",
                    "line_item_key": "SKU:CHAIR-001",
                    "shipment_line_reference": line_reference,
                    "candidate_line_references": [line_reference],
                    "source": {
                        "document_id": "doc-1",
                        "filename": "bom.pdf",
                        "assurance_document_id": 7,
                        "table_name": "page_1_table_1",
                    },
                    "product": {
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
                                    "mass": {
                                        "raw_value": "0.5",
                                        "raw_unit": "kg",
                                        "kilograms": "0.5",
                                    },
                                    "source": {
                                        "document_id": "doc-1",
                                        "row": 2,
                                        "locator": "pdf:page:1;table:1;row:2",
                                    },
                                },
                            }
                        ],
                    },
                }
            ],
        },
    }


def _field(
    *,
    line_reference="LT-LINE-1",
    value="9401692010",
    reviewed=True,
    validation_status="VALID",
):
    return SimpleNamespace(
        merchandise_line_reference=line_reference,
        field_name="hts_code",
        field_scope="PLANT_LINE",
        normalized_value=value,
        original_value=value,
        human_value=value if reviewed else None,
        reviewed_at=datetime.now(timezone.utc) if reviewed else None,
        validation_status=validation_status,
        source_assurance_document_id=11,
        source_page=3,
        source_locator="entry:line:1",
    )


def test_contract_accepts_only_reviewed_valid_hts10_as_supported():
    contract = build_regulatory_input_contract(
        product_intelligence_payload=_pi_payload(),
        operation_fields=(_field(),),
    )

    assert contract["schema_version"] == INPUT_CONTRACT_SCHEMA_VERSION
    item = contract["subjects"][0]
    assert item["subject_ref"] == "CHAIR-001"
    assert item["shipment_line_reference"] == "LT-LINE-1"
    hts = item["inputs"]["hts10"]
    assert hts["status"] == InputStatus.SUPPORTED.value
    assert hts["value"] == "9401692010"
    assert hts["evidence"]["source_assurance_document_id"] == 11


def test_unreviewed_hts10_remains_review_required_not_supported():
    contract = build_regulatory_input_contract(
        product_intelligence_payload=_pi_payload(),
        operation_fields=(_field(reviewed=False),),
    )

    hts = contract["subjects"][0]["inputs"]["hts10"]
    assert hts["status"] == InputStatus.REVIEW_REQUIRED.value
    assert hts["value"] == "9401692010"


def test_malformed_hts_is_not_supported_even_when_reviewed():
    contract = build_regulatory_input_contract(
        product_intelligence_payload=_pi_payload(),
        operation_fields=(_field(value="94016920"),),
    )

    hts = contract["subjects"][0]["inputs"]["hts10"]
    assert hts["status"] == InputStatus.MISSING.value
    assert hts["value"] is None


def test_bom_weight_is_explicitly_unsafe_for_de_minimis_plant_mass():
    contract = build_regulatory_input_contract(
        product_intelligence_payload=_pi_payload(),
        operation_fields=(_field(),),
    )

    item = contract["subjects"][0]
    mass = item["inputs"]["plant_mass_per_unit_kg"]
    assert mass["status"] == InputStatus.UNSAFE_SEMANTICS.value
    assert mass["value"] is None
    assert mass["observed_value"] == "0.5"
    assert mass["reason"] == "BOM_COMPONENT_WEIGHT_DOES_NOT_PROVE_PLANT_MASS_PER_UNIT"

    assert item["inputs"]["total_unit_mass_kg"]["status"] == InputStatus.MISSING.value
    assert item["inputs"]["entry_same_hts_plant_mass_kg"]["status"] == InputStatus.MISSING.value
    assert item["inputs"]["protected_status"]["status"] == InputStatus.MISSING.value


def test_unlinked_product_stays_review_required_and_never_inherits_another_line_hts():
    payload = _pi_payload()
    payload["shipment_product_bridge"]["links"][0]["status"] = "UNLINKED_REVIEW"
    payload["shipment_product_bridge"]["links"][0]["shipment_line_reference"] = None

    contract = build_regulatory_input_contract(
        product_intelligence_payload=payload,
        operation_fields=(_field(line_reference="OTHER-LINE"),),
    )

    item = contract["subjects"][0]
    assert item["link_status"] == "UNLINKED_REVIEW"
    assert item["inputs"]["hts10"]["status"] == InputStatus.MISSING.value
    assert item["inputs"]["hts10"]["value"] is None


def test_contract_summary_counts_supported_missing_review_and_unsafe_inputs():
    contract = build_regulatory_input_contract(
        product_intelligence_payload=_pi_payload(),
        operation_fields=(_field(reviewed=False),),
    )

    summary = contract["summary"]
    assert summary["subject_count"] == 1
    assert summary["supported_count"] == 0
    assert summary["review_required_count"] == 1
    assert summary["unsafe_semantics_count"] == 1
    assert summary["missing_count"] == 3
