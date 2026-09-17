from __future__ import annotations

from litoral_trace.us_lacey.regulatory_assessment_snapshot import build_regulatory_assessment_payload


def _pi_payload(material: str = "plywood") -> dict:
    return {
        "schema_version": "product-intelligence-snapshot-v1",
        "sources": [
            {
                "filename": "BOM.xlsx",
                "tables": [
                    {
                        "name": "BOM",
                        "compositions": [
                            {
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
                                            "source": {"document_id": "doc-1", "sheet": "BOM", "row": 2},
                                        },
                                        "source": {"document_id": "doc-1", "sheet": "BOM", "row": 2},
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
    }


def test_payload_fails_closed_for_de_minimis_when_exact_entry_facts_are_absent():
    payload = build_regulatory_assessment_payload(
        product_intelligence_payload=_pi_payload("Rubberwood"),
        source_set={"revision_id": 9, "generation": 2, "fingerprint": "f" * 64},
    )

    de_minimis = [item for item in payload["assessments"] if item["rule_id"] == "DE_MINIMIS"]
    assert len(de_minimis) == 1
    assert de_minimis[0]["status"] == "INDETERMINATE"
    assert de_minimis[0]["review_required"] is True
    assert "MISSING_REQUIRED_INPUTS" in de_minimis[0]["reason_codes"]
    assert payload["summary"]["indeterminate_count"] >= 1


def test_payload_can_deterministically_reject_plywood_special_composite():
    payload = build_regulatory_assessment_payload(
        product_intelligence_payload=_pi_payload("plywood"),
        source_set={"revision_id": 9, "generation": 2, "fingerprint": "f" * 64},
    )

    special = [item for item in payload["assessments"] if item["rule_id"] == "SPECIAL_COMPOSITE"]
    assert len(special) == 1
    assert special[0]["status"] == "FAIL"
    assert special[0]["reason_codes"] == ["THIN_SOLID_PLIES_DISQUALIFY"]
    assert special[0]["subject_ref"] == "CHAIR-001:row:2"
    assert special[0]["evidence_refs"][0]["source_id"] == "doc-1"


def test_payload_keeps_mdf_special_composite_indeterminate_without_due_care_and_multi_plant_evidence():
    payload = build_regulatory_assessment_payload(
        product_intelligence_payload=_pi_payload("MDF"),
        source_set={"revision_id": 9, "generation": 2, "fingerprint": "f" * 64},
    )
    special = [item for item in payload["assessments"] if item["rule_id"] == "SPECIAL_COMPOSITE"]
    assert special[0]["status"] == "INDETERMINATE"
    assert special[0]["review_required"] is True
