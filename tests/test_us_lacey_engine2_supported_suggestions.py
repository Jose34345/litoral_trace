from __future__ import annotations

from litoral_trace.us_lacey.engine2_suggestions import supported_engine2_suggestions


def _evidence(value: str, *, document_id: str = "17", evidence_class: str = "EXPLICIT", authority: float = 25.0):
    return {
        "document_id": document_id,
        "normalized_value": value,
        "candidate_score": 91.0,
        "source_authority": authority,
        "candidate": {
            "score": 91.0,
            "raw": {
                "normalized_value": value,
                "evidence_class": evidence_class,
            },
            "provenance": {
                "page": 2,
                "source_text": f"Container Number: {value}",
                "evidence_class": evidence_class,
            },
        },
    }


def test_supported_single_value_becomes_non_authoritative_suggestion():
    payload = {
        "engine_version": "lacey-engine-2.0.0",
        "canonical_fields": {
            "container_number": {
                "state": "SUPPORTED",
                "values": [{"value": "MSKU9228574", "evidence_ids": ["e1"]}],
                "supporting_evidence": [_evidence("MSKU9228574")],
            },
            "description": {
                "state": "SUPPORTED_MULTIPLE",
                "values": [{"value": "PINE PARTS", "evidence_ids": ["e2", "e3"]}],
                "supporting_evidence": [
                    _evidence("PINE PARTS", document_id="18", authority=15.0),
                    _evidence("PINE PARTS", document_id="19", authority=20.0),
                ],
            },
        },
    }
    suggestions = {item.field_name: item for item in supported_engine2_suggestions(payload)}
    assert suggestions["container_number"].value == "MSKU9228574"
    assert suggestions["container_number"].operation_document_id == 17
    assert suggestions["merchandise_description"].value == "PINE PARTS"
    assert suggestions["merchandise_description"].operation_document_id == 19
    assert suggestions["merchandise_description"].confidence > suggestions["container_number"].confidence


def test_conflicts_multiple_values_and_inferred_evidence_are_not_promoted():
    payload = {
        "engine_version": "lacey-engine-2.0.0",
        "canonical_fields": {
            "country_of_harvest": {
                "state": "CONFLICT",
                "values": [
                    {"value": "Chile", "evidence_ids": ["a"]},
                    {"value": "Peru", "evidence_ids": ["b"]},
                ],
                "supporting_evidence": [_evidence("Chile"), _evidence("Peru", document_id="18")],
            },
            "container_number": {
                "state": "SUPPORTED_MULTIPLE",
                "values": [
                    {"value": "MSKU9228574", "evidence_ids": ["a"]},
                    {"value": "MSKU1111111", "evidence_ids": ["b"]},
                ],
                "supporting_evidence": [_evidence("MSKU9228574"), _evidence("MSKU1111111", document_id="18")],
            },
            "species": {
                "state": "SUPPORTED",
                "values": [{"value": "radiata", "evidence_ids": ["c"]}],
                "supporting_evidence": [_evidence("radiata", evidence_class="INFERRED")],
            },
        },
    }
    assert supported_engine2_suggestions(payload) == ()
