from __future__ import annotations

import hashlib
import importlib
import json


def _model_module():
    return importlib.import_module("litoral_trace.db.models.us_lacey_regulatory_assessment")


def _snapshot_module():
    return importlib.import_module("litoral_trace.us_lacey.regulatory_assessment_snapshot")


def test_regulatory_assessment_model_exposes_tenant_source_set_ruleset_contract():
    model = _model_module().UsLaceyRegulatoryAssessmentSnapshot
    table = model.__table__

    assert table.name == "us_lacey_regulatory_assessment_snapshots"
    assert {
        "organization_id",
        "operation_id",
        "source_set_revision_id",
        "generation",
        "source_set_fingerprint",
        "ruleset_version",
        "input_fingerprint",
        "status",
        "assessment_count",
        "indeterminate_count",
        "payload_json",
    } <= set(table.c.keys())
    assert any(
        constraint.name == "fk_lacey_reg_assessment_operation_tenant"
        for constraint in table.foreign_key_constraints
    )
    assert any(
        constraint.name == "fk_lacey_reg_assessment_revision_tenant"
        for constraint in table.foreign_key_constraints
    )
    assert any(
        constraint.name == "uq_lacey_reg_assessment_revision_ruleset"
        for constraint in table.constraints
    )


def test_regulatory_assessment_model_is_exported_from_unified_models_module():
    models = importlib.import_module("litoral_trace.db.models")
    assert hasattr(models, "UsLaceyRegulatoryAssessmentSnapshot")


def test_rule_input_fingerprint_is_stable_and_ruleset_sensitive():
    snapshot = _snapshot_module()
    payload_a = {
        "ruleset_version": "rules-v1",
        "facts": {"mass": "2.900", "status": "UNKNOWN"},
    }
    payload_b = {
        "facts": {"status": "UNKNOWN", "mass": "2.900"},
        "ruleset_version": "rules-v1",
    }
    expected = hashlib.sha256(
        json.dumps(payload_a, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()

    assert snapshot.fingerprint_rule_inputs(payload_a) == expected
    assert snapshot.fingerprint_rule_inputs(payload_b) == expected
    assert snapshot.fingerprint_rule_inputs({**payload_a, "ruleset_version": "rules-v2"}) != expected


def test_assessment_consumes_supported_hts_but_stays_indeterminate_without_mass_inputs():
    snapshot = _snapshot_module()
    product_payload = {
        "sources": [
            {
                "tables": [
                    {
                        "compositions": [
                            {
                                "sku": "CHAIR-001",
                                "components": [],
                            }
                        ]
                    }
                ]
            }
        ]
    }
    contract = {
        "schema_version": "regulatory-input-contract-v1",
        "subjects": [
            {
                "subject_ref": "CHAIR-001",
                "shipment_line_reference": "LT-LINE-1",
                "link_status": "LINKED",
                "inputs": {
                    "hts10": {
                        "status": "SUPPORTED",
                        "value": "9401692010",
                        "evidence": {
                            "source_assurance_document_id": 11,
                            "source_locator": "entry:line:1",
                        },
                    }
                },
            }
        ],
    }

    payload = snapshot.build_regulatory_assessment_payload(
        product_intelligence_payload=product_payload,
        source_set={
            "revision_id": 9,
            "generation": 2,
            "fingerprint": "f" * 64,
        },
        regulatory_input_contract=contract,
    )

    de_minimis = next(
        item for item in payload["assessments"] if item["rule_id"] == "DE_MINIMIS"
    )
    assert de_minimis["status"] == "INDETERMINATE"
    assert "MISSING_REQUIRED_INPUTS" in de_minimis["reason_codes"]
    assert de_minimis["calculation_trace"] == {}
    assert de_minimis["evidence_refs"] == [
        {
            "source_type": "REVIEWED_HTS10",
            "source_id": "11",
            "locator": "entry:line:1",
        }
    ]
    assert payload["regulatory_input_contract"] == contract


def test_assessment_never_uses_unsafe_bom_weight_as_de_minimis_mass():
    snapshot = _snapshot_module()
    product_payload = {
        "sources": [
            {
                "tables": [
                    {
                        "compositions": [
                            {
                                "sku": "CHAIR-001",
                                "components": [
                                    {
                                        "component_key": "LEG",
                                        "material": {
                                            "name_raw": "Rubberwood",
                                            "mass": {"kilograms": "0.5"},
                                        },
                                    }
                                ],
                            }
                        ]
                    }
                ]
            }
        ]
    }
    contract = {
        "schema_version": "regulatory-input-contract-v1",
        "subjects": [
            {
                "subject_ref": "CHAIR-001",
                "inputs": {
                    "hts10": {"status": "SUPPORTED", "value": "9401692010"},
                    "plant_mass_per_unit_kg": {
                        "status": "UNSAFE_SEMANTICS",
                        "value": None,
                        "observed_value": "0.5",
                    },
                },
            }
        ],
    }

    payload = snapshot.build_regulatory_assessment_payload(
        product_intelligence_payload=product_payload,
        source_set={},
        regulatory_input_contract=contract,
    )

    de_minimis = next(
        item for item in payload["assessments"] if item["rule_id"] == "DE_MINIMIS"
    )
    assert de_minimis["status"] == "INDETERMINATE"
    assert "MISSING_REQUIRED_INPUTS" in de_minimis["reason_codes"]
    assert "0.5" not in str(de_minimis["calculation_trace"])
