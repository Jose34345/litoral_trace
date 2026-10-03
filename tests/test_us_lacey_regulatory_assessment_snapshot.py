from __future__ import annotations

import hashlib
import importlib
import json
from datetime import datetime, timezone
from types import SimpleNamespace


def _model_module():
    return importlib.import_module("litoral_trace.db.models.us_lacey_regulatory_assessment")


def _snapshot_module():
    return importlib.import_module("litoral_trace.us_lacey.regulatory_assessment_snapshot")


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
        json.dumps(
            payload_a,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()

    assert snapshot.fingerprint_rule_inputs(payload_a) == expected
    assert snapshot.fingerprint_rule_inputs(payload_b) == expected
    assert (
        snapshot.fingerprint_rule_inputs(
            {**payload_a, "ruleset_version": "rules-v2"}
        )
        != expected
    )


def test_assessment_uses_primary_plant_line_without_product_intelligence():
    snapshot = _snapshot_module()

    payload = snapshot.build_regulatory_assessment_payload(
        product_intelligence_payload={},
        source_set={
            "revision_id": 9,
            "generation": 2,
            "fingerprint": "f" * 64,
        },
        operation_fields=(
            _field("hts_code", "4407990190"),
            _field("article_component", "Sawn wood"),
        ),
        plant_line_references=("LT-LINE-1",),
    )

    assert payload["ruleset_version"] == "us-lacey-regulatory-rules-v3"
    assert payload["summary"]["subject_count"] == 1
    assert payload["summary"]["assessment_count"] == 4

    by_rule = {item["rule_id"]: item for item in payload["assessments"]}
    assert by_rule["HTS_APPLICABILITY"]["status"] == "PASS"

    de_minimis = by_rule["DE_MINIMIS"]
    assert de_minimis["status"] == "INDETERMINATE"
    assert "MISSING_REQUIRED_INPUTS" in de_minimis["reason_codes"]
    assert de_minimis["evidence_refs"] == [
        {
            "source_type": "REVIEWED_HTS10",
            "source_id": "11",
            "locator": "entry:LT-LINE-1:hts_code",
        }
    ]

    contract = payload["regulatory_input_contract"]
    assert contract["schema_version"] == "regulatory-input-contract-v2"
    assert contract["subjects"][0]["subject_ref"] == "LT-LINE-1"
    assert contract["subjects"][0]["link_status"] == "NO_BOM_LINK"


def test_assessment_never_uses_unsafe_bom_weight_as_de_minimis_mass():
    snapshot = _snapshot_module()
    product_payload = {
        "shipment_product_bridge": {
            "links": [
                {
                    "status": "LINKED",
                    "line_item_key": "SKU:CHAIR-001",
                    "shipment_line_reference": "LT-LINE-1",
                    "product": {
                        "sku": "CHAIR-001",
                        "components": [
                            {
                                "component_key": "LEG",
                                "description_raw": "Leg",
                                "material": {
                                    "name_raw": "Rubberwood",
                                    "mass": {"kilograms": "0.5"},
                                },
                            }
                        ],
                    },
                }
            ]
        }
    }

    payload = snapshot.build_regulatory_assessment_payload(
        product_intelligence_payload=product_payload,
        source_set={},
        operation_fields=(
            _field("hts_code", "9401692010"),
            _field("article_component", "Leg"),
        ),
        plant_line_references=("LT-LINE-1",),
    )

    de_minimis = next(
        item
        for item in payload["assessments"]
        if item["rule_id"] == "DE_MINIMIS"
    )
    assert de_minimis["status"] == "INDETERMINATE"
    assert "MISSING_REQUIRED_INPUTS" in de_minimis["reason_codes"]
    assert "0.5" not in str(de_minimis["calculation_trace"])

    contract_mass = payload["regulatory_input_contract"]["subjects"][0][
        "inputs"
    ]["plant_mass_per_unit_kg"]
    assert contract_mass["status"] == "UNSAFE_SEMANTICS"
    assert contract_mass["observed_value"] == "0.5"
