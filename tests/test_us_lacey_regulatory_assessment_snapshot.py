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
