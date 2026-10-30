"""P0/P1 unified shipment cockpit: source scope, authorization and no duplicate work."""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import re
from types import SimpleNamespace

from litoral_trace.web import shipment_v2_unified as unified
from test_assurance_v2_outputs_core import OPERATION_ID, make_case

ROOT = Path(__file__).resolve().parents[1]
OP_TEMPLATE = ROOT / "src/litoral_trace/templates/us_lacey/operation_detail.html"
V2_TEMPLATE = ROOT / "src/litoral_trace/templates/us_lacey/assurance_v2_case.html"
SHARED = ROOT / "src/litoral_trace/templates/us_lacey/fragments/assurance_unified_actions.html"


def _request(org_ids=(11,)):
    app = SimpleNamespace(state=SimpleNamespace(assurance_v2_lab_org_ids=frozenset(org_ids)))
    return SimpleNamespace(scope={"app": app}, cookies={"us_lacey_session": "fixture-session"})


def test_owner_lab_is_opt_in_and_normal_shipments_remain_separate():
    identity = SimpleNamespace(organization_id=11)
    assert unified.is_private_v2_shipment(_request(), identity, "V2LAB-PACK2")
    assert not unified.is_private_v2_shipment(_request(), identity, "PROD-IMPORT-1")
    assert not unified.is_private_v2_shipment(_request(org_ids=(22,)), identity, "V2LAB-PACK2")
    assert not unified.is_private_v2_shipment(
        _request(), SimpleNamespace(organization_id=22), "V2LAB-PACK2"
    )


def test_v2_shipment_actions_use_current_tenant_case_and_source_links(monkeypatch):
    case = make_case(approved=False)
    captured = []
    def get_case(**kwargs):
        captured.append(kwargs)
        return case
    monkeypatch.setattr(unified, "read_tenant_assurance_case", get_case)
    monkeypatch.setattr(unified, "preview_batch_review", lambda value: [
        {"field_id": 101, "claim_id": "claim-a", "field_name": "importer_name",
         "line_reference": "__shipment__", "document_id": "doc-invoice",
         "page": 2, "value": "Acme"},
        {"field_id": 102, "claim_id": "claim-b", "field_name": "consignee_name",
         "line_reference": "__shipment__", "document_id": "doc-other",
         "page": 1, "value": "Acme"},
    ])
    monkeypatch.setattr(unified, "us_lacey_csrf_token", lambda **kwargs: "signed-v2-csrf")
    monkeypatch.delenv("US_LACEY_ASSURANCE_V2_REVIEW_JOURNAL", raising=False)
    ctx = unified.unified_v2_review_context(
        request=_request(),
        identity=SimpleNamespace(organization_id=11),
        operation_public_id=OPERATION_ID,
    )
    assert captured == [{
        "organization_id": 11, "operation_public_id": OPERATION_ID
    }]
    assert ctx["available"] is True
    assert ctx["csrf_token"] == "signed-v2-csrf"
    assert ctx["case"]["operation_id"] == OPERATION_ID
    assert ctx["batch_choices"][0]["url"] == (
        f"/assurance-v2/operations/{OPERATION_ID}/documents/doc-invoice#page=2"
    )
    assert ctx["batch_choices"][1]["url"] is None
    assert ctx["review_workspace"]["verified_count"] == 0
    assert ctx["required_evidence"]["verified"] == 0


def test_unavailable_v2_authority_never_falls_back_to_legacy(monkeypatch):
    monkeypatch.setattr(unified, "read_tenant_assurance_case", lambda **kwargs: None)
    result = unified.unified_v2_review_context(
        request=_request(), identity=SimpleNamespace(organization_id=11),
        operation_public_id=OPERATION_ID,
    )
    assert result["available"] is False
    markup = OP_TEMPLATE.read_text(encoding="utf-8")
    assert "{% if private_v2 %}" in markup
    assert "V2 case currently unavailable" in markup
    assert "{% elif not bulk_upload_rejection %}" in markup
    assert "fragments/assurance_unified_actions.html" in markup


def test_single_review_surface_three_primary_sections_and_secondary_audit():
    op = OP_TEMPLATE.read_text(encoding="utf-8")
    v2 = V2_TEMPLATE.read_text(encoding="utf-8")
    actions = SHARED.read_text(encoding="utf-8")
    assert op.count('fragments/assurance_unified_actions.html') == 1
    assert v2.count('fragments/assurance_unified_actions.html') == 1
    assert 'data-assurance-case' in op
    assert 'data-assurance-case' in v2
    assert 'data-batch-review' in actions
    assert 'class="identity-form"' in actions
    assert 'name="explicit_confirmation"' in actions
    nav = re.search(r"<nav[^>]*data-shipment-section-nav.*?</nav>", op, re.S).group()
    assert re.findall(r'href="([^"]+)"', nav) == [
        "#operation-workspace", "#documents", "#declaration-package",
    ]
    assert 'href="#activity-audit-log"' in op
    summary = (ROOT / "src/litoral_trace/templates/us_lacey/fragments/shipment_workspace_summary.html").read_text(encoding="utf-8")
    assert 'shipment_workspace_summary.html' in op
    assert 'id="shipment-summary"' in summary  # legacy destination remains available
    assert 'data-private-v2-boundary' in op
    assert 'data-unified-overview-counts' in summary
    assert 'data-v2-compliance-package' in op
    assert 'data-unified-audit' in op
    assert 'data-secondary-reprocess' in op
    # Legacy stage stepper remains available outside owner-only laboratory.
    assert '{% include "us_lacey/fragments/workflow_stepper_shell.html" %}' in op
    assert "Workflow stages · view progress" in op
    assert "data-workflow-stage-details" in op


def test_entry_requirement_remains_blocking_and_v2_lab_cannot_issue():
    case = make_case(approved=False)
    case = replace(case, fields=tuple(
        f for f in case.fields if f["name"] != "filing_entry_reference"
    ))
    from litoral_trace.us_lacey.exporters.assurance_v2 import evaluate_filing_gate
    gate = evaluate_filing_gate(case)
    assert not gate.ready
    assert any(
        blocker.code == "MISSING_REQUIRED_FIELD"
        and "filing_entry_reference" in blocker.subject
        for blocker in gate.blockers
    )
    op = OP_TEMPLATE.read_text(encoding="utf-8")
    assert "unassigned Entry Number can stay pending before entry" in op
    assert "Private lab · Issuance disabled" in op
