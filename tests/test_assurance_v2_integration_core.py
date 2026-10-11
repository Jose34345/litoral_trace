"""Vertical boundary tests: source-linked authority, tenant case and controlled reuse."""
from __future__ import annotations

from litoral_trace.us_lacey.assurance_v2_case_reader import _case
from litoral_trace.us_lacey.exporters.assurance_v2 import evaluate_filing_gate
from test_assurance_v2_core_memory import (
    session, org_and_actor, supplier_product, shipment, approve_and_promote,
)


def test_initial_legacy_found_is_only_candidate_and_export_remains_blocked(session):
    org, actor = org_and_actor(session, "AssuranceRead")
    _, product = supplier_product(session, org.id, supplier_key="MID:ASSURANCE")
    op, doc, link, rev, field = shipment(
        session, org.id, product.id, reference="CASE-1",
        source_value="Quercus alba",
    )
    case = _case(session, org.id, op.public_id)
    assert case is not None
    assert case.documents[0]["sha256"]
    assert case.fields[0]["authority"] == "CANDIDATE"
    assert case.identities[0]["status"] == "REVIEW_REQUIRED"
    assert not evaluate_filing_gate(case).ready
    assert any(block.code == "EXPORT_NOT_AUTHORIZED"
               for block in evaluate_filing_gate(case).blockers)
    # An identical public operation UUID in a different organization is invisible.
    assert _case(session, org.id + 123456, op.public_id) is None


def test_human_decision_carries_forward_and_second_shipment_only_proposes_reuse(session, monkeypatch):
    org, actor = org_and_actor(session, "AssuranceLongitudinal")
    _, product = supplier_product(session, org.id, supplier_key="MID:LONG")
    op1, doc1, _, rev1, field1 = shipment(
        session, org.id, product.id, reference="CASE-MEM-1",
        source_value="Quercus alba",
    )
    decision, memory = approve_and_promote(
        session, org, actor, op1, doc1, rev1,
    )
    field1.human_value = "Quercus alba"
    session.flush()
    one = _case(session, org.id, op1.public_id)
    assert one.fields[0]["authority"] == "HUMAN_CONFIRMED"
    assert one.fields[0]["decision_id"] == str(decision.public_id)
    assert one.fields[0]["evidence_ids"]
    assert not evaluate_filing_gate(one).ready

    op2, doc2, _, rev2, field2 = shipment(
        session, org.id, product.id, reference="CASE-MEM-2",
    )
    assert not _case(session, org.id, op2.public_id).reuses
    monkeypatch.setattr("litoral_trace.us_lacey.assurance_v2_case_reader._source_claim_observations", lambda *a, **k: ())
    two = _case(session, org.id, op2.public_id)
    assert two.fields[0]["authority"] == "CANDIDATE"
    assert not two.fields[0]["value"]
    assert two.reuses and two.reuses[0]["value"] == "Quercus alba"
    assert two.reuses[0]["status"] == "ELIGIBLE"
    assert two.reuses[0]["provenance"] == "REUSE_PROPOSED_NOT_CANONICAL"
    assert not evaluate_filing_gate(two).ready
    assert _case(session, org.id + 987, op2.public_id) is None
