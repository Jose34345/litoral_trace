"""Assurance V2 may propose history only after complete current-PDF claim coverage."""
from __future__ import annotations

import pytest

from litoral_trace.db.models import UsLaceyEngineShipmentRun, VaultDocument
from litoral_trace.us_lacey.assurance_v2_case_reader import _case
from test_assurance_v2_core_memory import (
    session, org_and_actor, supplier_product, shipment, approve_and_promote,
)


def _ready_pair(db):
    org, reviewer = org_and_actor(db, "Coverage")
    _, product = supplier_product(db, org.id, supplier_key="MID:COVERAGE")
    origin, doc1, _, rev1, _ = shipment(
        db, org.id, product.id, reference="COVERAGE-1",
        source_value="Quercus alba",
    )
    approve_and_promote(db, org, reviewer, origin, doc1, rev1)
    op, doc, opdoc, rev, _ = shipment(
        db, org.id, product.id, reference="COVERAGE-2",
    )
    return org, op, doc, opdoc, rev


def _snapshot(db, org, op, doc, opdoc, rev, *, claims=(), status="CANDIDATE_ONLY",
              source_doc_id=None, doc_count=None):
    digest = db.get(VaultDocument, doc.vault_document_id).sha256
    source = {
        "organization_id": org.id, "operation_id": op.id,
        "source_set_revision_id": rev.id,
        "source_set_fingerprint": rev.source_set_fingerprint,
        "assurance_document_id": doc.id if source_doc_id is None else source_doc_id,
        "operation_document_id": opdoc.id,
        "document_version_number": 1,
        "original_sha256": digest,
        "claim_count": len(claims), "claims": list(claims),
    }
    db.add(UsLaceyEngineShipmentRun(
        organization_id=org.id, operation_id=op.id,
        engine_version="test-engine", ruleset_version="test-rules",
        schema_version="test-shipment",
        source_set_fingerprint=rev.source_set_fingerprint,
        document_count=1, readiness="REVIEW_REQUIRED",
        resolution_json={"assurance_v2_claim_snapshot": {
            "schema_version": "assurance.claim.snapshot.v1",
            "source_set_revision_id": rev.id,
            "source_set_fingerprint": rev.source_set_fingerprint,
            "status": status,
            "documents": [source],
            "claim_count": len(claims) if doc_count is None else doc_count,
        }},
    ))
    db.flush()
    return digest


def _claim(org, op, doc, opdoc, rev, digest, *, value="Quercus rubra",
           supported=True, placeholder=False):
    return {
        "claim_id": "source-1", "contract_version": "assurance.claim.v1",
        "candidate_state": "CANDIDATE", "interpretation_status": "PROPOSED",
        "organization_id": org.id, "operation_id": op.id,
        "source_set_revision_id": rev.id,
        "assurance_document_id": doc.id, "operation_document_id": opdoc.id,
        "document_version": "1", "original_sha256": digest,
        "field_name": "species", "original_value": value,
        "normalized_value": None if placeholder else value,
        "support_status": "UNSUPPORTED" if placeholder else (
            "SUPPORTED" if supported else "UNSUPPORTED"
        ),
        "source_locator": {"page": 1} if supported else None,
        "association_status": "UNBOUND", "line_reference": None,
        "reason_codes": ["NONFACTUAL_PLACEHOLDER"] if placeholder else [],
    }


def test_absent_snapshot_never_proves_current_botanicals_absent(session):
    org, op, _, _, _ = _ready_pair(session)
    case = _case(session, org.id, op.public_id)
    assert case is not None
    assert case.reuses == ()


def test_complete_verified_empty_claims_allow_proposed_not_canonical_reuse(session):
    org, op, doc, opdoc, rev = _ready_pair(session)
    _snapshot(session, org, op, doc, opdoc, rev)
    case = _case(session, org.id, op.public_id)
    assert case is not None
    assert len(case.reuses) == 1
    assert case.reuses[0]["value"] == "Quercus alba"
    assert case.reuses[0]["provenance"] == "REUSE_PROPOSED_NOT_CANONICAL"
    assert case.fields[0]["authority"] == "CANDIDATE"


@pytest.mark.parametrize("failure", ["PARTIAL", "wrong-document", "wrong-count"])
def test_incomplete_or_tampered_snapshot_blocks_reuse(session, failure):
    org, op, doc, opdoc, rev = _ready_pair(session)
    _snapshot(
        session, org, op, doc, opdoc, rev,
        status="PARTIAL" if failure == "PARTIAL" else "CANDIDATE_ONLY",
        source_doc_id=999999 if failure == "wrong-document" else None,
        doc_count=3 if failure == "wrong-count" else None,
    )
    case = _case(session, org.id, op.public_id)
    assert case is not None
    assert case.reuses == ()


def test_unbound_current_supported_botanical_blocks_historical_reuse(session):
    org, op, doc, opdoc, rev = _ready_pair(session)
    digest = session.get(VaultDocument, doc.vault_document_id).sha256
    candidate = _claim(org, op, doc, opdoc, rev, digest)
    _snapshot(session, org, op, doc, opdoc, rev, claims=(candidate,))
    case = _case(session, org.id, op.public_id)
    assert case is not None
    assert case.candidate_observations[0]["normalized_value"] == "Quercus rubra"
    assert case.reuses == ()


def test_nonfactual_placeholder_does_not_impersonate_new_botanical_evidence(session):
    org, op, doc, opdoc, rev = _ready_pair(session)
    digest = session.get(VaultDocument, doc.vault_document_id).sha256
    candidate = _claim(
        org, op, doc, opdoc, rev, digest,
        value="TO BE FILLED FROM VERIFIED SUPPLIER", supported=False, placeholder=True,
    )
    _snapshot(session, org, op, doc, opdoc, rev, claims=(candidate,))
    case = _case(session, org.id, op.public_id)
    assert case is not None
    assert len(case.reuses) == 1
    assert case.candidate_observations[0]["support_status"] == "UNSUPPORTED"
    assert case.fields[0]["authority"] == "CANDIDATE"


def test_forged_placeholder_reason_cannot_hide_supported_current_evidence(session):
    org, op, doc, opdoc, rev = _ready_pair(session)
    digest = session.get(VaultDocument, doc.vault_document_id).sha256
    candidate = _claim(org, op, doc, opdoc, rev, digest)
    candidate["reason_codes"] = ["NONFACTUAL_PLACEHOLDER"]
    _snapshot(session, org, op, doc, opdoc, rev, claims=(candidate,))
    case = _case(session, org.id, op.public_id)
    assert case is not None
    assert case.reuses == ()
