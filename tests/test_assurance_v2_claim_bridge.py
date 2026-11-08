"""The engine-to-authority conversion cannot invent source or identity."""
from __future__ import annotations

from dataclasses import replace

import pytest

import litoral_trace.us_lacey.assurance_v2_claim_bridge as claim_bridge
from litoral_trace.lacey_engine.source_linked_claims import (
    ClaimContext, SheetCell, SourceEvidenceDocument, SupportStatus,
    from_spreadsheet_cell,
)
from litoral_trace.us_lacey.assurance_v2_claim_bridge import (
    ResolvedDocumentBinding, to_authority_candidate,
)


def make():
    document = SourceEvidenceDocument(
        document_id="doc1", original_bytes=b"workbook source",
        cells=(SheetCell("Declared", 3, 1, "Species: Quercus alba"),),
        document_version="2",
    )
    claim = from_spreadsheet_cell(
        context=ClaimContext("11", "22", "33", "run-1"),
        document=document, sheet="Declared", row=3, column=1,
        field_name="species", original_value="Quercus alba",
    )
    binding = ResolvedDocumentBinding(
        organization_id=11, operation_id=22,
        source_set_revision_id=33, source_set_fingerprint="f" * 64,
        generation=4, assurance_document_id=44, operation_document_id=55,
        document_version_number=2, original_sha256=document.sha256,
    )
    return document, claim, binding


def test_supported_claim_remains_a_candidate_not_a_human_decision():
    doc, claim, binding = make()
    candidate = to_authority_candidate(claim, parsed_original=doc, binding=binding)
    assert claim.support_status is SupportStatus.SUPPORTED
    assert candidate.state == "CANDIDATE"
    assert candidate.assurance_document_id == 44
    assert candidate.as_authority_input()["candidate_origin"] == "DOCUMENT"
    assert "actor_id" not in candidate.as_authority_input()
    assert "decision_id" not in candidate.as_authority_input()


def test_bridge_retains_complete_source_identity_without_granting_authority():
    doc, claim, binding = make()
    candidate = to_authority_candidate(claim, parsed_original=doc, binding=binding)
    wire = candidate.as_authority_input()
    assert wire["source_claim_contract_version"] == "assurance.claim.v1"
    assert wire["contract_version"] == "assurance.v2/1.0.0"
    assert wire["source_set_fingerprint"] == "f" * 64
    assert wire["generation"] == 4
    assert wire["operation_document_id"] == 55
    assert wire["document_version_number"] == 2
    assert wire["original_sha256"] == doc.sha256
    assert wire["entity_type"] == claim.entity_type
    assert wire["subject_id"] == claim.subject_id
    assert wire["raw_value"] == "Quercus alba"
    assert wire["normalized_value"] == claim.normalized_value
    assert wire["support_status"] == "SUPPORTED"
    assert wire["authority_status"] == "CANDIDATE"
    assert wire["source_locator"]["kind"] == "SHEET_CELL"
    assert wire["source_locator"]["sheet"] == "Declared"
    assert wire["source_locator"]["row"] == 3
    assert wire["source_locator"]["column"] == 1
    assert wire["conflict_status"] == "UNRESOLVED"
    assert wire["historical_origin"] is None


def test_persisted_snapshot_bridge_rejects_incomplete_or_stale_claim():
    doc, claim, binding = make()
    source = {
        "contract_version": "assurance.claim.v1",
        "candidate_state": "CANDIDATE", "interpretation_status": "PROPOSED",
        "claim_id": claim.claim_id, "organization_id": 11, "operation_id": 22,
        "source_set_revision_id": 33, "assurance_document_id": 44,
        "operation_document_id": 55, "document_version": "2",
        "original_sha256": doc.sha256, "field_name": "species",
        "original_value": "Quercus alba", "normalized_value": claim.normalized_value,
        "entity_type": claim.entity_type, "subject_id": claim.subject_id,
        "line_reference": claim.line_reference, "sku": claim.sku,
        "association_status": claim.association_status.value,
        "support_status": "SUPPORTED", "source_locator": {
            "kind": "SHEET_CELL", "sheet": "Declared", "row": 3, "column": 1,
        }, "reason_codes": [],
    }
    wire = claim_bridge.to_authority_candidate_snapshot(source, binding=binding).as_authority_input()
    assert wire["source_set_fingerprint"] == binding.source_set_fingerprint
    assert wire["generation"] == binding.generation
    assert wire["authority_status"] == "CANDIDATE"
    for invalid in (
        {**source, "contract_version": "assurance.claim.v2"},
        {**source, "organization_id": 12},
        {**source, "original_sha256": "a" * 64},
        {**source, "document_version": "3"},
        {key: value for key, value in source.items() if key != "entity_type"},
        {key: value for key, value in source.items() if key != "source_locator"},
        {**source, "candidate_state": "HUMAN_AUTHORIZED"},
        {**source, "support_status": []},
        {**source, "reason_codes": "untrusted"},
        {**source, "source_locator": {"kind": "TEXT_SPAN", "page": 0}},
    ):
        with pytest.raises(ValueError):
            claim_bridge.to_authority_candidate_snapshot(invalid, binding=binding)


@pytest.mark.parametrize("mutation", [
    lambda b: replace(b, organization_id=42),
    lambda b: replace(b, operation_id=100),
    lambda b: replace(b, source_set_revision_id=99),
    lambda b: replace(b, original_sha256="a" * 64),
    lambda b: replace(b, document_version_number=8),
])
def test_wrong_tenant_operation_revision_document_hash_or_version_rejected(mutation):
    doc, claim, binding = make()
    with pytest.raises(ValueError):
        to_authority_candidate(
            claim, parsed_original=doc, binding=mutation(binding),
        )
