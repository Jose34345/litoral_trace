"""The engine-to-authority conversion cannot invent source or identity."""
from __future__ import annotations

from dataclasses import replace

import pytest

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
        source_set_revision_id=33, assurance_document_id=44,
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
