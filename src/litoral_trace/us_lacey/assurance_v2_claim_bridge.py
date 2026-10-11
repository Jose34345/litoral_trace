"""Strict adapter from document-engine candidates to Assurance V2 authority inputs.

This module is deliberately pure: it never promotes a source-backed observation
to human authority, never writes canonical shipment truth, and never accepts
caller-supplied external document IDs without a database-resolved binding.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from uuid import NAMESPACE_URL, UUID, uuid5

from litoral_trace.lacey_engine.source_linked_claims import (
    CLAIM_CONTRACT_VERSION,
    AssociationStatus,
    SourceEvidenceDocument,
    SourceLinkedClaim,
    SupportStatus,
    verify_claim_anchor,
)

AUTHORITY_CONTRACT_VERSION = "assurance.v2/1.0.0"


@dataclass(frozen=True, slots=True)
class ResolvedDocumentBinding:
    """Identity verified against tenant-owned operation/source-set DB rows."""

    organization_id: int
    operation_id: int
    source_set_revision_id: int
    assurance_document_id: int
    document_version_number: int
    original_sha256: str

    def __post_init__(self) -> None:
        if min(
            self.organization_id, self.operation_id,
            self.source_set_revision_id, self.assurance_document_id,
            self.document_version_number,
        ) < 1:
            raise ValueError("Database document identity must be positive")
        if len(self.original_sha256) != 64 or any(
            c not in "0123456789abcdef" for c in self.original_sha256
        ):
            raise ValueError("Database document hash is invalid")


@dataclass(frozen=True, slots=True)
class AuthorityCandidate:
    """Only candidate authority; no inferred human decision or memory promotion."""

    contract_version: str
    source_claim_contract_version: str
    claim_id: UUID
    state: str
    support_status: str
    organization_id: int
    operation_id: int
    source_set_revision_id: int
    assurance_document_id: int
    document_version_number: int
    original_sha256: str
    field_name: str
    raw_value: str
    normalized_value: str | None
    source_locator: str | None
    line_reference: str | None
    sku: str | None
    association_status: str
    reason_codes: tuple[str, ...]

    def as_authority_input(self) -> dict:
        return {
            "contract_version": self.contract_version,
            "claim_id": str(self.claim_id),
            "candidate_origin": "DOCUMENT",
            "state": self.state,
            "support_status": self.support_status,
            "organization_id": self.organization_id,
            "operation_id": self.operation_id,
            "source_set_revision_id": self.source_set_revision_id,
            "assurance_document_id": self.assurance_document_id,
            "document_version_number": self.document_version_number,
            "original_sha256": self.original_sha256,
            "field_name": self.field_name,
            "raw_value": self.raw_value,
            "normalized_value": self.normalized_value,
            "source_locator": self.source_locator,
            "line_reference": self.line_reference,
            "sku": self.sku,
            "association_status": self.association_status,
            "reason_codes": list(self.reason_codes),
        }


def to_authority_candidate(
    claim: SourceLinkedClaim, *, parsed_original: SourceEvidenceDocument,
    binding: ResolvedDocumentBinding,
) -> AuthorityCandidate:
    """Verify bytes, source anchor and *database* identity before adapting.

    Crucially SUPPORTED means only a literal anchor; a document can support
    a value without proving which supplier/product/line it belongs to.
    """
    if claim.contract_version != CLAIM_CONTRACT_VERSION:
        raise ValueError("Unknown extraction claim major/version")
    if claim.candidate_state != "CANDIDATE" or claim.interpretation_status != "PROPOSED":
        raise ValueError("Extraction attempted to grant authority")
    if (
        str(claim.organization_id) != str(binding.organization_id)
        or str(claim.operation_id) != str(binding.operation_id)
        or str(claim.source_set_revision_id) != str(binding.source_set_revision_id)
    ):
        raise ValueError("Cross-tenant or stale source-set claim")
    if (
        claim.document_id != parsed_original.document_id
        or claim.original_document_sha256 != parsed_original.sha256
        or claim.original_document_sha256 != binding.original_sha256
        or not claim.document_version
        or str(claim.document_version) != str(binding.document_version_number)
    ):
        raise ValueError("Document binding/version/hash mismatch")
    anchored = verify_claim_anchor(claim, parsed_original)
    if claim.support_status is SupportStatus.SUPPORTED and not anchored:
        raise ValueError("SUPPORT claim lacks replay-verifiable evidence")

    reasons = set(claim.issues)
    if not anchored:
        reasons.add("NO_VERIFIED_SOURCE_ANCHOR")
    if claim.association_status is AssociationStatus.UNBOUND:
        reasons.add("ENTITY_IDENTITY_UNBOUND")
    # Do not call this HUMAN_AUTHORIZED or assign canonical authority here.
    state = "CANDIDATE" if anchored else "REVIEW_REQUIRED"
    locator = None
    if claim.locator is not None and anchored:
        locator = str(claim.locator)
    return AuthorityCandidate(
        contract_version=AUTHORITY_CONTRACT_VERSION,
        source_claim_contract_version=claim.contract_version,
        claim_id=uuid5(NAMESPACE_URL, f"litoral:claim:{claim.claim_id}"),
        state=state,
        support_status=claim.support_status.value,
        organization_id=binding.organization_id,
        operation_id=binding.operation_id,
        source_set_revision_id=binding.source_set_revision_id,
        assurance_document_id=binding.assurance_document_id,
        document_version_number=binding.document_version_number,
        original_sha256=binding.original_sha256,
        field_name=claim.field_name,
        raw_value=claim.original_value,
        normalized_value=claim.normalized_value,
        source_locator=locator,
        line_reference=claim.line_reference,
        sku=claim.sku,
        association_status=claim.association_status.value,
        reason_codes=tuple(sorted(reasons)),
    )
