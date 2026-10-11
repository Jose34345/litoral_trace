"""Strict adapter from document-engine candidates to Assurance V2 authority inputs.

This module is deliberately pure: it never promotes a source-backed observation
to human authority, never writes canonical shipment truth, and never accepts
caller-supplied external document IDs without a database-resolved binding.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any, Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from litoral_trace.lacey_engine.source_linked_claims import (
    CLAIM_CONTRACT_VERSION,
    AssociationStatus,
    SourceEvidenceDocument,
    SourceLocator,
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
    source_set_fingerprint: str
    generation: int
    assurance_document_id: int
    operation_document_id: int
    document_version_number: int
    original_sha256: str

    def __post_init__(self) -> None:
        if min(
            self.organization_id, self.operation_id,
            self.source_set_revision_id, self.generation,
            self.assurance_document_id, self.operation_document_id,
            self.document_version_number,
        ) < 1:
            raise ValueError("Database document identity must be positive")
        if len(self.source_set_fingerprint) != 64 or any(
            c not in "0123456789abcdef" for c in self.source_set_fingerprint
        ):
            raise ValueError("Source-set fingerprint is invalid")
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
    source_set_fingerprint: str
    generation: int
    assurance_document_id: int
    operation_document_id: int
    document_version_number: int
    original_sha256: str
    field_name: str
    raw_value: str
    normalized_value: str | None
    source_locator: dict[str, Any] | None
    entity_type: str
    subject_id: str | None
    line_reference: str | None
    sku: str | None
    association_status: str
    reason_codes: tuple[str, ...]
    source_claim: dict[str, Any]
    conflict_status: str = "UNRESOLVED"
    historical_origin: dict[str, Any] | None = None

    def as_authority_input(self) -> dict:
        return {
            "contract_version": self.contract_version,
            "source_claim_contract_version": self.source_claim_contract_version,
            "claim_id": str(self.claim_id),
            "candidate_origin": "DOCUMENT",
            "state": self.state,
            "support_status": self.support_status,
            "authority_status": "CANDIDATE",
            "organization_id": self.organization_id,
            "operation_id": self.operation_id,
            "source_set_revision_id": self.source_set_revision_id,
            "source_set_fingerprint": self.source_set_fingerprint,
            "generation": self.generation,
            "assurance_document_id": self.assurance_document_id,
            "operation_document_id": self.operation_document_id,
            "document_version_number": self.document_version_number,
            "original_sha256": self.original_sha256,
            "field_name": self.field_name,
            "raw_value": self.raw_value,
            "normalized_value": self.normalized_value,
            "source_locator": self.source_locator,
            "entity_type": self.entity_type,
            "subject_id": self.subject_id,
            "line_reference": self.line_reference,
            "sku": self.sku,
            "association_status": self.association_status,
            "reason_codes": list(self.reason_codes),
            "conflict_status": self.conflict_status,
            "historical_origin": self.historical_origin,
            "source_claim": self.source_claim,
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
        locator = asdict(claim.locator)
    return AuthorityCandidate(
        contract_version=AUTHORITY_CONTRACT_VERSION,
        source_claim_contract_version=claim.contract_version,
        claim_id=uuid5(NAMESPACE_URL, f"litoral:claim:{claim.claim_id}"),
        state=state,
        support_status=claim.support_status.value,
        organization_id=binding.organization_id,
        operation_id=binding.operation_id,
        source_set_revision_id=binding.source_set_revision_id,
        source_set_fingerprint=binding.source_set_fingerprint,
        generation=binding.generation,
        assurance_document_id=binding.assurance_document_id,
        operation_document_id=binding.operation_document_id,
        document_version_number=binding.document_version_number,
        original_sha256=binding.original_sha256,
        field_name=claim.field_name,
        raw_value=claim.original_value,
        normalized_value=claim.normalized_value,
        source_locator=locator,
        entity_type=claim.entity_type,
        subject_id=claim.subject_id,
        line_reference=claim.line_reference,
        sku=claim.sku,
        association_status=claim.association_status.value,
        reason_codes=tuple(sorted(reasons)),
        source_claim=claim.to_dict(),
    )


def to_authority_candidate_snapshot(
    source: Mapping[str, Any], *, binding: ResolvedDocumentBinding,
) -> AuthorityCandidate:
    """Validate a persisted, byte-verified claim against current DB identity.

    The ingestion boundary already replayed the parser anchor against original
    bytes. This read boundary cannot invent that proof: it verifies the stored
    contract, current document/hash/version and the locator shape again.
    """
    required = {
        "contract_version", "candidate_state", "interpretation_status",
        "claim_id", "organization_id", "operation_id",
        "source_set_revision_id", "assurance_document_id",
        "operation_document_id", "document_version", "original_sha256",
        "field_name", "original_value", "normalized_value", "entity_type",
        "subject_id", "line_reference", "sku", "association_status",
        "support_status", "source_locator", "reason_codes",
    }
    if not required <= source.keys():
        raise ValueError("Persisted claim contract is incomplete")
    if (source["contract_version"] != CLAIM_CONTRACT_VERSION
            or source["candidate_state"] != "CANDIDATE"
            or source["interpretation_status"] != "PROPOSED"):
        raise ValueError("Persisted claim contract or authority state is invalid")
    if any(source[key] != expected for key, expected in (
        ("organization_id", binding.organization_id),
        ("operation_id", binding.operation_id),
        ("source_set_revision_id", binding.source_set_revision_id),
        ("assurance_document_id", binding.assurance_document_id),
        ("operation_document_id", binding.operation_document_id),
        ("original_sha256", binding.original_sha256),
    )) or str(source["document_version"]) != str(binding.document_version_number):
        raise ValueError("Persisted claim does not match current source identity")
    if not isinstance(source["claim_id"], str) or not source["claim_id"]:
        raise ValueError("Persisted claim ID is missing")
    if not isinstance(source["field_name"], str) or not source["field_name"].strip():
        raise ValueError("Persisted claim field is missing")
    if not isinstance(source["original_value"], str):
        raise ValueError("Persisted original value is missing")
    if source["normalized_value"] is not None and not isinstance(source["normalized_value"], str):
        raise ValueError("Persisted normalized value is invalid")
    if not isinstance(source["entity_type"], str) or not source["entity_type"].strip():
        raise ValueError("Persisted entity type is missing")
    if source["subject_id"] is not None and not isinstance(source["subject_id"], str):
        raise ValueError("Persisted subject identity is invalid")
    try:
        support = SupportStatus(source["support_status"])
        association = AssociationStatus(source["association_status"])
    except ValueError as exc:
        raise ValueError("Persisted claim status is incompatible") from exc
    if association is AssociationStatus.EXPLICIT_SKU and not source["sku"]:
        raise ValueError("Exact SKU association lacks SKU")
    locator = source["source_locator"]
    if locator is not None:
        if not isinstance(locator, Mapping):
            raise ValueError("Persisted source locator is invalid")
        try:
            SourceLocator(**{key: value for key, value in locator.items() if key != "bbox"})
        except (TypeError, ValueError) as exc:
            raise ValueError("Persisted source locator is invalid") from exc
    if support is SupportStatus.SUPPORTED and locator is None:
        raise ValueError("Supported claim lacks source locator")
    reasons = source["reason_codes"]
    if not isinstance(reasons, (list, tuple)) or any(not isinstance(x, str) for x in reasons):
        raise ValueError("Persisted claim reason codes are invalid")
    return AuthorityCandidate(
        contract_version=AUTHORITY_CONTRACT_VERSION,
        source_claim_contract_version=CLAIM_CONTRACT_VERSION,
        claim_id=uuid5(NAMESPACE_URL, f"litoral:claim:{source['claim_id']}"),
        state="CANDIDATE" if locator is not None else "REVIEW_REQUIRED",
        support_status=support.value,
        organization_id=binding.organization_id,
        operation_id=binding.operation_id,
        source_set_revision_id=binding.source_set_revision_id,
        source_set_fingerprint=binding.source_set_fingerprint,
        generation=binding.generation,
        assurance_document_id=binding.assurance_document_id,
        operation_document_id=binding.operation_document_id,
        document_version_number=binding.document_version_number,
        original_sha256=binding.original_sha256,
        field_name=source["field_name"],
        raw_value=source["original_value"],
        normalized_value=source["normalized_value"],
        source_locator=dict(locator) if locator is not None else None,
        entity_type=source["entity_type"],
        subject_id=source["subject_id"],
        line_reference=source["line_reference"],
        sku=source["sku"],
        association_status=association.value,
        reason_codes=tuple(reasons),
        source_claim=dict(source),
    )
