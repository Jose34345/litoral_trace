"""Source-linked candidate projection inside Engine 2's existing source-set snapshot.

No new document/evidence database and no authority escalation. The original
bytes are used transiently to prove source claims; only a bounded derivative
with tenant/source-set/document identities and hashes may be persisted.
"""
from __future__ import annotations

from hashlib import sha256
from typing import Any

from litoral_trace.lacey_engine.domain import (
    AdmittedCandidate, BundleResolution, EvidenceClass, Provenance, RawCandidate,
)
from litoral_trace.lacey_engine.labeled_pdf_fields import (
    EXTRACTOR_VERSION, extract_explicit_field_spans,
)
from litoral_trace.lacey_engine.source_linked_claims import (
    CLAIM_CONTRACT_VERSION, ClaimContext, SourceEvidenceDocument,
    claims_from_engine2_resolution, from_engine2_candidate, verify_claim_anchor,
)

ASSURANCE_CLAIMS_SNAPSHOT_VERSION = "assurance.claim.snapshot.v1"
MAX_SOURCE_CLAIMS = 1600

# A literal, verifiable PDF span is not necessarily a factual assertion.
# Template instructions in missing botanical fields must remain candidate-only,
# never be described as supported botanical facts.
_PLACEHOLDER_DOMAINS = frozenset({
    "genus", "species", "country_of_harvest", "plant_quantity",
    "metric_unit", "article_component", "percent_recycled",
})
_PLACEHOLDERS = frozenset({
    "tbd", "tbc", "unknown", "pending", "not provided", "not available",
    "to be determined", "to be confirmed", "to be filled",
})
_PLACEHOLDER_PREFIXES = (
    "TO BE FILLED FROM ", "TO BE CONFIRMED FROM ",
    "TO BE PROVIDED BY ", "PENDING SUPPLIER ",
    "REFER TO VERIFIED ", "SEE SUPPLIER ",
)


def _nonfactual_placeholder(field_name: str, value: str) -> bool:
    if field_name not in _PLACEHOLDER_DOMAINS:
        return False
    normalized = " ".join(str(value or "").upper().split())
    return normalized in {v.upper() for v in _PLACEHOLDERS} or any(
        normalized.startswith(prefix) for prefix in _PLACEHOLDER_PREFIXES
    )


def _project_claim(claim, *, document_id: int, operation_document_id: int) -> dict[str, Any]:
    locator = claim.locator
    placeholder = _nonfactual_placeholder(
        claim.field_name, claim.normalized_value or claim.original_value
    )
    # Preserve original text/location; downgrade only its factual support
    # status so a placeholder cannot masquerade as corroborated evidence.
    support = "UNSUPPORTED" if placeholder else claim.support_status.value
    reasons = list(claim.issues)
    if placeholder:
        reasons.append("NONFACTUAL_PLACEHOLDER")
    return {
        "claim_id": claim.claim_id,
        "observation_id": claim.observation_id,
        "contract_version": claim.contract_version,
        "candidate_state": "CANDIDATE",
        "interpretation_status": "PROPOSED",
        "organization_id": int(claim.organization_id),
        "operation_id": int(claim.operation_id),
        "source_set_revision_id": int(claim.source_set_revision_id),
        "assurance_document_id": document_id,
        "operation_document_id": operation_document_id,
        "logical_document_id": claim.document_id,
        "document_version": claim.document_version,
        "original_sha256": claim.original_document_sha256,
        "field_name": claim.field_name,
        "original_value": claim.original_value,
        "normalized_value": None if placeholder else claim.normalized_value,
        "entity_type": claim.entity_type,
        "subject_id": claim.subject_id,
        "line_reference": claim.line_reference,
        "sku": claim.sku,
        "association_status": claim.association_status.value,
        "support_status": support,
        "source_locator": (
            {"kind": locator.kind, "page": locator.page,
             "block_id": locator.block_id, "start": locator.start,
             "end": locator.end, "sheet": locator.sheet,
             "row": locator.row, "column": locator.column}
            if locator else None
        ),
        "source_line_sha256": (
            sha256(claim.source_excerpt.encode("utf-8")).hexdigest()
            if claim.source_excerpt else None
        ),
        "extractor_name": claim.extractor_name,
        "extractor_version": claim.extractor_version,
        "reason_codes": reasons,
    }


def collect_source_claims(
    *, organization_id: int, operation_id: int,
    source_set_revision_id: int, source_set_fingerprint: str,
    assurance_document_id: int, operation_document_id: int,
    document_version_number: int, original_sha256: str,
    original_bytes: bytes, bundle: BundleResolution,
) -> dict[str, Any]:
    """Create source-linked claims with exact byte identity and source anchors.

    All candidates, including absent/ambiguous support, remain observations;
    no claim is published as canonical, verified memory or regulatory output.
    """
    if any(int(x) <= 0 for x in (
        organization_id, operation_id, source_set_revision_id,
        assurance_document_id, operation_document_id, document_version_number,
    )):
        raise ValueError("Positive tenant/operation/revision/document identity required")
    if not source_set_fingerprint or not isinstance(original_bytes, bytes) or not original_bytes:
        raise ValueError("Original source evidence or source-set fingerprint absent")
    digest = sha256(original_bytes).hexdigest()
    if digest != original_sha256:
        raise ValueError("Original document SHA-256 mismatch")

    context = ClaimContext(
        organization_id=str(organization_id),
        operation_id=str(operation_id),
        source_set_revision_id=str(source_set_revision_id),
        extraction_run_id=f"engine2:{operation_document_id}:{digest}",
    )
    claims = []
    for logical in bundle.documents:
        source = SourceEvidenceDocument(
            document_id=f"{operation_document_id}:{logical.logical_document_id}",
            original_bytes=original_bytes,
            layout=logical.resolution.layout,
            document_version=str(document_version_number),
        )
        for claim in claims_from_engine2_resolution(
            logical.resolution, context=context, document=source,
        ):
            if claim.support_status.value == "SUPPORTED" and not verify_claim_anchor(claim, source):
                raise ValueError("Source claim verifier rejected purported support")
            claims.append(_project_claim(
                claim, document_id=assurance_document_id,
                operation_document_id=operation_document_id,
            ))
            if len(claims) > MAX_SOURCE_CLAIMS:
                raise ValueError("Assurance V2 source claims exceeded processing budget")

        # Explicit PDF label/value spans are *supplemental source observations*.
        # Keep them out of Engine2's canonical/fusion result and out of filing
        # authority. Each candidate preserves its literal page/block/subspan.
        # The existing V2 documentary identity bridge binds only when the
        # full packet proves one exact supplier/SKU/line.
        for span in extract_explicit_field_spans(logical.resolution.layout):
            raw = RawCandidate(
                field_key=span.field_name,
                raw_text=span.value, normalized_value=span.value,
                source_block=span.block,
                evidence_class=EvidenceClass.EXPLICIT,
                extractor_name="explicit_pdf_label",
                extractor_version=EXTRACTOR_VERSION,
            )
            provenance = Provenance(
                filename=bundle.filename, page=span.block.page,
                bbox=span.block.bbox, block_id=span.block.block_id,
                source_text=span.block.text,
                extractor_name="explicit_pdf_label",
                extractor_version=EXTRACTOR_VERSION,
                evidence_class=EvidenceClass.EXPLICIT,
            )
            candidate = AdmittedCandidate(
                raw=raw, provenance=provenance, score=1.0,
                document_type=logical.resolution.document_type,
            )
            claim = from_engine2_candidate(
                candidate, context=context, document=source,
            )
            if not verify_claim_anchor(claim, source):
                raise ValueError("Explicit PDF label span failed source verification")
            projected = _project_claim(
                claim, document_id=assurance_document_id,
                operation_document_id=operation_document_id,
            )
            # If Engine2 itself already reported the same evidence span,
            # don't duplicate its existing source observation.
            if any(c["field_name"] == projected["field_name"]
                   and c["original_value"] == projected["original_value"]
                   and c["source_locator"] == projected["source_locator"]
                   and c["support_status"] == "SUPPORTED"
                   for c in claims):
                continue
            claims.append(projected)
            if len(claims) > MAX_SOURCE_CLAIMS:
                raise ValueError("Assurance V2 source claims exceeded processing budget")

    return {
        "schema_version": ASSURANCE_CLAIMS_SNAPSHOT_VERSION,
        "source_claim_contract_version": CLAIM_CONTRACT_VERSION,
        "organization_id": organization_id,
        "operation_id": operation_id,
        "source_set_revision_id": source_set_revision_id,
        "source_set_fingerprint": source_set_fingerprint,
        "assurance_document_id": assurance_document_id,
        "operation_document_id": operation_document_id,
        "document_version_number": document_version_number,
        "original_sha256": digest,
        "claim_count": len(claims),
        "claims": claims,
    }
