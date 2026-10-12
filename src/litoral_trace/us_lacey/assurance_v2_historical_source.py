"""Owner-lab, tenant-scoped historical source availability and integrity guard."""
from __future__ import annotations

from uuid import UUID

from sqlalchemy import and_, select

from litoral_trace.db.models import (
    AssuranceDocument, AssuranceV2MemoryLink, UsLaceyEvidenceClaim,
    UsLaceySupplierEvidence, VaultDocument,
)


def eligible_historical_source(
    session, *, organization_id: int, memory_id: str, assurance_document_id: int,
) -> dict | None:
    """Read-only proof the original verified evidence is still downloadable.

    Historical memory metadata may persist longer than raw Vault bytes.
    Reuse authorization must never imply a reviewer can inspect an original
    which has already expired/purged.
    """
    try:
        memory = UUID(str(memory_id))
        document_id = int(assurance_document_id)
    except (TypeError, ValueError, AttributeError):
        return None
    if document_id < 1:
        return None
    row = session.execute(
        select(AssuranceV2MemoryLink, UsLaceyEvidenceClaim,
               UsLaceySupplierEvidence, AssuranceDocument, VaultDocument)
        .join(
            UsLaceyEvidenceClaim,
            and_(UsLaceyEvidenceClaim.id == AssuranceV2MemoryLink.evidence_claim_id,
                 UsLaceyEvidenceClaim.organization_id == AssuranceV2MemoryLink.organization_id),
        )
        .join(
            UsLaceySupplierEvidence,
            and_(UsLaceySupplierEvidence.id == UsLaceyEvidenceClaim.evidence_id,
                 UsLaceySupplierEvidence.organization_id == UsLaceyEvidenceClaim.organization_id),
        )
        .join(
            AssuranceDocument,
            and_(AssuranceDocument.id == AssuranceV2MemoryLink.assurance_document_id,
                 AssuranceDocument.organization_id == AssuranceV2MemoryLink.organization_id),
        )
        .join(
            VaultDocument,
            and_(VaultDocument.id == AssuranceDocument.vault_document_id,
                 VaultDocument.organization_id == AssuranceDocument.organization_id),
        )
        .where(
            AssuranceV2MemoryLink.organization_id == int(organization_id),
            AssuranceV2MemoryLink.public_id == memory,
            AssuranceV2MemoryLink.assurance_document_id == document_id,
        )
    ).one_or_none()
    if row is None:
        return None
    link, claim, evidence, original, vault = row
    if (evidence.status != "VERIFIED" or vault.status != "available"
            or not evidence.document_hash or evidence.document_hash != vault.sha256
            or not claim.field_name or not (claim.normalized_value or claim.field_value)
            or link.evidence_claim_id != claim.id):
        return None
    return {
        "vault_public_id": str(vault.public_id),
        "sha256": vault.sha256,
        "assurance_document_id": document_id,
    }
