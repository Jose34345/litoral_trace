"""Current-tenant, source-hash-verified historical evidence inspector.

An eligible reusable claim is not permission to retrieve arbitrary archival
files. The caller must first resolve an eligible claim on its CURRENT shipment
and then prove the origin has not been deleted or altered.
"""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import and_, select

from litoral_trace.db.models import (
    AssuranceDocument, AssuranceV2MemoryLink, UsLaceyEvidenceClaim,
    UsLaceySupplierEvidence, VaultDocument,
)


def verified_historical_original(
    session, *, organization_id: int, memory_public_id: str,
    origin_document_id: int, now: datetime | None = None,
) -> dict | None:
    try:
        memory_id = UUID(str(memory_public_id))
        document_id = int(origin_document_id)
    except (TypeError, ValueError):
        return None
    org_id = int(organization_id)
    if org_id <= 0 or document_id <= 0:
        return None
    at = now or datetime.now(timezone.utc)
    row = session.execute(
        select(
            AssuranceV2MemoryLink, UsLaceyEvidenceClaim,
            UsLaceySupplierEvidence, VaultDocument,
        )
        .join(UsLaceyEvidenceClaim, and_(
            UsLaceyEvidenceClaim.id == AssuranceV2MemoryLink.evidence_claim_id,
            UsLaceyEvidenceClaim.organization_id == AssuranceV2MemoryLink.organization_id,
        ))
        .join(UsLaceySupplierEvidence, and_(
            UsLaceySupplierEvidence.id == UsLaceyEvidenceClaim.evidence_id,
            UsLaceySupplierEvidence.organization_id == UsLaceyEvidenceClaim.organization_id,
        ))
        .join(AssuranceDocument, and_(
            AssuranceDocument.id == AssuranceV2MemoryLink.assurance_document_id,
            AssuranceDocument.organization_id == AssuranceV2MemoryLink.organization_id,
        ))
        .join(VaultDocument, and_(
            VaultDocument.id == AssuranceDocument.vault_document_id,
            VaultDocument.organization_id == AssuranceDocument.organization_id,
        ))
        .where(
            AssuranceV2MemoryLink.organization_id == org_id,
            AssuranceV2MemoryLink.public_id == memory_id,
            AssuranceV2MemoryLink.assurance_document_id == document_id,
            UsLaceySupplierEvidence.status == "VERIFIED",
            UsLaceySupplierEvidence.valid_from <= at,
            UsLaceySupplierEvidence.valid_until > at,
            UsLaceySupplierEvidence.document_hash == VaultDocument.sha256,
            VaultDocument.status == "available",
        )
    ).one_or_none()
    if row is None:
        return None
    link, claim, evidence, vault = row
    if not str(claim.normalized_value or claim.field_value or "").strip():
        return None
    return {
        "vault_public_id": str(vault.public_id),
        "sha256": str(vault.sha256),
        "document_id": str(document_id),
        "field_name": str(claim.field_name),
        "value": str(claim.normalized_value or claim.field_value),
        "valid_until": evidence.valid_until.isoformat(),
    }
