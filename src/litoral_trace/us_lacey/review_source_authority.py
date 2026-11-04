"""Resolve current document authority inside a tenant-scoped review transaction.

Preparation worksheets remain reviewable information, not independent support
for a confirmed PPQ505 value. Filename classification cannot itself establish
positive regulatory authority.
"""
from __future__ import annotations

import re

from sqlalchemy import select

from litoral_trace.db.models import (
    AssuranceDocument, UsLaceyOperationDocument, VaultDocument,
)
from litoral_trace.us_lacey.document_evidence_class import classify_document
from litoral_trace.us_lacey.document_field_authority import source_role_supports_field


def _reviewed_source_details(
    session, *, organization_id: int, operation_id: int,
    assurance_document_id: int | None,
):
    """Load role and classification only for a current tenant-owned source."""
    if assurance_document_id is None:
        return None
    row = session.execute(
        select(
            UsLaceyOperationDocument.document_role,
            VaultDocument.original_filename,
            VaultDocument.status,
            VaultDocument.deleted_at,
            VaultDocument.sha256,
        )
        .join(
            AssuranceDocument,
            (AssuranceDocument.id == UsLaceyOperationDocument.assurance_document_id)
            & (AssuranceDocument.organization_id == UsLaceyOperationDocument.organization_id),
        )
        .join(
            VaultDocument,
            (VaultDocument.id == AssuranceDocument.vault_document_id)
            & (VaultDocument.organization_id == AssuranceDocument.organization_id),
        )
        .where(
            UsLaceyOperationDocument.organization_id == int(organization_id),
            UsLaceyOperationDocument.operation_id == int(operation_id),
            UsLaceyOperationDocument.assurance_document_id == int(assurance_document_id),
            UsLaceyOperationDocument.is_current.is_(True),
        )
    ).first()
    if not row or str(row[2]) != "available" or row[3] is not None:
        return None
    role = str(row[0] or "")
    filename = str(row[1] or "")
    fingerprint = str(row[4] or "").lower() if len(row) > 4 else ""
    return classify_document(filename, role), role, filename, fingerprint


def reviewed_source_class(
    session, *, organization_id: int, operation_id: int,
    assurance_document_id: int | None,
):
    """Return the current operation-owned source classification (if any)."""
    details = _reviewed_source_details(
        session, organization_id=organization_id, operation_id=operation_id,
        assurance_document_id=assurance_document_id,
    )
    return details[0] if details is not None else None


def independent_review_source_role(
    session, *, organization_id: int, operation_id: int,
    assurance_document_id: int | None,
    field_name: str | None = None,
) -> str | None:
    """Return the recorded role of a current, eligible source, not a filename guess."""
    details = _reviewed_source_details(
        session, organization_id=organization_id, operation_id=operation_id,
        assurance_document_id=assurance_document_id,
    )
    if details is None:
        return None
    source, role, filename, _fingerprint = details
    if (
        source.code not in {"COMMERCIAL_SOURCE", "SUPPLIER_SOURCE"}
        or (field_name is not None and not source_role_supports_field(field_name, role, filename))
    ):
        return None
    return role.strip().upper()


def independent_review_source_identity(
    session, *, organization_id: int, operation_id: int,
    assurance_document_id: int | None,
    field_name: str,
) -> tuple[str, str] | None:
    """Role plus Vault SHA-256: duplicate uploads are not corroboration."""
    details = _reviewed_source_details(
        session, organization_id=organization_id, operation_id=operation_id,
        assurance_document_id=assurance_document_id,
    )
    if details is None:
        return None
    source, role, filename, fingerprint = details
    if (
        source.code not in {"COMMERCIAL_SOURCE", "SUPPLIER_SOURCE"}
        or not source_role_supports_field(field_name, role, filename)
        or re.fullmatch(r"[a-f0-9]{64}", fingerprint) is None
    ):
        return None
    return role.strip().upper(), fingerprint


def is_independent_review_source(
    session, *, organization_id: int, operation_id: int,
    assurance_document_id: int | None,
    field_name: str | None = None,
) -> bool:
    """Source must be current, original, and appropriate for the named field."""
    return independent_review_source_role(
        session, organization_id=organization_id, operation_id=operation_id,
        assurance_document_id=assurance_document_id, field_name=field_name,
    ) is not None


def is_anchored_human_shipping_source(
    session, *, organization_id: int, operation_id: int,
    assurance_document_id: int | None,
    field_name: str, source_page: int | None, source_locator: str | None,
) -> bool:
    """Permit explicit human B/L review from an original with a real PDF anchor.

    The exact candidate/source remains tenant- and current-revision-fenced.
    A carrier role is mandatory for *machine* corroboration, not for a human
    inspecting an original commercial document with an unclassified upload role.
    Supplier declarations and derived worksheets never qualify.
    """
    details = _reviewed_source_details(
        session, organization_id=organization_id, operation_id=operation_id,
        assurance_document_id=assurance_document_id,
    )
    if details is None or not source_page or not str(source_locator or "").strip():
        return False
    source, role, filename, _fingerprint = details
    if source.code in {
        "DERIVED_PREPARATION", "ROLE_CONFLICT",
        "SUPPLIER_SOURCE", "UNVERIFIED_SUPPLIER",
    }:
        return False
    if source.code not in {
        "COMMERCIAL_SOURCE", "UNVERIFIED_COMMERCIAL", "UNCLASSIFIED",
    }:
        return False
    # Original/current document + exact span + explicit authenticated reviewer
    # are required here. No automatic, bulk or unanchored promotion is implied.
    return True
