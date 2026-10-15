"""Resolve current document authority inside a tenant-scoped review transaction.

Preparation worksheets remain reviewable information, not independent support
for a confirmed PPQ505 value. Filename classification cannot itself establish
positive regulatory authority.
"""
from __future__ import annotations

from sqlalchemy import select

from litoral_trace.db.models import (
    AssuranceDocument, UsLaceyOperationDocument, VaultDocument,
)
from litoral_trace.us_lacey.document_evidence_class import classify_document


def reviewed_source_class(
    session, *, organization_id: int, operation_id: int,
    assurance_document_id: int | None,
):
    """Return source classification only for a current operation-owned document."""
    if assurance_document_id is None:
        return None
    row = session.execute(
        select(
            UsLaceyOperationDocument.document_role,
            VaultDocument.original_filename,
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
    return classify_document(str(row[1] or ""), str(row[0] or "")) if row else None


def is_independent_review_source(
    session, *, organization_id: int, operation_id: int,
    assurance_document_id: int | None,
) -> bool:
    """A missing, stale or worksheet source is not independently supportive."""
    source = reviewed_source_class(
        session, organization_id=organization_id, operation_id=operation_id,
        assurance_document_id=assurance_document_id,
    )
    return source is not None and source.code in {"COMMERCIAL_SOURCE", "SUPPLIER_SOURCE"}
