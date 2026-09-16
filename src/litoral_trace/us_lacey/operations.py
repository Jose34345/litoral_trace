"""U.S. Lacey operation service with safe current-document link semantics.

The established operation service remains in ``_operations_core``. This module
keeps its public API stable while isolating the document-linking policy so
unclassified evidence cannot silently supersede unrelated evidence.
"""
from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, select

from litoral_trace.db.models import AssuranceDocument, UsLaceyOperationDocument
from litoral_trace.us_lacey._operations_core import *  # noqa: F403
from litoral_trace.us_lacey._operations_core import (
    UsLaceyOperationNotFound,
    UsLaceyOperationService as _CoreUsLaceyOperationService,
)

_NON_VERSIONED_DOCUMENT_ROLES = frozenset({"UNKNOWN", "OTHER"})


def _normalize_document_role(value: str | None) -> str:
    return str(value or "UNKNOWN").strip().upper() or "UNKNOWN"


def _document_role_replaces_current(role: str | None) -> bool:
    """Return whether a role represents one replaceable semantic document slot.

    UNKNOWN and OTHER are deliberately append-only current evidence: sharing an
    unclassified bucket is not evidence that two uploads are versions of one
    another. Explicit semantic roles retain the existing versioning behavior.
    """

    return _normalize_document_role(role) not in _NON_VERSIONED_DOCUMENT_ROLES


class UsLaceyOperationService(_CoreUsLaceyOperationService):
    """Operation service with fail-safe multi-document attachment semantics."""

    def attach_document(
        self,
        *,
        organization_id: int,
        operation_public_id: UUID | str,
        assurance_document_id: int,
        document_role: str,
    ) -> int:
        org_id = int(organization_id)
        role = _normalize_document_role(document_role)
        session = self._session(org_id)
        try:
            operation = self._get_model(
                session,
                organization_id=org_id,
                operation_public_id=operation_public_id,
            )
            assurance_document = session.scalar(
                select(AssuranceDocument).where(
                    AssuranceDocument.organization_id == org_id,
                    AssuranceDocument.id == int(assurance_document_id),
                )
            )
            if assurance_document is None:
                raise UsLaceyOperationNotFound("Document not found for this company.")

            # Exact evidence attachment is idempotent regardless of whether an
            # explicit semantic successor has since become current.
            existing = session.scalar(
                select(UsLaceyOperationDocument)
                .where(
                    UsLaceyOperationDocument.organization_id == org_id,
                    UsLaceyOperationDocument.operation_id == operation.id,
                    UsLaceyOperationDocument.assurance_document_id == assurance_document.id,
                )
                .order_by(UsLaceyOperationDocument.id.desc())
            )
            if existing is not None:
                return int(existing.id)

            current = None
            if _document_role_replaces_current(role):
                current = session.scalar(
                    select(UsLaceyOperationDocument)
                    .where(
                        UsLaceyOperationDocument.organization_id == org_id,
                        UsLaceyOperationDocument.operation_id == operation.id,
                        UsLaceyOperationDocument.document_role == role,
                        UsLaceyOperationDocument.is_current.is_(True),
                    )
                    .order_by(UsLaceyOperationDocument.version_number.desc())
                )

            version = 1
            if current is not None:
                version = int(current.version_number) + 1
                current.is_current = False

            link = UsLaceyOperationDocument(
                organization_id=org_id,
                operation_id=operation.id,
                assurance_document_id=assurance_document.id,
                document_role=role,
                version_number=version,
                is_current=True,
            )
            session.add(link)
            session.flush()
            operation.document_count = int(
                session.scalar(
                    select(func.count(UsLaceyOperationDocument.id)).where(
                        UsLaceyOperationDocument.organization_id == org_id,
                        UsLaceyOperationDocument.operation_id == operation.id,
                        UsLaceyOperationDocument.is_current.is_(True),
                    )
                )
                or 0
            )
            session.commit()
            return int(link.id)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
