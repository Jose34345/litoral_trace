"""Atomic, tenant-scoped Assurance V2 filing-package issuance boundary.

Not connected to production routes until a persisted explicit export-authorization
command and the complete two-shipment PostgreSQL Golden Test are accepted.
Never grant authority from candidates or a detached HTTP read model.
"""
from __future__ import annotations

from uuid import UUID

from sqlalchemy import select

from litoral_trace.db.models import (
    AssuranceDocument, AssuranceV2MemoryLink, UsLaceyOperation, UsLaceyOperationDocument,
    UsLaceyOperationProductLink, UsLaceyRegulatoryAssessmentSnapshot,
    UsLaceySourceSetMember, UsLaceySourceSetRevision, UsLaceySupplier,
    UsLaceySupplierEvidence, UsLaceySupplierProduct, VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.assurance_v2_case_reader import _case
from litoral_trace.us_lacey.assurance_v2_memory import _actor
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.exporters.assurance_v2_package import (
    PostgresOperationEventPackageStore, issue_assurance_package,
)


class StaleAssuranceIssue(ValueError):
    """A reviewed case was changed before the issuance transaction."""


def _lock_case_dependencies(session, *, organization_id: int, operation_id: int,
                            revision: UsLaceySourceSetRevision) -> None:
    """Hold source, identity, evidence and rule rows until package commit.

    The operation/revision locks serialize source-set transitions and decisions.
    These additional locks prevent changes to current documents, historical
    evidence and regulatory snapshots during byte generation.
    """
    org_id = int(organization_id)
    members = session.scalars(select(UsLaceySourceSetMember).where(
        UsLaceySourceSetMember.organization_id == org_id,
        UsLaceySourceSetMember.source_set_revision_id == revision.id,
    ).order_by(UsLaceySourceSetMember.operation_document_id)).all()
    if len(members) != revision.document_count:
        raise StaleAssuranceIssue("Source membership changed since review")
    for member in members:
        link = session.scalar(select(UsLaceyOperationDocument).where(
            UsLaceyOperationDocument.organization_id == org_id,
            UsLaceyOperationDocument.operation_id == operation_id,
            UsLaceyOperationDocument.id == member.operation_document_id,
            UsLaceyOperationDocument.assurance_document_id == member.assurance_document_id,
            UsLaceyOperationDocument.is_current.is_(True),
        ).with_for_update(read=True))
        if link is None:
            raise StaleAssuranceIssue("Source document changed since review")
        source = session.scalar(select(AssuranceDocument).where(
            AssuranceDocument.organization_id == org_id,
            AssuranceDocument.id == member.assurance_document_id,
        ).with_for_update(read=True))
        vault = session.scalar(select(VaultDocument).where(
            VaultDocument.organization_id == org_id,
            VaultDocument.id == source.vault_document_id,
            VaultDocument.status == "available",
        ).with_for_update(read=True)) if source is not None else None
        if vault is None:
            raise StaleAssuranceIssue("Source document unavailable")

    links = session.scalars(select(UsLaceyOperationProductLink).where(
        UsLaceyOperationProductLink.organization_id == org_id,
        UsLaceyOperationProductLink.operation_id == operation_id,
        UsLaceyOperationProductLink.source_set_revision_id == revision.id,
    )).all()
    products = []
    for product_id in sorted({link.supplier_product_id for link in links}):
        product = session.scalar(select(UsLaceySupplierProduct).where(
            UsLaceySupplierProduct.organization_id == org_id,
            UsLaceySupplierProduct.id == product_id,
        ).with_for_update())
        if product is None:
            raise StaleAssuranceIssue("Product identity changed since review")
        products.append(product)
    for supplier_id in sorted({product.supplier_id for product in products}):
        if session.scalar(select(UsLaceySupplier).where(
            UsLaceySupplier.organization_id == org_id,
            UsLaceySupplier.id == supplier_id,
        ).with_for_update(read=True)) is None:
            raise StaleAssuranceIssue("Supplier identity changed since review")
    for product in products:
        session.scalars(select(UsLaceySupplierEvidence).where(
            UsLaceySupplierEvidence.organization_id == org_id,
            UsLaceySupplierEvidence.supplier_product_id == product.id,
        ).with_for_update(read=True)).all()
    session.scalars(select(UsLaceyRegulatoryAssessmentSnapshot).where(
        UsLaceyRegulatoryAssessmentSnapshot.organization_id == org_id,
        UsLaceyRegulatoryAssessmentSnapshot.operation_id == operation_id,
        UsLaceyRegulatoryAssessmentSnapshot.source_set_revision_id == revision.id,
    ).with_for_update(read=True)).all()


def _lock_applied_reuse_origins(session, *, organization_id: int, case) -> None:
    """Keep historical source revisions and PDF hashes stable during issuance."""
    org_id = int(organization_id)
    for reuse in sorted((r for r in case.reuses if r.get("status") == "VERIFIED"),
                        key=lambda r: str(r.get("id"))):
        try:
            public_id = UUID(str(reuse["id"]))
        except (KeyError, ValueError, TypeError) as exc:
            raise StaleAssuranceIssue("Historical reuse reference invalid") from exc
        memory = session.scalar(select(AssuranceV2MemoryLink).where(
            AssuranceV2MemoryLink.organization_id == org_id,
            AssuranceV2MemoryLink.public_id == public_id,
        ))
        if memory is None:
            raise StaleAssuranceIssue("Historical reuse unavailable")
        origin_revision = session.scalar(select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == org_id,
            UsLaceySourceSetRevision.id == memory.source_set_revision_id,
            UsLaceySourceSetRevision.is_current.is_(True),
        ).with_for_update(read=True))
        member = session.scalar(select(UsLaceySourceSetMember).where(
            UsLaceySourceSetMember.organization_id == org_id,
            UsLaceySourceSetMember.source_set_revision_id == memory.source_set_revision_id,
            UsLaceySourceSetMember.assurance_document_id == memory.assurance_document_id,
        ))
        if origin_revision is None or member is None:
            raise StaleAssuranceIssue("Historical source set changed")
        origin_link = session.scalar(select(UsLaceyOperationDocument).where(
            UsLaceyOperationDocument.organization_id == org_id,
            UsLaceyOperationDocument.id == member.operation_document_id,
            UsLaceyOperationDocument.is_current.is_(True),
        ).with_for_update(read=True))
        origin = session.scalar(select(AssuranceDocument).where(
            AssuranceDocument.organization_id == org_id,
            AssuranceDocument.id == memory.assurance_document_id,
        ).with_for_update(read=True))
        vault = session.scalar(select(VaultDocument).where(
            VaultDocument.organization_id == org_id,
            VaultDocument.id == origin.vault_document_id,
            VaultDocument.status == "available",
        ).with_for_update(read=True)) if origin is not None else None
        if origin_link is None or vault is None or vault.sha256 != reuse.get("source_document_hash"):
            raise StaleAssuranceIssue("Historical source document changed")


def issue_locked_assurance_package(
    *, organization_id: int, operation_public_id: str,
    authenticated_user_id: int, expected_source_set_fingerprint: str,
    expected_case_fingerprint: str,
    session_factory=get_us_lacey_db_session,
    case_loader=_case,
    package_store: PostgresOperationEventPackageStore | None = None,
) -> dict[str, object]:
    """Re-read and persist one authorized package in the SAME DB transaction.

    The caller must obtain the actor and organization from the trusted portal
    session, not from a request body. Locks are held through package generation,
    event insertion and commit. A missing explicit export authorization always
    fails closed, even if all extracted values are supported.
    """
    try:
        public_id = UUID(str(operation_public_id))
    except (TypeError, ValueError, AttributeError) as exc:
        raise PermissionError("Operation unavailable") from exc
    org_id, user_id = int(organization_id), int(authenticated_user_id)
    if org_id <= 0 or user_id <= 0:
        raise PermissionError("Authenticated tenant reviewer required")

    session = session_factory()
    try:
        set_tenant_db_context(session, org_id)
        _actor(session, org_id, user_id)
        operation = session.scalar(select(UsLaceyOperation).where(
            UsLaceyOperation.organization_id == org_id,
            UsLaceyOperation.public_id == public_id,
        ).with_for_update())
        if operation is None:
            raise PermissionError("Operation unavailable")
        revision = session.scalar(select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == org_id,
            UsLaceySourceSetRevision.operation_id == operation.id,
            UsLaceySourceSetRevision.is_current.is_(True),
            UsLaceySourceSetRevision.status == "FINALIZED",
        ).with_for_update())
        if revision is None or revision.source_set_fingerprint != expected_source_set_fingerprint:
            raise StaleAssuranceIssue("Source set changed since review")

        _lock_case_dependencies(
            session, organization_id=org_id, operation_id=operation.id,
            revision=revision,
        )

        case = case_loader(session, org_id, public_id)
        if case is not None:
            _lock_applied_reuse_origins(session, organization_id=org_id, case=case)
            # Historical evidence may have changed while waiting on its lock.
            # Recompute authority only after all source and evidence rows are held.
            case = case_loader(session, org_id, public_id)
        if (case is None
                or case.organization_id != org_id
                or case.operation_id != str(public_id)
                or case.source_set_revision != revision.id
                or case.source_set_fingerprint != revision.source_set_fingerprint
                or case.authority_fingerprint() != expected_case_fingerprint):
            raise StaleAssuranceIssue("Case authority changed since review")
        export_actor = str(case.export_authorization.get("actor_id") or "")
        if export_actor not in {str(user_id), f"user:{user_id}"}:
            raise PermissionError("Package needs explicit authorization from this reviewer")

        package = issue_assurance_package(case)
        store = package_store or PostgresOperationEventPackageStore(session_factory=session_factory)
        store.save_in_transaction(
            session, organization_id=org_id,
            operation_id=str(public_id), package=package,
        )
        session.commit()
        return {
            "status": "FILING_READY",
            "package_fingerprint": package.fingerprint,
            "submitted_to_agency": False,
        }
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
