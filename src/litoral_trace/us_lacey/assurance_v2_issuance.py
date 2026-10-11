"""Atomic, tenant-scoped Assurance V2 filing-package issuance boundary.

Not connected to production routes until a persisted explicit export-authorization
command and the complete two-shipment PostgreSQL Golden Test are accepted.
Never grant authority from candidates or a detached HTTP read model.
"""
from __future__ import annotations

from uuid import UUID

from sqlalchemy import select

from litoral_trace.db.models import UsLaceyOperation, UsLaceySourceSetRevision
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.assurance_v2_case_reader import _case
from litoral_trace.us_lacey.assurance_v2_memory import _actor
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.exporters.assurance_v2_package import (
    PostgresOperationEventPackageStore, issue_assurance_package,
)


class StaleAssuranceIssue(ValueError):
    """A reviewed case was changed before the issuance transaction."""


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
