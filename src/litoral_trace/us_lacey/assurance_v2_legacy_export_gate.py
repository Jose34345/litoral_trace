"""Prevent an Assurance V2 operation from using ungated V1 preparation exports."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    AssuranceV2Decision, UsLaceyOperation, UsLaceyOperationEvent,
    UsLaceySourceSetRevision,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.operations import UsLaceyOperationNotFound


class AssuranceV2LegacyExportBlocked(PermissionError):
    """The operation has adopted V2 authority and requires its export gate."""


@contextmanager
def legacy_export_guard(
    *, organization_id: int, operation_public_id: str,
    session_factory=get_us_lacey_db_session,
) -> Iterator[None]:
    """Hold source-set locks through the legacy byte export.

    A V2 human decision, reuse application, identity event or export
    authorization is a durable adoption signal. Shadow claim extraction alone
    is not. Historical V1 operations remain on their established route.
    """
    try:
        public_id = UUID(str(operation_public_id))
    except (TypeError, ValueError, AttributeError) as exc:
        raise UsLaceyOperationNotFound("Operation not found") from exc
    org_id = int(organization_id)
    session: Session = session_factory()
    try:
        set_tenant_db_context(session, org_id)
        # FOR SHARE blocks an operation rewrite but permits the FK KEY SHARE
        # acquired by an in-flight decision. Lock the current revision too:
        # V2 decisions and authorization lock it before committing authority.
        op = session.scalar(select(UsLaceyOperation).where(
            UsLaceyOperation.organization_id == org_id,
            UsLaceyOperation.public_id == public_id,
        ).with_for_update(read=True))
        if op is None:
            raise UsLaceyOperationNotFound("Operation not found")
        revisions = session.scalars(select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == org_id,
            UsLaceySourceSetRevision.operation_id == op.id,
            UsLaceySourceSetRevision.is_current.is_(True),
        ).with_for_update()).all()
        if len(revisions) > 1:
            raise AssuranceV2LegacyExportBlocked("Ambiguous source revision; export blocked.")

        has_decision = session.scalar(select(AssuranceV2Decision.id).where(
            AssuranceV2Decision.organization_id == org_id,
            AssuranceV2Decision.operation_id == op.id,
        ).limit(1)) is not None
        has_v2_event = session.scalar(select(UsLaceyOperationEvent.id).where(
            UsLaceyOperationEvent.organization_id == org_id,
            UsLaceyOperationEvent.operation_id == op.id,
            UsLaceyOperationEvent.event_key.like("assurance_v2:%"),
        ).limit(1)) is not None
        if has_decision or has_v2_event:
            raise AssuranceV2LegacyExportBlocked(
                "Assurance V2 controls this operation. Use its authorized package export."
            )
        yield
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
