"""Explicit, immutable Assurance V2 export authorization bound to one case digest.

Never infer a filing/export authorization from a candidate, review decision,
regulatory assessment or UI state. This module uses the existing append-only,
tenant-scoped operation event table and shares the caller's DB transaction.
"""
from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    UsLaceyOperation, UsLaceyOperationEvent, UsLaceySourceSetRevision,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.assurance_v2_memory import _actor
from litoral_trace.us_lacey.audit_trail import (
    OperationActorType, OperationEventType, append_operation_event,
)
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.exporters.assurance_v2 import (
    AssuranceCase, evaluate_filing_gate,
)

_AUTHORIZATION_KIND = "ASSURANCE_V2_EXPLICIT_EXPORT_AUTHORIZATION"


def _event_key(digest: str) -> str:
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValueError("Invalid Assurance V2 case fingerprint")
    return f"assurance_v2:export_authorization:{digest}"


def authorization_from_event(
    session: Session, *, organization_id: int, operation_id: int,
    case: AssuranceCase,
) -> dict:
    """Find only a prior explicit human authorization for this exact case."""
    digest = case.authority_fingerprint()
    row = session.scalar(select(UsLaceyOperationEvent).where(
        UsLaceyOperationEvent.organization_id == int(organization_id),
        UsLaceyOperationEvent.operation_id == int(operation_id),
        UsLaceyOperationEvent.event_key == _event_key(digest),
        UsLaceyOperationEvent.event_type == OperationEventType.HUMAN_REVIEW.value,
        UsLaceyOperationEvent.actor_type == OperationActorType.USER.value,
    ))
    if row is None or not isinstance(row.details, dict):
        return {}
    details = row.details
    if (
        details.get("kind") != _AUTHORIZATION_KIND
        or details.get("case_fingerprint") != digest
        or details.get("source_set_fingerprint") != case.source_set_fingerprint
        or details.get("source_set_revision_id") != case.source_set_revision
        or details.get("explicit_confirmation") is not True
        or not str(details.get("reason") or "").strip()
        or not row.actor_identity.startswith("user:")
        or not row.actor_identity[5:].isdigit()
        or str(details.get("actor_user_id")) != row.actor_identity[5:]
    ):
        return {}
    return {
        "id": str(row.public_id),
        "actor_id": row.actor_identity,
        "authorized_at": row.timestamp.isoformat(),
        "status": "AUTHORIZED",
        "case_fingerprint": digest,
        "source_set_fingerprint": case.source_set_fingerprint,
    }


def explicitly_authorize_assurance_export(
    *, organization_id: int, operation_public_id: str,
    authenticated_user_id: int, expected_source_set_fingerprint: str,
    expected_case_fingerprint: str, reason: str,
    explicit_confirmation: bool,
    session_factory=get_us_lacey_db_session,
    case_loader=None,
) -> dict:
    """Record a human export authorization only for a filing-ready case.

    It does not generate artifacts, skip another gate, or submit to CBP/APHIS.
    A subsequent issuance must re-read under locks and confirm the exact digest.
    """
    from litoral_trace.us_lacey.assurance_v2_case_reader import _case

    if explicit_confirmation is not True or not str(reason or "").strip():
        raise ValueError("Explicit reviewer confirmation and reason required")
    if len(str(reason)) > 1000:
        raise ValueError("Reviewer reason exceeds 1000 characters")
    if not expected_source_set_fingerprint or not expected_case_fingerprint:
        raise ValueError("Expected revision and case fingerprints required")
    try:
        public_id = UUID(str(operation_public_id))
    except (TypeError, ValueError, AttributeError) as exc:
        raise PermissionError("Operation unavailable") from exc
    org_id, user_id = int(organization_id), int(authenticated_user_id)
    if org_id <= 0 or user_id <= 0:
        raise PermissionError("Authenticated reviewer required")

    session = session_factory()
    try:
        set_tenant_db_context(session, org_id)
        _actor(session, org_id, user_id)
        op = session.scalar(select(UsLaceyOperation).where(
            UsLaceyOperation.organization_id == org_id,
            UsLaceyOperation.public_id == public_id,
        ).with_for_update())
        if op is None:
            raise PermissionError("Operation unavailable")
        revision = session.scalar(select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == org_id,
            UsLaceySourceSetRevision.operation_id == op.id,
            UsLaceySourceSetRevision.is_current.is_(True),
            UsLaceySourceSetRevision.status == "FINALIZED",
        ).with_for_update())
        if revision is None or revision.source_set_fingerprint != expected_source_set_fingerprint:
            raise ValueError("STALE_SOURCE_SET")
        loader = case_loader or _case
        case = loader(session, org_id, public_id)
        if (
            case is None
            or case.organization_id != org_id
            or case.operation_id != str(public_id)
            or case.source_set_revision != revision.id
            or case.source_set_fingerprint != revision.source_set_fingerprint
            or case.authority_fingerprint() != expected_case_fingerprint
        ):
            raise ValueError("STALE_CASE_AUTHORITY")
        # We are recording permission to export, not granting regulatory
        # readiness. EVERY other gate must already pass independently.
        gate = evaluate_filing_gate(case)
        other = {item.code for item in gate.blockers} - {"EXPORT_NOT_AUTHORIZED"}
        if other:
            raise ValueError("Filing prerequisites not satisfied: " + ", ".join(sorted(other)))

        digest = case.authority_fingerprint()
        row = append_operation_event(
            session,
            organization_id=org_id,
            operation_id=op.id,
            actor_type=OperationActorType.USER,
            actor_identity=f"user:{user_id}",
            event_type=OperationEventType.HUMAN_REVIEW,
            event_key=_event_key(digest),
            details={
                "kind": _AUTHORIZATION_KIND,
                "case_fingerprint": digest,
                "source_set_fingerprint": revision.source_set_fingerprint,
                "source_set_revision_id": revision.id,
                "actor_user_id": user_id,
                "explicit_confirmation": True,
                "reason": reason.strip(),
            },
        )
        auth = authorization_from_event(
            session, organization_id=org_id, operation_id=op.id, case=case,
        )
        if auth.get("actor_id") != f"user:{user_id}" or str(row.public_id) != auth.get("id"):
            raise PermissionError("Export authorization identity mismatch")
        session.commit()
        return auth
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
