"""One explicit human authorization for a bounded botanical evidence group.

Each historical claim is still checked independently and receives its own
append-only application event. One failing origin/claim rolls back the entire
group. Current source claims and filing authority are never overwritten.
"""
from __future__ import annotations

from hashlib import sha256
from uuid import UUID

from sqlalchemy import select

from litoral_trace.db.models import UsLaceyOperation, UsLaceySourceSetRevision
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.assurance_v2_case_reader import _case
from litoral_trace.us_lacey.assurance_v2_historical_source import (
    verified_historical_original,
)
from litoral_trace.us_lacey.assurance_v2_memory import (
    _actor, _current_claim_snapshot, authorize_reuse_application,
    canonical_claim_snapshot_digest,
)
from litoral_trace.us_lacey.assurance_v2_reuse_guard import current_claim_values_for_reuse
from litoral_trace.us_lacey.audit_trail import append_operation_event
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.assurance_v2_review_adapter import review_journal_enabled

_BOTANICAL = frozenset({"genus", "species", "country_of_harvest"})


def approve_botanical_evidence_group(
    *, organization_id: int, operation_public_id: str,
    authenticated_user_id: int, selected: tuple[dict, ...],
    reason: str, explicit_confirmation: bool,
    expected_source_set_fingerprint: str, expected_case_fingerprint: str,
    session_factory=get_us_lacey_db_session,
) -> dict:
    org_id, user_id = int(organization_id), int(authenticated_user_id)
    try:
        operation_uuid = UUID(str(operation_public_id))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("Invalid shipment identifier") from exc
    note = str(reason or "").strip()
    if org_id <= 0 or user_id <= 0:
        raise PermissionError("Authenticated reviewer required")
    if not explicit_confirmation or not 6 <= len(note) <= 180:
        raise ValueError("Explicit botanical review and a reason are required")
    if not isinstance(selected, (tuple, list)) or not 1 <= len(selected) <= 3:
        raise ValueError("Select between 1 and 3 botanical values")
    if not all(isinstance(v, dict)
               and set(v) == {"memory_id", "field_name", "line_reference"}
               for v in selected):
        raise ValueError("Only eligible botanical memory claims may be selected")
    entries = tuple(
        (str(v["memory_id"]), str(v["field_name"]), str(v["line_reference"]))
        for v in selected
    )
    if (len(set(entries)) != len(entries)
            or len({field for _, field, _ in entries}) != len(entries)
            or any(field not in _BOTANICAL for _, field, _ in entries)
            or len({line for _, _, line in entries}) != 1):
        raise ValueError("A group must contain unique botanical facts for one line")
    for memory_id, _, line in entries:
        try:
            UUID(memory_id)
        except ValueError as exc:
            raise ValueError("Invalid historical evidence reference") from exc
        if not line:
            raise ValueError("Line identity is required")

    session = session_factory()
    try:
        set_tenant_db_context(session, org_id)
        _actor(session, org_id, user_id)
        operation = session.scalar(select(UsLaceyOperation).where(
            UsLaceyOperation.organization_id == org_id,
            UsLaceyOperation.public_id == operation_uuid,
        ).with_for_update())
        if operation is None:
            raise PermissionError("Shipment unavailable")
        if not review_journal_enabled(org_id, operation.client_reference):
            raise PermissionError("Grouped reuse is restricted to the private V2 lab")
        revision = session.scalar(select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == org_id,
            UsLaceySourceSetRevision.operation_id == operation.id,
            UsLaceySourceSetRevision.is_current.is_(True),
            UsLaceySourceSetRevision.status == "FINALIZED",
        ).with_for_update())
        if revision is None or revision.source_set_fingerprint != expected_source_set_fingerprint:
            raise ValueError("STALE_SOURCE_SET")
        case = _case(session, org_id, operation_uuid)
        if (case is None or case.source_set_revision != revision.id
                or case.authority_fingerprint() != expected_case_fingerprint):
            raise ValueError("STALE_CASE_AUTHORITY")
        eligible = {
            (str(row["id"]), str(row["field_name"]), str(row["line_reference"])): row
            for row in case.reuses if row.get("status") == "ELIGIBLE"
            and row.get("field_name") in _BOTANICAL
        }
        for entry in entries:
            item = eligible.get(entry)
            if item is None:
                raise ValueError("HISTORICAL_REUSE_CHANGED")
            origin = verified_historical_original(
                session, organization_id=org_id,
                memory_public_id=entry[0],
                origin_document_id=int(item["origin_document_id"]),
            )
            if origin is None or origin["field_name"] != entry[1]:
                raise ValueError("HISTORICAL_SOURCE_UNAVAILABLE")
            if origin["value"].strip().casefold() != str(item["value"]).strip().casefold():
                raise ValueError("HISTORICAL_EVIDENCE_CHANGED")
        claim_snapshot = _current_claim_snapshot(
            session, organization_id=org_id, operation_id=operation.id,
            revision=revision,
        )
        if claim_snapshot is None:
            raise ValueError("SOURCE_CLAIMS_INVALID")
        claim_digest = canonical_claim_snapshot_digest(claim_snapshot)
        raw_claims = tuple(
            claim for source in claim_snapshot["documents"]
            for claim in source["claims"]
        )
        request_digest = sha256((
            f"{org_id}:{operation.id}:{revision.id}:{user_id}:"
            + "|".join(":".join(entry) for entry in sorted(entries))
        ).encode("utf-8")).hexdigest()
        applied = []
        for memory_id, field, line in sorted(entries):
            values = current_claim_values_for_reuse(
                raw_claims, field_name=field, line_reference=line,
            )
            if values is None:
                raise ValueError("CURRENT_CLAIM_VALUES_INVALID")
            row = authorize_reuse_application(
                session, organization_id=org_id, operation_id=operation.id,
                source_set_revision_id=revision.id,
                authenticated_user_id=user_id, line_reference=line,
                field_name=field, memory_public_id=UUID(memory_id),
                current_claim_values=values,
                current_claim_snapshot_digest=claim_digest,
                reason=note,
                idempotency_key=f"v2lab-group:{request_digest}:{field}",
            )
            if row.status != "VERIFIED_REUSE":
                raise ValueError("HISTORICAL_REUSE_NOT_VERIFIED")
            applied.append({"field_name": row.field_name,
                            "line_reference": row.line_reference,
                            "memory_id": str(row.memory_public_id),
                            "origin_document_id": str(row.origin_document_id)})
        event = append_operation_event(
            session, organization_id=org_id, operation_id=operation.id,
            actor_type="USER", actor_identity=f"user:{user_id}",
            event_type="HUMAN_REVIEW",
            event_key=f"assurance_v2:botanical_group:{request_digest}",
            details={
                "action": "BOTANICAL_GROUP_REVIEW",
                "source_set_revision_id": revision.id,
                "source_set_fingerprint": revision.source_set_fingerprint,
                "line_reference": entries[0][2],
                "field_count": len(entries),
                "fields": [r["field_name"] for r in applied],
                "origin_document_ids": sorted({r["origin_document_id"] for r in applied}),
                "reason": note,
            },
        )
        session.commit()
        return {"status": "RECORDED", "authorized_count": len(applied),
                "audit_event_id": event.id, "verified": applied}
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
