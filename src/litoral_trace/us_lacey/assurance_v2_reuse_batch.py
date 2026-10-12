"""One reviewed consent for a group of verified historical plant facts.

Only available, hash-verified original source documents may be authorized.
Each underlying field retains independent immutable provenance/event authority.
"""
from __future__ import annotations

from hashlib import sha256
from uuid import UUID

from sqlalchemy import select

from litoral_trace.db.models import UsLaceyOperation, UsLaceySourceSetRevision
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.assurance_v2_case_reader import _case
from litoral_trace.us_lacey.assurance_v2_historical_source import eligible_historical_source
from litoral_trace.us_lacey.assurance_v2_memory import (
    _actor, _current_claim_snapshot, authorize_reuse_application,
    canonical_claim_snapshot_digest,
)
from litoral_trace.us_lacey.assurance_v2_reuse_guard import current_claim_values_for_reuse
from litoral_trace.us_lacey.audit_trail import append_operation_event
from litoral_trace.us_lacey.assurance_v2_review_adapter import review_journal_enabled
from litoral_trace.us_lacey.db import get_us_lacey_db_session

_ALLOWED = frozenset({"genus", "species", "country_of_harvest"})


def authorize_assurance_reuse_group(
    *, organization_id: int, operation_public_id: str,
    authenticated_user_id: int, reason: str, explicit_confirmation: bool,
    expected_source_set_fingerprint: str, expected_case_fingerprint: str,
    session_factory=get_us_lacey_db_session,
) -> dict:
    """Lock current generation; all applications commit or none do."""
    org_id, actor_id = int(organization_id), int(authenticated_user_id)
    try:
        public_id = UUID(str(operation_public_id))
    except (ValueError, TypeError) as exc:
        raise ValueError("Invalid shipment") from exc
    justification = str(reason or "").strip()
    if min(org_id, actor_id) < 1:
        raise PermissionError("Reviewer access required")
    if (explicit_confirmation is not True or not justification
            or len(justification) > 180):
        raise ValueError("A signed review of the historical sources is required")
    db = session_factory()
    try:
        set_tenant_db_context(db, org_id)
        _actor(db, org_id, actor_id)
        operation = db.scalar(select(UsLaceyOperation).where(
            UsLaceyOperation.organization_id == org_id,
            UsLaceyOperation.public_id == public_id,
        ).with_for_update())
        if operation is None:
            raise PermissionError("Shipment unavailable")
        if not review_journal_enabled(org_id, operation.client_reference):
            raise PermissionError("V2 lab review journal disabled for this shipment")
        if not str(operation.client_reference or "").upper().startswith("V2LAB-"):
            raise PermissionError("Historical batch review is private-lab only")
        revision = db.scalar(select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == org_id,
            UsLaceySourceSetRevision.operation_id == operation.id,
            UsLaceySourceSetRevision.is_current.is_(True),
            UsLaceySourceSetRevision.status == "FINALIZED",
        ).with_for_update())
        if revision is None or revision.source_set_fingerprint != expected_source_set_fingerprint:
            raise ValueError("STALE_SOURCE_SET")
        case = _case(db, org_id, public_id)
        if (case is None or case.source_set_revision != revision.id
                or case.authority_fingerprint() != expected_case_fingerprint):
            raise ValueError("STALE_CASE_AUTHORITY")
        if not case.identities or any(
            i.get("status") not in {"VERIFIED", "HUMAN_CONFIRMED"}
            for i in case.identities
        ):
            raise ValueError("Confirm exact supplier and SKU before memory reuse")
        eligible = [r for r in case.reuses if r.get("status") == "ELIGIBLE"]
        if (not eligible or len({(r.get("line_reference"),r.get("field_name")) for r in eligible}) != len(eligible)
                or any(r.get("field_name") not in _ALLOWED for r in eligible)):
            raise ValueError("Historical source group is unavailable or ambiguous")
        for item in eligible:
            if eligible_historical_source(
                db, organization_id=org_id, memory_id=item["id"],
                assurance_document_id=int(item["origin_document_id"]),
            ) is None:
                raise ValueError("ORIGINAL_HISTORICAL_DOCUMENT_UNAVAILABLE")
        snapshot = _current_claim_snapshot(
            db, organization_id=org_id, operation_id=operation.id, revision=revision,
        )
        if snapshot is None:
            raise ValueError("SOURCE_CLAIMS_INVALID")
        digest = canonical_claim_snapshot_digest(snapshot)
        observations = tuple(
            claim for item in snapshot["documents"] for claim in item["claims"]
        )
        group_id = sha256(
            f"{org_id}:{operation.id}:{revision.id}:{actor_id}:{expected_case_fingerprint}".encode()
        ).hexdigest()
        results = []
        for item in sorted(eligible, key=lambda r: (r["line_reference"], r["field_name"])):
            values = current_claim_values_for_reuse(
                observations, field_name=item["field_name"],
                line_reference=item["line_reference"],
            )
            if values is None:
                raise ValueError("SOURCE_CLAIMS_INVALID")
            receipt = authorize_reuse_application(
                db, organization_id=org_id, operation_id=operation.id,
                source_set_revision_id=revision.id,
                authenticated_user_id=actor_id,
                line_reference=item["line_reference"],
                field_name=item["field_name"],
                memory_public_id=UUID(str(item["id"])),
                current_claim_values=values,
                current_claim_snapshot_digest=digest,
                reason=justification,
                idempotency_key=f"review-group:{group_id}:{item['field_name']}",
            )
            results.append(receipt)
        append_operation_event(
            db, organization_id=org_id, operation_id=operation.id,
            actor_type="USER", actor_identity=f"user:{actor_id}",
            event_type="HUMAN_REVIEW", event_key=f"v2lab:memory_group:{group_id}",
            details={"action": "MEMORY_GROUP_REVIEW_CONFIRMED",
                     "group_id": group_id, "source_set_revision_id": revision.id,
                     "field_count": len(results), "human_actions": 1},
        )
        db.commit()
        return {"status": "VERIFIED_REUSE_BATCH", "field_count": len(results),
                "group_id": group_id}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
