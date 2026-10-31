"""Tenant-authenticated confirmation of one current supplier/product/line binding."""
from __future__ import annotations

from hashlib import sha256
from uuid import UUID

from sqlalchemy import select

from litoral_trace.db.models import (
    UsLaceyOperation, UsLaceySourceSetRevision, UsLaceySupplierProduct,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.assurance_v2_case_reader import _case
from litoral_trace.us_lacey.assurance_v2_memory import _actor, _current_claim_snapshot
from litoral_trace.us_lacey.audit_trail import append_operation_event
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.identity_memory import confirm_operation_product_link


def confirm_assurance_v2_identity(
    *, organization_id: int, operation_public_id: str, authenticated_user_id: int,
    line_reference: str, supplier_id: int, product_id: int, reason: str,
    expected_source_set_fingerprint: str, expected_case_fingerprint: str,
    session_factory=get_us_lacey_db_session,
) -> dict:
    """Fence the source set and case, then commit identity and audit together."""
    try:
        public_id = UUID(str(operation_public_id))
        org_id, user_id = int(organization_id), int(authenticated_user_id)
        supplier_id, product_id = int(supplier_id), int(product_id)
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("Invalid operation or identity reference") from exc
    line = str(line_reference or "").strip()
    justification = str(reason or "").strip()
    if min(org_id, user_id, supplier_id, product_id) <= 0:
        raise PermissionError("Authenticated tenant reviewer required")
    if not line or not justification or len(justification) > 180:
        raise ValueError("Exact line and reviewer reason are required")
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
            raise ValueError("STALE_SOURCE_SET")
        case = _case(session, org_id, public_id)
        if case is None or case.source_set_revision != revision.id or case.authority_fingerprint() != expected_case_fingerprint:
            raise ValueError("STALE_CASE_AUTHORITY")
        matches = [item for item in case.identities if (
            item.get("status") == "REVIEW_REQUIRED"
            and item.get("line_reference") == line
            and item.get("supplier_id") == supplier_id
            and item.get("product_id") == product_id
        )]
        if len(matches) != 1:
            raise ValueError("IDENTITY_CANDIDATE_CHANGED")
        if _current_claim_snapshot(
            session, organization_id=org_id, operation_id=operation.id,
            revision=revision,
        ) is None:
            raise ValueError("SOURCE_CLAIMS_INVALID")
        product = session.scalar(select(UsLaceySupplierProduct).where(
            UsLaceySupplierProduct.organization_id == org_id,
            UsLaceySupplierProduct.id == product_id,
            UsLaceySupplierProduct.supplier_id == supplier_id,
        ).with_for_update())
        if product is None or str(product.sku) != str(matches[0].get("sku")):
            raise ValueError("IDENTITY_CANDIDATE_CHANGED")
        binding = confirm_operation_product_link(
            session, organization_id=org_id, operation_id=operation.id,
            source_set_revision_id=revision.id, line_reference=line,
            supplier_product_id=product_id, user_id=user_id,
        )
        event_key = "assurance_v2:identity_confirm:" + sha256(
            f"{org_id}:{operation.id}:{revision.id}:{line}:{supplier_id}:{product_id}:{user_id}".encode()
        ).hexdigest()
        details = {
            "action": "IDENTITY_CONFIRMED", "line_reference": line,
            "supplier_id": supplier_id, "product_id": product_id,
            "sku": str(product.sku), "source_set_fingerprint": revision.source_set_fingerprint,
            "reason": justification,
        }
        event = append_operation_event(
            session, organization_id=org_id, operation_id=operation.id,
            actor_type="USER", actor_identity=f"user:{user_id}",
            event_type="HUMAN_REVIEW", details=details, event_key=event_key,
        )
        if event.actor_identity != f"user:{user_id}" or event.details != details:
            raise ValueError("IDENTITY_AUDIT_COLLISION")
        session.commit()
        return {
            "status": "HUMAN_CONFIRMED", "line_reference": line,
            "supplier_id": supplier_id, "product_id": product_id,
            "sku": str(product.sku), "link_id": binding.id,
            "audit_event_id": event.id,
        }
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
