"""Server-side operational entitlement for the isolated U.S. Lacey portal."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select

from litoral_trace.db.models import Organization, UsLaceyOrganizationProfile, UsLaceySubscription
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session


class UsLaceyOperationalAccessError(RuntimeError):
    """Safe denial raised when an authenticated account cannot process documents."""


@dataclass(frozen=True, slots=True)
class UsLaceyOperationalEntitlement:
    organization_id: int
    account_status: str
    subscription_status: str
    monthly_operation_limit: int
    used_operations: int
    is_sandbox: bool = False
    expires_at: datetime | None = None

    @property
    def remaining_operations(self) -> int:
        return max(0, self.monthly_operation_limit - self.used_operations)


def require_us_lacey_operational_access(
    *,
    organization_id: int,
    require_operation_slot: bool = False,
) -> UsLaceyOperationalEntitlement:
    """Verify paid/pilot entitlement and optionally require a new-operation slot.

    Browser state is never authoritative. The account/subscription decision is
    persisted and tenant-scoped. Quota limits creation of *new* operations; once
    an operation exists, its upload/review/export lifecycle remains accessible so
    the customer can finish work already counted against the plan.

    Slot enforcement is deliberately opt-in. Callers that create a new billable
    operation must pass ``require_operation_slot=True``; ordinary workspace,
    upload, review and export access must not be blocked merely because the final
    paid slot has already been consumed.
    """
    org_id = int(organization_id)
    if org_id <= 0:
        raise UsLaceyOperationalAccessError("This workspace cannot process documents.")

    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, org_id)
        organization = session.scalar(
            select(Organization).where(Organization.id == org_id)
        )
        if organization is None or not bool(organization.is_active):
            raise UsLaceyOperationalAccessError(
                "This workspace is not available."
            )

        is_sandbox = bool(getattr(organization, "is_sandbox", False))
        expires_at = getattr(organization, "expires_at", None)
        if is_sandbox:
            if expires_at is None:
                raise UsLaceyOperationalAccessError(
                    "This sandbox is not available."
                )
            normalized_expiry = (
                expires_at.replace(tzinfo=timezone.utc)
                if expires_at.tzinfo is None
                else expires_at.astimezone(timezone.utc)
            )
            if normalized_expiry <= datetime.now(timezone.utc):
                raise UsLaceyOperationalAccessError(
                    "This sandbox has expired."
                )
            expires_at = normalized_expiry

        profile = session.scalar(
            select(UsLaceyOrganizationProfile).where(
                UsLaceyOrganizationProfile.organization_id == org_id
            )
        )
        subscription = session.scalar(
            select(UsLaceySubscription).where(
                UsLaceySubscription.organization_id == org_id
            )
        )
        if profile is None or subscription is None:
            raise UsLaceyOperationalAccessError(
                "This workspace is not ready for document processing."
            )

        account_status = str(profile.account_status)
        subscription_status = str(subscription.status)
        legacy_pilot = account_status == "PILOT"
        paid_active = account_status == "ACTIVE" and subscription_status == "ACTIVE"
        if not (legacy_pilot or paid_active):
            raise UsLaceyOperationalAccessError(
                "Document processing is locked until payment is confirmed."
            )

        limit = int(subscription.monthly_operation_limit)
        used = int(subscription.used_operations)
        if require_operation_slot and used >= limit:
            raise UsLaceyOperationalAccessError(
                "This workspace has reached its current operation limit."
            )

        return UsLaceyOperationalEntitlement(
            organization_id=org_id,
            account_status=account_status,
            subscription_status=subscription_status,
            monthly_operation_limit=limit,
            used_operations=used,
            is_sandbox=is_sandbox,
            expires_at=expires_at,
        )
    except UsLaceyOperationalAccessError:
        raise
    except Exception as exc:
        raise UsLaceyOperationalAccessError(
            "Unable to verify document-processing access right now."
        ) from exc
    finally:
        session.close()
