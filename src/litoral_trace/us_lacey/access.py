"""Server-side operational entitlement for the isolated U.S. Lacey portal."""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select

from litoral_trace.db.models import UsLaceyOrganizationProfile, UsLaceySubscription
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

    @property
    def remaining_operations(self) -> int:
        return max(0, self.monthly_operation_limit - self.used_operations)


def require_us_lacey_operational_access(
    *, organization_id: int
) -> UsLaceyOperationalEntitlement:
    """Allow customer operations only after verified payment or explicit pilot access.

    Browser state is never authoritative.  The decision is made from the tenant's
    persisted U.S. account/subscription records on every operational entry point.
    """
    org_id = int(organization_id)
    if org_id <= 0:
        raise UsLaceyOperationalAccessError("This workspace cannot process documents.")

    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, org_id)
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
        if used >= limit:
            raise UsLaceyOperationalAccessError(
                "This workspace has reached its current operation limit."
            )

        return UsLaceyOperationalEntitlement(
            organization_id=org_id,
            account_status=account_status,
            subscription_status=subscription_status,
            monthly_operation_limit=limit,
            used_operations=used,
        )
    except UsLaceyOperationalAccessError:
        raise
    except Exception as exc:
        raise UsLaceyOperationalAccessError(
            "Unable to verify document-processing access right now."
        ) from exc
    finally:
        session.close()
