"""Transactional application of validated Lemon Squeezy orders."""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text

from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.lemon_squeezy import UsLaceyLemonPaidOrder


class UsLaceyLemonBillingError(RuntimeError):
    """Sanitized error safe to return from the webhook endpoint."""


@dataclass(frozen=True)
class UsLaceyLemonActivationResult:
    payment_status: str
    subscription_status: str
    account_status: str
    idempotent: bool


def apply_us_lacey_lemon_order(
    order: UsLaceyLemonPaidOrder,
) -> UsLaceyLemonActivationResult:
    """Apply one already signature-validated paid order atomically in PostgreSQL."""
    session = get_us_lacey_db_session()
    try:
        row = session.execute(
            text(
                """
                SELECT * FROM public.us_lacey_apply_lemon_order(
                    :organization_id,
                    :payment_public_id,
                    :provider_order_id,
                    :event_name,
                    :payload_sha256,
                    :amount_cents,
                    :currency,
                    :store_id,
                    :variant_id,
                    :test_mode
                )
                """
            ),
            {
                "organization_id": order.organization_id,
                "payment_public_id": order.payment_public_id,
                "provider_order_id": order.provider_order_id,
                "event_name": order.event_name,
                "payload_sha256": order.payload_sha256,
                "amount_cents": order.amount_cents,
                "currency": order.currency,
                "store_id": order.store_id,
                "variant_id": order.variant_id,
                "test_mode": order.test_mode,
            },
        ).mappings().one()
        session.commit()
        return UsLaceyLemonActivationResult(
            payment_status=str(row["payment_status"]),
            subscription_status=str(row["subscription_status"]),
            account_status=str(row["account_status"]),
            idempotent=bool(row["idempotent"]),
        )
    except Exception as exc:
        session.rollback()
        raise UsLaceyLemonBillingError("Unable to apply Lemon Squeezy payment.") from exc
    finally:
        session.close()
