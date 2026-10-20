"""Transactional application of validated Paddle events."""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text

from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.paddle import (
    UsLaceyPaddleSubscriptionEvent,
    UsLaceyPaddleTransaction,
)


class UsLaceyPaddleBillingError(RuntimeError):
    """Sanitized error safe to return from the webhook endpoint."""


@dataclass(frozen=True, slots=True)
class UsLaceyPaddleBillingResult:
    payment_status: str
    subscription_status: str
    account_status: str
    idempotent: bool


def apply_us_lacey_paddle_transaction(
    event: UsLaceyPaddleTransaction,
) -> UsLaceyPaddleBillingResult:
    session = get_us_lacey_db_session()
    try:
        row = session.execute(
            text(
                """
                SELECT * FROM public.us_lacey_apply_paddle_transaction(
                    :organization_id,
                    :payment_public_id,
                    :transaction_id,
                    :event_id,
                    :payload_sha256,
                    :customer_id,
                    :subscription_id,
                    :amount_cents,
                    :currency,
                    :period_start,
                    :period_end,
                    :provider_updated_at
                )
                """
            ),
            {
                "organization_id": event.organization_id,
                "payment_public_id": event.payment_public_id,
                "transaction_id": event.transaction_id,
                "event_id": event.event_id,
                "payload_sha256": event.payload_sha256,
                "customer_id": event.customer_id,
                "subscription_id": event.subscription_id,
                "amount_cents": event.amount_cents,
                "currency": event.currency,
                "period_start": event.period_start,
                "period_end": event.period_end,
                "provider_updated_at": event.provider_updated_at,
            },
        ).mappings().one()
        session.commit()
        return UsLaceyPaddleBillingResult(
            payment_status=str(row["payment_status"]),
            subscription_status=str(row["subscription_status"]),
            account_status=str(row["account_status"]),
            idempotent=bool(row["idempotent"]),
        )
    except Exception as exc:
        session.rollback()
        raise UsLaceyPaddleBillingError("Unable to apply Paddle transaction.") from exc
    finally:
        session.close()


def apply_us_lacey_paddle_subscription_event(
    event: UsLaceyPaddleSubscriptionEvent,
) -> UsLaceyPaddleBillingResult:
    session = get_us_lacey_db_session()
    try:
        row = session.execute(
            text(
                """
                SELECT * FROM public.us_lacey_apply_paddle_subscription_event(
                    :organization_id,
                    :payment_public_id,
                    :event_id,
                    :payload_sha256,
                    :customer_id,
                    :subscription_id,
                    :provider_status,
                    :next_billed_at,
                    :provider_updated_at
                )
                """
            ),
            {
                "organization_id": event.organization_id,
                "payment_public_id": event.payment_public_id,
                "event_id": event.event_id,
                "payload_sha256": event.payload_sha256,
                "customer_id": event.customer_id,
                "subscription_id": event.subscription_id,
                "provider_status": event.status,
                "next_billed_at": event.next_billed_at,
                "provider_updated_at": event.provider_updated_at,
            },
        ).mappings().one()
        session.commit()
        return UsLaceyPaddleBillingResult(
            payment_status=str(row["payment_status"]),
            subscription_status=str(row["subscription_status"]),
            account_status=str(row["account_status"]),
            idempotent=bool(row["idempotent"]),
        )
    except Exception as exc:
        session.rollback()
        raise UsLaceyPaddleBillingError("Unable to apply Paddle subscription event.") from exc
    finally:
        session.close()
