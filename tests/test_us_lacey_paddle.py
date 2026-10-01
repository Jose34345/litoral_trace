"""Fail-closed Paddle Billing contracts for U.S. Lacey."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import hmac
import json
import time
from uuid import UUID

import pytest

from litoral_trace.us_lacey.paddle import (
    UsLaceyPaddleConfigurationError,
    UsLaceyPaddleWebhookError,
    load_us_lacey_paddle_config,
    parse_us_lacey_paddle_subscription_event,
    parse_us_lacey_paddle_transaction,
    verify_us_lacey_paddle_signature,
)


PAYMENT_ID = UUID("d8123ecb-3901-46b8-85af-e0b13f048177")
SECRET = "pdl_webhook_secret_test_2026"
PRICE_ID = "pri_" + "a" * 26
TXN_ID = "txn_" + "b" * 26
EVENT_ID = "evt_" + "c" * 26
CUSTOMER_ID = "ctm_" + "d" * 26
SUBSCRIPTION_ID = "sub_" + "e" * 26


def _env(**overrides: str) -> dict[str, str]:
    values = {
        "US_LACEY_PADDLE_ENVIRONMENT": "SANDBOX",
        "US_LACEY_PADDLE_CLIENT_TOKEN": "test_" + "x" * 40,
        "US_LACEY_PADDLE_PRICE_ID": PRICE_ID,
        "US_LACEY_PADDLE_WEBHOOK_SECRET": SECRET,
    }
    values.update(overrides)
    return values


def _sign(body: bytes, *, timestamp: int | None = None) -> str:
    ts = int(time.time()) if timestamp is None else timestamp
    signed = str(ts).encode("ascii") + b":" + body
    digest = hmac.new(SECRET.encode(), signed, hashlib.sha256).hexdigest()
    return f"ts={ts};h1={digest}"


def _transaction_payload(**overrides: object) -> bytes:
    data: dict[str, object] = {
        "id": TXN_ID,
        "status": "completed",
        "customer_id": CUSTOMER_ID,
        "subscription_id": SUBSCRIPTION_ID,
        "currency_code": "USD",
        "origin": "web",
        "custom_data": {
            "organization_id": 42,
            "payment_public_id": str(PAYMENT_ID),
        },
        "items": [
            {
                "quantity": 1,
                "price": {
                    "id": PRICE_ID,
                    "unit_price": {"amount": "14900", "currency_code": "USD"},
                    "trial_period": None,
                },
            }
        ],
        "details": {
            "totals": {
                "subtotal": "14900",
                "discount": "0",
                "credit": "0",
                "grand_total": "14900",
            }
        },
        "payments": [{"status": "captured", "amount": "14900"}],
        "billing_period": {
            "starts_at": "2026-10-01T12:00:00Z",
            "ends_at": "2026-11-01T12:00:00Z",
        },
    }
    data.update(overrides)
    return json.dumps(
        {
            "event_id": EVENT_ID,
            "event_type": "transaction.completed",
            "occurred_at": "2026-10-01T12:00:05Z",
            "data": data,
        },
        separators=(",", ":"),
    ).encode()


def _subscription_payload(*, status: str = "active") -> bytes:
    return json.dumps(
        {
            "event_id": EVENT_ID,
            "event_type": "subscription.updated",
            "occurred_at": "2026-10-01T12:00:06Z",
            "data": {
                "id": SUBSCRIPTION_ID,
                "customer_id": CUSTOMER_ID,
                "status": status,
                "currency_code": "USD",
                "items": [
                    {
                        "quantity": 1,
                        "price": {"id": PRICE_ID},
                    }
                ],
                "next_billed_at": "2026-11-01T12:00:00Z",
                "custom_data": {
                    "organization_id": 42,
                    "payment_public_id": str(PAYMENT_ID),
                },
            },
        },
        separators=(",", ":"),
    ).encode()


def test_paddle_config_is_environment_bound() -> None:
    config = load_us_lacey_paddle_config(_env())
    assert config.sandbox is True
    assert config.price_id == PRICE_ID

    with pytest.raises(UsLaceyPaddleConfigurationError):
        load_us_lacey_paddle_config(
            _env(US_LACEY_PADDLE_CLIENT_TOKEN="live_" + "x" * 40)
        )
    with pytest.raises(UsLaceyPaddleConfigurationError):
        load_us_lacey_paddle_config(
            _env(US_LACEY_PADDLE_ENVIRONMENT="maybe")
        )


def test_signature_verification_uses_raw_body_and_rejects_stale_timestamp() -> None:
    body = b'{"hello":"paddle"}'
    now = 1_800_000_000
    signature = _sign(body, timestamp=now)
    assert verify_us_lacey_paddle_signature(
        raw_body=body,
        signature=signature,
        secret=SECRET,
        now=now,
    )
    assert not verify_us_lacey_paddle_signature(
        raw_body=body + b" ",
        signature=signature,
        secret=SECRET,
        now=now,
    )
    assert not verify_us_lacey_paddle_signature(
        raw_body=body,
        signature=signature,
        secret=SECRET,
        now=now + 301,
    )


def test_completed_transaction_requires_exact_offer_and_correlation() -> None:
    config = load_us_lacey_paddle_config(_env())
    body = _transaction_payload()
    event = parse_us_lacey_paddle_transaction(
        raw_body=body,
        signature=_sign(body),
        config=config,
        expected_price_cents=14900,
    )
    assert event.organization_id == 42
    assert event.payment_public_id == PAYMENT_ID
    assert event.transaction_id == TXN_ID
    assert event.customer_id == CUSTOMER_ID
    assert event.subscription_id == SUBSCRIPTION_ID
    assert event.amount_cents == 14900
    assert event.currency == "USD"
    assert event.period_end == datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "override",
    [
        {"status": "paid"},
        {"currency_code": "EUR"},
        {"origin": "subscription_update"},
        {"subscription_id": None},
        {
            "items": [
                {
                    "quantity": 2,
                    "price": {
                        "id": PRICE_ID,
                        "unit_price": {"amount": "14900", "currency_code": "USD"},
                    },
                }
            ]
        },
        {
            "items": [
                {
                    "quantity": 1,
                    "price": {
                        "id": "pri_" + "z" * 26,
                        "unit_price": {"amount": "14900", "currency_code": "USD"},
                    },
                }
            ]
        },
    ],
)
def test_transaction_rejects_noncanonical_checkout(override: dict[str, object]) -> None:
    config = load_us_lacey_paddle_config(_env())
    body = _transaction_payload(**override)
    with pytest.raises(UsLaceyPaddleWebhookError):
        parse_us_lacey_paddle_transaction(
            raw_body=body,
            signature=_sign(body),
            config=config,
            expected_price_cents=14900,
        )


def test_subscription_event_carries_same_tenant_identity() -> None:
    config = load_us_lacey_paddle_config(_env())
    body = _subscription_payload(status="past_due")
    event = parse_us_lacey_paddle_subscription_event(
        raw_body=body,
        signature=_sign(body),
        config=config,
    )
    assert event.organization_id == 42
    assert event.payment_public_id == PAYMENT_ID
    assert event.subscription_id == SUBSCRIPTION_ID
    assert event.status == "past_due"



@pytest.mark.parametrize(
    "override",
    [
        {"details": {"totals": {"subtotal": "14900", "discount": "100", "credit": "0", "grand_total": "14800"}}},
        {"details": {"totals": {"subtotal": "14900", "discount": "0", "credit": "100", "grand_total": "14800"}}},
        {"details": {"totals": {"subtotal": "9900", "discount": "0", "credit": "0", "grand_total": "9900"}}},
        {"payments": [{"status": "captured", "amount": "14899"}]},
    ],
)
def test_transaction_rejects_financial_mismatch(override: dict[str, object]) -> None:
    config = load_us_lacey_paddle_config(_env())
    body = _transaction_payload(**override)
    with pytest.raises(UsLaceyPaddleWebhookError):
        parse_us_lacey_paddle_transaction(
            raw_body=body,
            signature=_sign(body),
            config=config,
            expected_price_cents=14900,
        )


def test_transaction_rejects_trial_price() -> None:
    config = load_us_lacey_paddle_config(_env())
    body = _transaction_payload(
        items=[
            {
                "quantity": 1,
                "price": {
                    "id": PRICE_ID,
                    "unit_price": {"amount": "14900", "currency_code": "USD"},
                    "trial_period": {"interval": "day", "frequency": 14},
                },
            }
        ]
    )
    with pytest.raises(UsLaceyPaddleWebhookError):
        parse_us_lacey_paddle_transaction(
            raw_body=body,
            signature=_sign(body),
            config=config,
            expected_price_cents=14900,
        )


def test_subscription_event_rejects_wrong_offer() -> None:
    config = load_us_lacey_paddle_config(_env())
    payload = json.loads(_subscription_payload().decode())
    payload["data"]["items"][0]["price"]["id"] = "pri_" + "z" * 26
    body = json.dumps(payload, separators=(",", ":")).encode()
    with pytest.raises(UsLaceyPaddleWebhookError):
        parse_us_lacey_paddle_subscription_event(
            raw_body=body,
            signature=_sign(body),
            config=config,
        )

def test_invalid_signature_never_parses_provider_event() -> None:
    config = load_us_lacey_paddle_config(_env())
    body = _transaction_payload()
    with pytest.raises(UsLaceyPaddleWebhookError):
        parse_us_lacey_paddle_transaction(
            raw_body=body,
            signature="ts=1;h1=" + "0" * 64,
            config=config,
            expected_price_cents=14900,
        )
