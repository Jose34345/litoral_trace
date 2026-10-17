"""Fail-closed Lemon Squeezy contracts for U.S. Lacey billing."""
from __future__ import annotations

import hashlib
import hmac
import json
from urllib.parse import parse_qs, urlsplit
from uuid import UUID

import pytest

from litoral_trace.us_lacey.lemon_squeezy import (
    UsLaceyLemonConfigurationError,
    UsLaceyLemonWebhookError,
    build_us_lacey_lemon_checkout_url,
    load_us_lacey_lemon_config,
    parse_us_lacey_lemon_paid_order,
)


PAYMENT_ID = UUID("d8123ecb-3901-46b8-85af-e0b13f048177")
SECRET = "test-signing-secret-2026"


def _env(**overrides: str) -> dict[str, str]:
    values = {
        "US_LACEY_LEMON_CHECKOUT_URL": "https://litoral-trace.lemonsqueezy.com/checkout/buy/test-variant",
        "US_LACEY_LEMON_WEBHOOK_SECRET": SECRET,
        "US_LACEY_LEMON_STORE_ID": "321",
        "US_LACEY_LEMON_VARIANT_ID": "654",
        "US_LACEY_LEMON_TEST_MODE": "1",
    }
    values.update(overrides)
    return values


def _payload(**attribute_overrides: object) -> bytes:
    attributes: dict[str, object] = {
        "store_id": 321,
        "currency": "USD",
        "subtotal": 19900,
        "status": "paid",
        "test_mode": True,
        "first_order_item": {"variant_id": 654},
    }
    attributes.update(attribute_overrides)
    return json.dumps(
        {
            "meta": {
                "event_name": "order_created",
                "custom_data": {
                    "organization_id": "42",
                    "payment_public_id": str(PAYMENT_ID),
                },
            },
            "data": {"type": "orders", "id": "9001", "attributes": attributes},
        },
        separators=(",", ":"),
    ).encode("utf-8")


def _signature(body: bytes) -> str:
    return hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()


def test_lemon_config_is_fail_closed_and_test_mode_explicit() -> None:
    config = load_us_lacey_lemon_config(_env())
    assert config.store_id == 321
    assert config.variant_id == 654
    assert config.test_mode is True

    with pytest.raises(UsLaceyLemonConfigurationError):
        load_us_lacey_lemon_config(_env(US_LACEY_LEMON_WEBHOOK_SECRET="short"))
    with pytest.raises(UsLaceyLemonConfigurationError):
        load_us_lacey_lemon_config(_env(US_LACEY_LEMON_TEST_MODE="maybe"))


def test_checkout_url_carries_only_local_correlation_ids() -> None:
    config = load_us_lacey_lemon_config(_env())
    url = build_us_lacey_lemon_checkout_url(
        config=config, organization_id=42, payment_public_id=PAYMENT_ID
    )
    query = parse_qs(urlsplit(url).query)
    assert query["checkout[custom][organization_id]"] == ["42"]
    assert query["checkout[custom][payment_public_id]"] == [str(PAYMENT_ID)]
    assert "secret" not in url.lower()


def test_paid_order_requires_valid_signature_and_exact_offer() -> None:
    config = load_us_lacey_lemon_config(_env())
    body = _payload()
    order = parse_us_lacey_lemon_paid_order(
        raw_body=body,
        signature=_signature(body),
        config=config,
        expected_price_cents=19900,
    )
    assert order.organization_id == 42
    assert order.payment_public_id == PAYMENT_ID
    assert order.provider_order_id == "9001"
    assert order.amount_cents == 19900
    assert order.currency == "USD"
    assert order.test_mode is True

    with pytest.raises(UsLaceyLemonWebhookError):
        parse_us_lacey_lemon_paid_order(
            raw_body=body,
            signature="0" * 64,
            config=config,
            expected_price_cents=19900,
        )


@pytest.mark.parametrize(
    "override",
    [
        {"store_id": 999},
        {"currency": "EUR"},
        {"subtotal": 19899},
        {"status": "pending"},
        {"test_mode": False},
        {"first_order_item": {"variant_id": 999}},
    ],
)
def test_paid_order_rejects_mismatched_or_unpaid_events(override: dict[str, object]) -> None:
    config = load_us_lacey_lemon_config(_env())
    body = _payload(**override)
    with pytest.raises(UsLaceyLemonWebhookError):
        parse_us_lacey_lemon_paid_order(
            raw_body=body,
            signature=_signature(body),
            config=config,
            expected_price_cents=19900,
        )
