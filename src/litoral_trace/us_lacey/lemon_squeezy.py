"""Lemon Squeezy checkout and signed-webhook primitives for U.S. Lacey billing.

The first integration intentionally uses Lemon's hosted checkout URL instead of
an API key. Customer/account correlation is carried only through minimal custom
checkout data and payment activation is driven by a verified webhook.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import json
import os
from collections.abc import Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import UUID


class UsLaceyLemonConfigurationError(RuntimeError):
    """Raised when Lemon Squeezy configuration is missing or unsafe."""


class UsLaceyLemonWebhookError(RuntimeError):
    """Sanitized webhook validation error."""


@dataclass(frozen=True)
class UsLaceyLemonConfig:
    checkout_url: str
    webhook_secret: str
    store_id: int
    variant_id: int
    test_mode: bool


@dataclass(frozen=True)
class UsLaceyLemonPaidOrder:
    organization_id: int
    payment_public_id: UUID
    provider_order_id: str
    event_name: str
    payload_sha256: str
    amount_cents: int
    currency: str
    store_id: int
    variant_id: int
    test_mode: bool


def _required(env: Mapping[str, str], name: str) -> str:
    value = str(env.get(name, "")).strip()
    if not value:
        raise UsLaceyLemonConfigurationError(f"{name} is required.")
    return value


def _positive_int(env: Mapping[str, str], name: str) -> int:
    raw = _required(env, name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise UsLaceyLemonConfigurationError(f"{name} must be an integer.") from exc
    if value <= 0:
        raise UsLaceyLemonConfigurationError(f"{name} must be positive.")
    return value


def _strict_bool(env: Mapping[str, str], name: str) -> bool:
    raw = _required(env, name).lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise UsLaceyLemonConfigurationError(f"{name} must be a boolean value.")


def load_us_lacey_lemon_config(
    environ: Mapping[str, str] | None = None,
) -> UsLaceyLemonConfig:
    env = os.environ if environ is None else environ
    checkout_url = _required(env, "US_LACEY_LEMON_CHECKOUT_URL")
    parts = urlsplit(checkout_url)
    if parts.scheme != "https" or not parts.netloc or "/checkout/buy/" not in parts.path:
        raise UsLaceyLemonConfigurationError(
            "US_LACEY_LEMON_CHECKOUT_URL must be an HTTPS Lemon checkout URL."
        )
    secret = _required(env, "US_LACEY_LEMON_WEBHOOK_SECRET")
    if len(secret) < 16:
        raise UsLaceyLemonConfigurationError(
            "US_LACEY_LEMON_WEBHOOK_SECRET must contain at least 16 characters."
        )
    return UsLaceyLemonConfig(
        checkout_url=checkout_url,
        webhook_secret=secret,
        store_id=_positive_int(env, "US_LACEY_LEMON_STORE_ID"),
        variant_id=_positive_int(env, "US_LACEY_LEMON_VARIANT_ID"),
        test_mode=_strict_bool(env, "US_LACEY_LEMON_TEST_MODE"),
    )


def build_us_lacey_lemon_checkout_url(
    *,
    config: UsLaceyLemonConfig,
    organization_id: int,
    payment_public_id: UUID,
) -> str:
    if organization_id <= 0:
        raise UsLaceyLemonConfigurationError("Organization is invalid.")
    parts = urlsplit(config.checkout_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["checkout[custom][organization_id]"] = str(organization_id)
    query["checkout[custom][payment_public_id]"] = str(payment_public_id)
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment)
    )


def verify_us_lacey_lemon_signature(
    *, raw_body: bytes, signature: str, secret: str
) -> bool:
    if not raw_body or not signature or not secret:
        return False
    expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature.strip().lower())


def parse_us_lacey_lemon_paid_order(
    *,
    raw_body: bytes,
    signature: str,
    config: UsLaceyLemonConfig,
    expected_price_cents: int,
) -> UsLaceyLemonPaidOrder:
    """Validate one paid order event and return only data safe for activation."""
    if expected_price_cents <= 0:
        raise UsLaceyLemonWebhookError("Expected price is invalid.")
    if not verify_us_lacey_lemon_signature(
        raw_body=raw_body, signature=signature, secret=config.webhook_secret
    ):
        raise UsLaceyLemonWebhookError("Webhook signature is invalid.")
    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UsLaceyLemonWebhookError("Webhook payload is invalid.") from exc

    meta = payload.get("meta") if isinstance(payload, dict) else None
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(meta, dict) or not isinstance(data, dict):
        raise UsLaceyLemonWebhookError("Webhook payload is incomplete.")
    event_name = str(meta.get("event_name", "")).strip()
    if event_name != "order_created" or data.get("type") != "orders":
        raise UsLaceyLemonWebhookError("Webhook event is not a payable order.")

    attributes = data.get("attributes")
    custom = meta.get("custom_data")
    if not isinstance(attributes, dict) or not isinstance(custom, dict):
        raise UsLaceyLemonWebhookError("Webhook order metadata is incomplete.")
    if str(attributes.get("status", "")).lower() != "paid":
        raise UsLaceyLemonWebhookError("Order is not paid.")

    try:
        store_id = int(attributes["store_id"])
        amount_cents = int(attributes["subtotal"])
        organization_id = int(custom["organization_id"])
        payment_public_id = UUID(str(custom["payment_public_id"]))
        first_item = attributes["first_order_item"]
        if not isinstance(first_item, dict):
            raise TypeError("first_order_item")
        variant_id = int(first_item["variant_id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise UsLaceyLemonWebhookError("Webhook order identifiers are invalid.") from exc

    currency = str(attributes.get("currency", "")).upper()
    test_mode = attributes.get("test_mode")
    provider_order_id = str(data.get("id", "")).strip()
    if (
        organization_id <= 0
        or not provider_order_id
        or len(provider_order_id) > 128
        or currency != "USD"
        or store_id != config.store_id
        or variant_id != config.variant_id
        or test_mode is not config.test_mode
        or amount_cents != expected_price_cents
    ):
        raise UsLaceyLemonWebhookError("Webhook order does not match this offer.")

    return UsLaceyLemonPaidOrder(
        organization_id=organization_id,
        payment_public_id=payment_public_id,
        provider_order_id=provider_order_id,
        event_name=event_name,
        payload_sha256=hashlib.sha256(raw_body).hexdigest(),
        amount_cents=amount_cents,
        currency=currency,
        store_id=store_id,
        variant_id=variant_id,
        test_mode=test_mode,
    )
