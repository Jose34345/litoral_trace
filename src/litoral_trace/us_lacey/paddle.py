"""Paddle Billing checkout and signed-webhook primitives for U.S. Lacey."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
import re
import time
from uuid import UUID


_ID_RE = re.compile(r"^[a-z]{3}_[a-z0-9]{26}$")
_PRICE_RE = re.compile(r"^pri_[a-z0-9]{26}$")


class UsLaceyPaddleConfigurationError(RuntimeError):
    """Raised when Paddle runtime configuration is missing or unsafe."""


class UsLaceyPaddleWebhookError(RuntimeError):
    """Sanitized Paddle webhook validation error."""


@dataclass(frozen=True, slots=True)
class UsLaceyPaddleConfig:
    environment: str
    client_token: str
    price_id: str
    webhook_secret: str

    @property
    def sandbox(self) -> bool:
        return self.environment == "SANDBOX"


@dataclass(frozen=True, slots=True)
class UsLaceyPaddleTransaction:
    organization_id: int
    payment_public_id: UUID
    transaction_id: str
    event_id: str
    payload_sha256: str
    customer_id: str
    subscription_id: str
    price_id: str
    amount_cents: int
    currency: str
    period_start: datetime | None
    period_end: datetime | None
    provider_updated_at: datetime


@dataclass(frozen=True, slots=True)
class UsLaceyPaddleSubscriptionEvent:
    organization_id: int
    payment_public_id: UUID
    event_id: str
    payload_sha256: str
    customer_id: str
    subscription_id: str
    status: str
    next_billed_at: datetime | None
    provider_updated_at: datetime


def _required(env: Mapping[str, str], name: str) -> str:
    value = str(env.get(name, "")).strip()
    if not value:
        raise UsLaceyPaddleConfigurationError(f"{name} is required.")
    return value


def load_us_lacey_paddle_config(
    environ: Mapping[str, str] | None = None,
) -> UsLaceyPaddleConfig:
    env = os.environ if environ is None else environ
    environment = _required(env, "US_LACEY_PADDLE_ENVIRONMENT").upper()
    if environment not in {"SANDBOX", "PRODUCTION"}:
        raise UsLaceyPaddleConfigurationError(
            "US_LACEY_PADDLE_ENVIRONMENT must be SANDBOX or PRODUCTION."
        )

    client_token = _required(env, "US_LACEY_PADDLE_CLIENT_TOKEN")
    expected_prefix = "test_" if environment == "SANDBOX" else "live_"
    if not client_token.startswith(expected_prefix) or len(client_token) < 20:
        raise UsLaceyPaddleConfigurationError(
            "Paddle client token does not match the configured environment."
        )

    price_id = _required(env, "US_LACEY_PADDLE_PRICE_ID")
    if not _PRICE_RE.fullmatch(price_id):
        raise UsLaceyPaddleConfigurationError("US_LACEY_PADDLE_PRICE_ID is invalid.")

    webhook_secret = _required(env, "US_LACEY_PADDLE_WEBHOOK_SECRET")
    if len(webhook_secret) < 16:
        raise UsLaceyPaddleConfigurationError(
            "US_LACEY_PADDLE_WEBHOOK_SECRET must contain at least 16 characters."
        )

    return UsLaceyPaddleConfig(
        environment=environment,
        client_token=client_token,
        price_id=price_id,
        webhook_secret=webhook_secret,
    )


def _parse_signature_header(signature: str) -> tuple[int, list[str]]:
    timestamp: int | None = None
    hashes: list[str] = []
    for part in signature.split(";"):
        key, sep, value = part.strip().partition("=")
        if not sep:
            continue
        if key == "ts":
            try:
                timestamp = int(value)
            except ValueError as exc:
                raise UsLaceyPaddleWebhookError("Webhook signature timestamp is invalid.") from exc
        elif key == "h1" and value:
            hashes.append(value.lower())
    if timestamp is None or not hashes:
        raise UsLaceyPaddleWebhookError("Webhook signature is incomplete.")
    return timestamp, hashes


def verify_us_lacey_paddle_signature(
    *,
    raw_body: bytes,
    signature: str,
    secret: str,
    now: int | None = None,
    tolerance_seconds: int = 300,
) -> bool:
    if not raw_body or not signature or not secret:
        return False
    try:
        timestamp, hashes = _parse_signature_header(signature)
    except UsLaceyPaddleWebhookError:
        return False
    current = int(time.time()) if now is None else int(now)
    if tolerance_seconds > 0 and abs(current - timestamp) > tolerance_seconds:
        return False
    signed_payload = str(timestamp).encode("ascii") + b":" + raw_body
    expected = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    return any(hmac.compare_digest(expected, candidate) for candidate in hashes)


def _payload(
    *, raw_body: bytes, signature: str, config: UsLaceyPaddleConfig
) -> dict:
    if not verify_us_lacey_paddle_signature(
        raw_body=raw_body,
        signature=signature,
        secret=config.webhook_secret,
    ):
        raise UsLaceyPaddleWebhookError("Webhook signature is invalid.")
    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UsLaceyPaddleWebhookError("Webhook payload is invalid.") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
        raise UsLaceyPaddleWebhookError("Webhook payload is incomplete.")
    return payload


def _custom_identity(data: dict) -> tuple[int, UUID]:
    custom = data.get("custom_data")
    if not isinstance(custom, dict):
        raise UsLaceyPaddleWebhookError("Paddle custom data is missing.")
    try:
        organization_id = int(custom["organization_id"])
        payment_public_id = UUID(str(custom["payment_public_id"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise UsLaceyPaddleWebhookError("Paddle custom identity is invalid.") from exc
    if organization_id <= 0:
        raise UsLaceyPaddleWebhookError("Paddle organization identity is invalid.")
    return organization_id, payment_public_id


def _iso_datetime(value: object, *, required: bool = False) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        if required:
            raise UsLaceyPaddleWebhookError("Paddle event timestamp is missing.")
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise UsLaceyPaddleWebhookError("Paddle event timestamp is invalid.") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _identifier(value: object, prefix: str) -> str:
    result = str(value or "").strip()
    if not result.startswith(prefix + "_") or not _ID_RE.fullmatch(result):
        raise UsLaceyPaddleWebhookError(f"Paddle {prefix} identifier is invalid.")
    return result


def parse_us_lacey_paddle_transaction(
    *,
    raw_body: bytes,
    signature: str,
    config: UsLaceyPaddleConfig,
    expected_price_cents: int,
) -> UsLaceyPaddleTransaction:
    payload = _payload(raw_body=raw_body, signature=signature, config=config)
    if str(payload.get("event_type", "")) != "transaction.completed":
        raise UsLaceyPaddleWebhookError("Webhook event is not a completed transaction.")
    data = payload["data"]
    if str(data.get("status", "")) != "completed":
        raise UsLaceyPaddleWebhookError("Paddle transaction is not completed.")

    organization_id, payment_public_id = _custom_identity(data)
    event_id = _identifier(payload.get("event_id"), "evt")
    transaction_id = _identifier(data.get("id"), "txn")
    customer_id = _identifier(data.get("customer_id"), "ctm")
    subscription_id = _identifier(data.get("subscription_id"), "sub")

    items = data.get("items")
    if not isinstance(items, list) or len(items) != 1:
        raise UsLaceyPaddleWebhookError("Paddle transaction does not match this offer.")
    item = items[0]
    price = item.get("price") if isinstance(item, dict) else None
    if not isinstance(price, dict):
        raise UsLaceyPaddleWebhookError("Paddle transaction price is missing.")
    price_id = str(price.get("id", "")).strip()
    try:
        quantity = int(item.get("quantity"))
        unit_amount = int(price["unit_price"]["amount"])
    except (KeyError, TypeError, ValueError) as exc:
        raise UsLaceyPaddleWebhookError("Paddle transaction amount is invalid.") from exc
    currency = str(data.get("currency_code", "")).upper()
    if (
        price_id != config.price_id
        or quantity != 1
        or currency != "USD"
        or expected_price_cents <= 0
        or unit_amount != expected_price_cents
    ):
        raise UsLaceyPaddleWebhookError("Paddle transaction does not match this offer.")

    billing_period = data.get("billing_period")
    period_start = period_end = None
    if isinstance(billing_period, dict):
        period_start = _iso_datetime(billing_period.get("starts_at"))
        period_end = _iso_datetime(billing_period.get("ends_at"))

    return UsLaceyPaddleTransaction(
        organization_id=organization_id,
        payment_public_id=payment_public_id,
        transaction_id=transaction_id,
        event_id=event_id,
        payload_sha256=hashlib.sha256(raw_body).hexdigest(),
        customer_id=customer_id,
        subscription_id=subscription_id,
        price_id=price_id,
        amount_cents=unit_amount,
        currency=currency,
        period_start=period_start,
        period_end=period_end,
        provider_updated_at=_iso_datetime(payload.get("occurred_at"), required=True),
    )


def parse_us_lacey_paddle_subscription_event(
    *,
    raw_body: bytes,
    signature: str,
    config: UsLaceyPaddleConfig,
) -> UsLaceyPaddleSubscriptionEvent:
    payload = _payload(raw_body=raw_body, signature=signature, config=config)
    event_type = str(payload.get("event_type", ""))
    if event_type not in {"subscription.created", "subscription.updated", "subscription.canceled"}:
        raise UsLaceyPaddleWebhookError("Unsupported Paddle subscription event.")
    data = payload["data"]
    organization_id, payment_public_id = _custom_identity(data)
    status = str(data.get("status", "")).lower()
    if status not in {"active", "trialing", "past_due", "paused", "canceled"}:
        raise UsLaceyPaddleWebhookError("Paddle subscription status is unsupported.")
    return UsLaceyPaddleSubscriptionEvent(
        organization_id=organization_id,
        payment_public_id=payment_public_id,
        event_id=_identifier(payload.get("event_id"), "evt"),
        payload_sha256=hashlib.sha256(raw_body).hexdigest(),
        customer_id=_identifier(data.get("customer_id"), "ctm"),
        subscription_id=_identifier(data.get("id"), "sub"),
        status=status,
        next_billed_at=_iso_datetime(data.get("next_billed_at")),
        provider_updated_at=_iso_datetime(payload.get("occurred_at"), required=True),
    )
