"""Fail-closed commercial configuration for the U.S. Lacey self-service launch.

No price, payment destination or legal version is hard-coded here. Production
must provide each value explicitly so a deploy cannot silently charge the wrong
amount or accept the wrong legal text.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os


class UsLaceyCommercialConfigurationError(RuntimeError):
    pass


@dataclass(frozen=True)
class UsLaceyCommercialConfig:
    price_cents: int
    monthly_operation_limit: int
    payment_provider: str
    bank_transfer_instructions: str
    terms_version: str
    privacy_version: str
    beta_terms_version: str
    support_email: str


def _required(env: Mapping[str, str], name: str) -> str:
    value = str(env.get(name, "")).strip()
    if not value:
        raise UsLaceyCommercialConfigurationError(f"{name} is required.")
    return value


def _positive_int(env: Mapping[str, str], name: str) -> int:
    raw = _required(env, name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise UsLaceyCommercialConfigurationError(f"{name} must be an integer.") from exc
    if value <= 0:
        raise UsLaceyCommercialConfigurationError(f"{name} must be positive.")
    return value


def load_us_lacey_commercial_config(
    environ: Mapping[str, str] | None = None,
) -> UsLaceyCommercialConfig:
    env = os.environ if environ is None else environ
    provider = _required(env, "US_LACEY_PAYMENT_PROVIDER").upper()
    if provider not in {"MANUAL_BANK_TRANSFER", "LEMON_SQUEEZY"}:
        raise UsLaceyCommercialConfigurationError(
            "US_LACEY_PAYMENT_PROVIDER must be MANUAL_BANK_TRANSFER or LEMON_SQUEEZY."
        )

    bank_instructions = str(env.get("US_LACEY_BANK_TRANSFER_INSTRUCTIONS", "")).strip()
    if provider == "MANUAL_BANK_TRANSFER" and not bank_instructions:
        raise UsLaceyCommercialConfigurationError(
            "US_LACEY_BANK_TRANSFER_INSTRUCTIONS is required for manual bank transfer."
        )

    support_email = str(env.get("US_LACEY_SUPPORT_EMAIL", "support@litoraltrace.com")).strip().lower()
    if not support_email or "@" not in support_email:
        raise UsLaceyCommercialConfigurationError("US_LACEY_SUPPORT_EMAIL is invalid.")

    return UsLaceyCommercialConfig(
        price_cents=_positive_int(env, "US_LACEY_PRIVATE_BETA_PRICE_CENTS"),
        monthly_operation_limit=_positive_int(env, "US_LACEY_MONTHLY_OPERATION_LIMIT"),
        payment_provider=provider,
        bank_transfer_instructions=bank_instructions,
        terms_version=_required(env, "US_LACEY_TERMS_VERSION"),
        privacy_version=_required(env, "US_LACEY_PRIVACY_VERSION"),
        beta_terms_version=_required(env, "US_LACEY_BETA_TERMS_VERSION"),
        support_email=support_email,
    )
