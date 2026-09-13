from __future__ import annotations

import pytest

from litoral_trace.us_lacey.commercial import (
    UsLaceyCommercialConfigurationError,
    load_us_lacey_commercial_config,
)


def _env(**overrides: str) -> dict[str, str]:
    values = {
        "US_LACEY_PRIVATE_BETA_PRICE_CENTS": "12500",
        "US_LACEY_MONTHLY_OPERATION_LIMIT": "25",
        "US_LACEY_PAYMENT_PROVIDER": "LEMON_SQUEEZY",
        "US_LACEY_TERMS_VERSION": "terms-v1",
        "US_LACEY_PRIVACY_VERSION": "privacy-v1",
        "US_LACEY_BETA_TERMS_VERSION": "beta-v1",
        "US_LACEY_SUPPORT_EMAIL": "support@litoraltrace.com",
    }
    values.update(overrides)
    return values


def test_lemon_squeezy_remains_valid_without_transfer_instructions() -> None:
    config = load_us_lacey_commercial_config(_env())
    assert config.payment_provider == "LEMON_SQUEEZY"
    assert config.bank_transfer_instructions == ""


def test_wise_is_rejected_fail_closed() -> None:
    with pytest.raises(
        UsLaceyCommercialConfigurationError,
        match="MANUAL_BANK_TRANSFER or LEMON_SQUEEZY",
    ):
        load_us_lacey_commercial_config(
            _env(
                US_LACEY_PAYMENT_PROVIDER="WISE",
                US_LACEY_BANK_TRANSFER_INSTRUCTIONS="must never be used",
            )
        )


def test_manual_bank_transfer_requires_explicit_instructions() -> None:
    with pytest.raises(
        UsLaceyCommercialConfigurationError,
        match="US_LACEY_BANK_TRANSFER_INSTRUCTIONS",
    ):
        load_us_lacey_commercial_config(
            _env(US_LACEY_PAYMENT_PROVIDER="MANUAL_BANK_TRANSFER")
        )
