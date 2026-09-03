"""Contracts that keep Wise out of the U.S. Lacey commercial flow."""
from __future__ import annotations

from pathlib import Path

import pytest

from litoral_trace.us_lacey.commercial import (
    UsLaceyCommercialConfigurationError,
    load_us_lacey_commercial_config,
)


MIGRATION = Path("alembic/versions/043_disable_us_lacey_wise_signup.py")


def _env(**overrides: str) -> dict[str, str]:
    values = {
        "US_LACEY_PRIVATE_BETA_PRICE_CENTS": "19900",
        "US_LACEY_MONTHLY_OPERATION_LIMIT": "25",
        "US_LACEY_PAYMENT_PROVIDER": "LEMON_SQUEEZY",
        "US_LACEY_TERMS_VERSION": "terms-v1",
        "US_LACEY_PRIVACY_VERSION": "privacy-v1",
        "US_LACEY_BETA_TERMS_VERSION": "early-access-v1",
        "US_LACEY_SUPPORT_EMAIL": "support@litoraltrace.com",
    }
    values.update(overrides)
    return values


def test_lemon_is_valid_without_manual_transfer_instructions() -> None:
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
                US_LACEY_BANK_TRANSFER_INSTRUCTIONS="unused",
            )
        )


def test_manual_bank_transfer_still_requires_explicit_instructions() -> None:
    with pytest.raises(
        UsLaceyCommercialConfigurationError,
        match="US_LACEY_BANK_TRANSFER_INSTRUCTIONS",
    ):
        load_us_lacey_commercial_config(
            _env(US_LACEY_PAYMENT_PROVIDER="MANUAL_BANK_TRANSFER")
        )


def test_043_wraps_041_registration_and_rejects_wise() -> None:
    text = MIGRATION.read_text(encoding="utf-8")
    assert 'revision: str = "043_042_us_lacey_owner_admin"' in text
    assert 'down_revision: Union[str, Sequence[str], None] = "042_us_lacey_owner_admin"' in text
    assert "RENAME TO us_lacey_self_register_pre_043" in text
    assert "normalized_provider NOT IN ('MANUAL_BANK_TRANSFER','LEMON_SQUEEZY')" in text
    assert "ERRCODE = '22023'" in text
    assert "REVOKE ALL ON FUNCTION {PRE_043_SIGNATURE} FROM {RUNTIME_ROLE}" in text
    assert "FROM public.us_lacey_self_register_pre_043(" in text
