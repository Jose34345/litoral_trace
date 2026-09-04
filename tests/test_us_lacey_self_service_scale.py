from __future__ import annotations

import hashlib
from uuid import UUID

import pytest

from litoral_trace.us_lacey.commercial import (
    UsLaceyCommercialConfig,
    UsLaceyCommercialConfigurationError,
    load_us_lacey_commercial_config,
)
from litoral_trace.us_lacey.self_service import (
    UsLaceySelfServiceError,
    register_us_lacey_company,
)
from litoral_trace.us_lacey.worker_db import (
    UsLaceyWorkerConfigurationError,
    get_us_lacey_worker_database_url,
    reset_us_lacey_worker_engine_state,
)


class _FakeMappings:
    def __init__(self, row):
        self._row = row

    def one(self):
        return self._row


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return _FakeMappings(self._row)


class _FakeSession:
    def __init__(self, row):
        self.row = row
        self.params = None
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def execute(self, statement, params=None):
        self.params = dict(params or {})
        return _FakeResult(self.row)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def _commercial_config() -> UsLaceyCommercialConfig:
    return UsLaceyCommercialConfig(
        price_cents=12500,
        monthly_operation_limit=25,
        payment_provider="LEMON_SQUEEZY",
        bank_transfer_instructions="",
        terms_version="terms-2026-08",
        privacy_version="privacy-2026-08",
        beta_terms_version="beta-2026-08",
        support_email="support@litoraltrace.com",
    )


def test_commercial_config_fails_closed_without_price_or_legal_versions():
    with pytest.raises(UsLaceyCommercialConfigurationError):
        load_us_lacey_commercial_config({"US_LACEY_PAYMENT_PROVIDER": "LEMON_SQUEEZY"})


def test_commercial_config_accepts_explicit_launch_values():
    config = load_us_lacey_commercial_config(
        {
            "US_LACEY_PAYMENT_PROVIDER": "LEMON_SQUEEZY",
            "US_LACEY_PRIVATE_BETA_PRICE_CENTS": "12500",
            "US_LACEY_MONTHLY_OPERATION_LIMIT": "25",
            "US_LACEY_TERMS_VERSION": "terms-v1",
            "US_LACEY_PRIVACY_VERSION": "privacy-v1",
            "US_LACEY_BETA_TERMS_VERSION": "beta-v1",
        }
    )
    assert config.price_cents == 12500
    assert config.monthly_operation_limit == 25
    assert config.payment_provider == "LEMON_SQUEEZY"
    assert config.bank_transfer_instructions == ""


def test_registration_never_persists_raw_verification_token(monkeypatch):
    fake = _FakeSession(
        {
            "organization_id": 41,
            "user_id": 77,
            "payment_public_id": UUID("11111111-1111-1111-1111-111111111111"),
            "payment_reference": "LT-US-ABC123",
            "amount_cents": 12500,
            "account_status": "PENDING_EMAIL",
        }
    )
    monkeypatch.setattr(
        "litoral_trace.us_lacey.self_service.get_us_lacey_db_session",
        lambda: fake,
    )
    monkeypatch.setattr(
        "litoral_trace.us_lacey.self_service.hash_password",
        lambda _password: "$2b$12$" + ("x" * 53),
    )
    monkeypatch.setattr(
        "litoral_trace.us_lacey.self_service.secrets.token_urlsafe",
        lambda _size: "raw-verification-token",
    )

    result = register_us_lacey_company(
        legal_name="Example Imports LLC",
        business_type="IMPORTER",
        admin_name="Alex Importer",
        admin_email="alex@example.com",
        password="correct-horse-123",
        commercial_config=_commercial_config(),
    )

    assert result.verification_token == "raw-verification-token"
    assert fake.params["verification_hash"] == hashlib.sha256(
        b"raw-verification-token"
    ).hexdigest()
    assert fake.params["verification_hash"] != result.verification_token
    assert fake.params["price_cents"] == 12500
    assert fake.params["monthly_operation_limit"] == 25
    assert fake.committed is True
    assert fake.closed is True


def test_registration_rejects_short_password_before_database_access():
    with pytest.raises(UsLaceySelfServiceError):
        register_us_lacey_company(
            legal_name="Example Imports LLC",
            business_type="IMPORTER",
            admin_name="Alex Importer",
            admin_email="alex@example.com",
            password="short",
            commercial_config=_commercial_config(),
        )


def _set_us_runtime_env(monkeypatch):
    monkeypatch.setenv("US_LACEY_ENVIRONMENT", "test")
    monkeypatch.setenv(
        "US_LACEY_DATABASE_URL",
        "postgresql+psycopg://litoral_trace_app:runtime@db.example:5432/lacey_us",
    )
    monkeypatch.setenv("US_LACEY_STORAGE_BUCKET", "lacey-us-test")
    monkeypatch.setenv("US_LACEY_STORAGE_PREFIX", "pilot")
    monkeypatch.delenv("DATABASE_URL", raising=False)


def test_worker_database_must_target_same_us_database_with_different_role(monkeypatch):
    reset_us_lacey_worker_engine_state()
    _set_us_runtime_env(monkeypatch)
    monkeypatch.setenv(
        "US_LACEY_WORKER_DATABASE_URL",
        "postgresql+psycopg://litoral_trace_worker_executor:worker@db.example:5432/lacey_us",
    )
    assert "litoral_trace_worker_executor" in get_us_lacey_worker_database_url()


def test_worker_database_rejects_web_runtime_credentials(monkeypatch):
    reset_us_lacey_worker_engine_state()
    _set_us_runtime_env(monkeypatch)
    monkeypatch.setenv(
        "US_LACEY_WORKER_DATABASE_URL",
        "postgresql+psycopg://litoral_trace_app:another-password@db.example:5432/lacey_us",
    )
    with pytest.raises(UsLaceyWorkerConfigurationError):
        get_us_lacey_worker_database_url()


def test_worker_database_rejects_different_database(monkeypatch):
    reset_us_lacey_worker_engine_state()
    _set_us_runtime_env(monkeypatch)
    monkeypatch.setenv(
        "US_LACEY_WORKER_DATABASE_URL",
        "postgresql+psycopg://litoral_trace_worker_executor:worker@db.example:5432/argentina",
    )
    with pytest.raises(UsLaceyWorkerConfigurationError):
        get_us_lacey_worker_database_url()
