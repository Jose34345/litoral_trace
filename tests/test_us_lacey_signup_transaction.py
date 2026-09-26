from __future__ import annotations

from uuid import UUID

import pytest

from litoral_trace.us_lacey.commercial import UsLaceyCommercialConfig
from litoral_trace.us_lacey.self_service import (
    UsLaceySelfServiceError,
    register_us_lacey_company,
)


class _Mappings:
    def __init__(self, row):
        self.row = row

    def one(self):
        return self.row


class _Result:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return _Mappings(self.row)


class _Session:
    def __init__(self):
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def execute(self, _statement, _params=None):
        return _Result(
            {
                "organization_id": 10,
                "user_id": 20,
                "payment_public_id": UUID("11111111-1111-1111-1111-111111111111"),
                "payment_reference": "LT-US-ROLLBACK",
                "amount_cents": 12500,
                "account_status": "PENDING_EMAIL",
            }
        )

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def _commercial() -> UsLaceyCommercialConfig:
    return UsLaceyCommercialConfig(
        price_cents=12500,
        monthly_operation_limit=25,
        payment_provider="WISE",
        bank_transfer_instructions="CI only",
        terms_version="terms-v1",
        privacy_version="privacy-v1",
        beta_terms_version="beta-v1",
        support_email="support@litoraltrace.com",
    )


def test_failed_verification_delivery_rolls_back_signup(monkeypatch):
    fake = _Session()
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
        lambda _size: "verification-token",
    )

    def fail_delivery(_recipient: str, _company_name: str, _token: str) -> None:
        raise RuntimeError("SMTP unavailable")

    with pytest.raises(UsLaceySelfServiceError, match="No account was created"):
        register_us_lacey_company(
            legal_name="Rollback Imports LLC",
            business_type="IMPORTER",
            admin_name="Rollback Admin",
            admin_email="rollback@example.com",
            password="correct-horse-123",
            commercial_config=_commercial(),
            verification_delivery=fail_delivery,
        )

    assert fake.rolled_back is True
    assert fake.committed is False
    assert fake.closed is True
