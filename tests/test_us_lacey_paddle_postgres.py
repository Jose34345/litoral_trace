"""PostgreSQL acceptance test for the U.S. Lacey Paddle billing state machine."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from litoral_trace.us_lacey.commercial import UsLaceyCommercialConfig
from litoral_trace.us_lacey.db import reset_us_lacey_engine_state
from litoral_trace.us_lacey.paddle import (
    UsLaceyPaddleSubscriptionEvent,
    UsLaceyPaddleTransaction,
)
from litoral_trace.us_lacey.paddle_billing import (
    apply_us_lacey_paddle_subscription_event,
    apply_us_lacey_paddle_transaction,
)
from litoral_trace.us_lacey.self_service import (
    register_us_lacey_company,
    verify_us_lacey_email,
)
from litoral_trace.us_lacey.schema_compatibility import _revision_satisfies_required


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_DATABASE_URL")
    or not os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")
    or not os.environ.get("US_LACEY_TEST_AUDIT_DATABASE_URL"),
    reason="requires isolated U.S. Lacey PostgreSQL runtime, migration, and audit databases",
)

PRICE = 14900
CUSTOMER_ID = "ctm_" + "d" * 26
SUBSCRIPTION_ID = "sub_" + "e" * 26


def _commercial_config() -> UsLaceyCommercialConfig:
    return UsLaceyCommercialConfig(
        price_cents=PRICE,
        monthly_operation_limit=100,
        payment_provider="PADDLE",
        bank_transfer_instructions="",
        terms_version="terms-paddle-test",
        privacy_version="privacy-paddle-test",
        beta_terms_version="beta-paddle-test",
        support_email="support@litoraltrace.com",
    )


def _sha(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _subscription_event(
    *,
    organization_id: int,
    payment_public_id,
    event_id: str,
    status: str,
    occurred_at: datetime,
) -> UsLaceyPaddleSubscriptionEvent:
    return UsLaceyPaddleSubscriptionEvent(
        organization_id=organization_id,
        payment_public_id=payment_public_id,
        event_id=event_id,
        payload_sha256=_sha(event_id + status),
        customer_id=CUSTOMER_ID,
        subscription_id=SUBSCRIPTION_ID,
        status=status,
        next_billed_at=occurred_at + timedelta(days=30) if status == "active" else None,
        provider_updated_at=occurred_at,
    )


def _transaction(
    *,
    organization_id: int,
    payment_public_id,
    transaction_id: str,
    event_id: str,
    occurred_at: datetime,
) -> UsLaceyPaddleTransaction:
    return UsLaceyPaddleTransaction(
        organization_id=organization_id,
        payment_public_id=payment_public_id,
        transaction_id=transaction_id,
        event_id=event_id,
        payload_sha256=_sha(event_id + transaction_id),
        customer_id=CUSTOMER_ID,
        subscription_id=SUBSCRIPTION_ID,
        price_id="pri_" + "a" * 26,
        amount_cents=PRICE,
        currency="USD",
        period_start=occurred_at,
        period_end=occurred_at + timedelta(days=30),
        provider_updated_at=occurred_at,
    )


def test_paddle_signup_activation_renewal_idempotency_and_cancel() -> None:
    reset_us_lacey_engine_state()
    migration_engine = create_engine(
        os.environ["TEST_POSTGRES_MIGRATION_DATABASE_URL"],
        pool_pre_ping=True,
        hide_parameters=True,
    )
    audit_engine = create_engine(
        os.environ["US_LACEY_TEST_AUDIT_DATABASE_URL"],
        pool_pre_ping=True,
        hide_parameters=True,
    )
    with migration_engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    if not _revision_satisfies_required(
        current=str(revision), required="070_us_lacey_paddle_billing"
    ):
        migration_engine.dispose()
        audit_engine.dispose()
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_070")

    suffix = uuid4().hex[:12]
    registered = register_us_lacey_company(
        legal_name=f"Paddle State Machine {suffix} LLC",
        business_type="IMPORTER",
        admin_name="Paddle Test Admin",
        admin_email=f"paddle-{suffix}@example.com",
        password="correct-horse-paddle-123",
        commercial_config=_commercial_config(),
    )
    org_id = registered.organization_id
    try:
        verified = verify_us_lacey_email(registered.verification_token)
        assert verified.account_status == "PAYMENT_PENDING"

        with audit_engine.begin() as connection:
            initial = connection.execute(
                text(
                    """
                    SELECT p.status AS payment_status, s.status AS subscription_status,
                           s.billing_provider, profile.account_status
                    FROM public.us_lacey_payments p
                    JOIN public.us_lacey_subscriptions s
                      ON s.id=p.subscription_id AND s.organization_id=p.organization_id
                    JOIN public.us_lacey_organization_profiles profile
                      ON profile.organization_id=p.organization_id
                    WHERE p.organization_id=:org_id AND p.public_id=:payment_id
                    """
                ),
                {"org_id": org_id, "payment_id": registered.payment_public_id},
            ).mappings().one()
        assert initial["payment_status"] == "PENDING"
        assert initial["subscription_status"] == "PENDING"
        assert initial["billing_provider"] == "PADDLE"
        assert initial["account_status"] == "PAYMENT_PENDING"

        t0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
        created = apply_us_lacey_paddle_subscription_event(
            _subscription_event(
                organization_id=org_id,
                payment_public_id=registered.payment_public_id,
                event_id="evt_" + "a" * 26,
                status="active",
                occurred_at=t0,
            )
        )
        assert created.payment_status == "PENDING"
        assert created.subscription_status == "PENDING"
        assert created.account_status == "PAYMENT_PENDING"

        activated = apply_us_lacey_paddle_transaction(
            _transaction(
                organization_id=org_id,
                payment_public_id=registered.payment_public_id,
                transaction_id="txn_" + "b" * 26,
                event_id="evt_" + "b" * 26,
                occurred_at=t0 + timedelta(seconds=1),
            )
        )
        assert activated.payment_status == "VERIFIED"
        assert activated.subscription_status == "ACTIVE"
        assert activated.account_status == "ACTIVE"
        assert activated.idempotent is False

        with audit_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE public.us_lacey_subscriptions "
                    "SET used_operations=73 WHERE organization_id=:org_id"
                ),
                {"org_id": org_id},
            )

        renewal_event = _transaction(
            organization_id=org_id,
            payment_public_id=registered.payment_public_id,
            transaction_id="txn_" + "c" * 26,
            event_id="evt_" + "c" * 26,
            occurred_at=t0 + timedelta(days=30),
        )
        renewed = apply_us_lacey_paddle_transaction(renewal_event)
        assert renewed.idempotent is False

        replayed = apply_us_lacey_paddle_transaction(renewal_event)
        assert replayed.idempotent is True

        with audit_engine.begin() as connection:
            used_operations = connection.execute(
                text(
                    "SELECT used_operations FROM public.us_lacey_subscriptions "
                    "WHERE organization_id=:org_id"
                ),
                {"org_id": org_id},
            ).scalar_one()
        assert used_operations == 0

        canceled = apply_us_lacey_paddle_subscription_event(
            _subscription_event(
                organization_id=org_id,
                payment_public_id=registered.payment_public_id,
                event_id="evt_" + "d" * 26,
                status="canceled",
                occurred_at=t0 + timedelta(days=31),
            )
        )
        assert canceled.payment_status == "VERIFIED"
        assert canceled.subscription_status == "CANCELED"
        assert canceled.account_status == "SUSPENDED"

    finally:
        with audit_engine.begin() as connection:
            connection.execute(
                text("DELETE FROM public.organizations WHERE id=:org_id"),
                {"org_id": org_id},
            )
        migration_engine.dispose()
        audit_engine.dispose()
