from __future__ import annotations

import hashlib
import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from litoral_trace.us_lacey.commercial import UsLaceyCommercialConfig
from litoral_trace.us_lacey.db import reset_us_lacey_engine_state
from litoral_trace.us_lacey.portal_auth import (
    UsLaceyPortalAuthError,
    login_us_lacey_user,
    logout_us_lacey_user,
    resolve_us_lacey_session,
)
from litoral_trace.us_lacey.self_service import (
    get_us_lacey_billing_summary,
    register_us_lacey_company,
    verify_us_lacey_email,
)


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_DATABASE_URL")
    or not os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL"),
    reason="requires the isolated U.S. PostgreSQL runtime and migration databases",
)


def _commercial_config() -> UsLaceyCommercialConfig:
    return UsLaceyCommercialConfig(
        price_cents=12500,
        monthly_operation_limit=25,
        payment_provider="WISE",
        bank_transfer_instructions="CI-only payment instructions",
        terms_version="terms-ci-v1",
        privacy_version="privacy-ci-v1",
        beta_terms_version="beta-ci-v1",
        support_email="support@litoraltrace.com",
    )


def test_verified_customer_gets_isolated_opaque_session_and_billing():
    reset_us_lacey_engine_state()
    suffix = uuid4().hex[:12]
    email = f"portal-{suffix}@example.com"
    password = "correct-horse-portal-123"

    registered = register_us_lacey_company(
        legal_name=f"Portal Gate Imports {suffix} LLC",
        business_type="IMPORTER",
        admin_name="Portal Gate Admin",
        admin_email=email,
        password=password,
        commercial_config=_commercial_config(),
    )
    assert registered.account_status == "PENDING_EMAIL"

    with pytest.raises(UsLaceyPortalAuthError) as unverified:
        login_us_lacey_user(email=email, password=password)
    assert unverified.value.code == "email_unverified"

    verified = verify_us_lacey_email(registered.verification_token)
    assert verified.organization_id == registered.organization_id
    assert verified.account_status == "PAYMENT_PENDING"

    logged_in = login_us_lacey_user(email=email, password=password)
    assert logged_in.identity.organization_id == registered.organization_id
    assert logged_in.identity.account_status == "PAYMENT_PENDING"
    assert len(logged_in.session_token) >= 48

    resolved = resolve_us_lacey_session(logged_in.session_token)
    assert resolved.organization_id == registered.organization_id
    assert resolved.email == email

    # conftest deliberately strips MIGRATION_DATABASE_URL from the pytest process.
    # The isolated PostgreSQL gate exposes its owner URL through the explicit test
    # variable so this inspection cannot accidentally bind to a production owner.
    migration_engine = create_engine(
        os.environ["TEST_POSTGRES_MIGRATION_DATABASE_URL"],
        pool_pre_ping=True,
        hide_parameters=True,
    )
    with migration_engine.begin() as connection:
        connection.execute(
            text("SELECT set_config('app.current_organization_id', :org_id, true)"),
            {"org_id": str(registered.organization_id)},
        )
        persisted_hash = connection.execute(
            text(
                """
                SELECT token_hash
                FROM public.user_sessions
                WHERE organization_id = :organization_id
                  AND user_id = :user_id
                ORDER BY id DESC
                LIMIT 1
                """
            ),
            {
                "organization_id": registered.organization_id,
                "user_id": registered.user_id,
            },
        ).scalar_one()
    migration_engine.dispose()

    assert persisted_hash == hashlib.sha256(
        logged_in.session_token.encode("utf-8")
    ).hexdigest()
    assert persisted_hash != logged_in.session_token

    billing = get_us_lacey_billing_summary(
        organization_id=registered.organization_id
    )
    assert billing.payment_reference == registered.payment_reference
    assert billing.payment_status == "PENDING"
    assert billing.subscription_status == "PENDING"

    logout_us_lacey_user(logged_in.session_token)
    with pytest.raises(UsLaceyPortalAuthError) as logged_out:
        resolve_us_lacey_session(logged_in.session_token)
    assert logged_out.value.code == "session_invalid"
