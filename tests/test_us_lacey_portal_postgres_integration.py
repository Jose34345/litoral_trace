from __future__ import annotations

import hashlib
import os
from uuid import uuid4

import pytest
from sqlalchemy import select

from litoral_trace.db.models import UserSession
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.commercial import UsLaceyCommercialConfig
from litoral_trace.us_lacey.db import get_us_lacey_db_session, reset_us_lacey_engine_state
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
    or not os.environ.get("US_LACEY_DATABASE_URL"),
    reason="requires the isolated U.S. PostgreSQL integration database",
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

    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, registered.organization_id)
        persisted = session.execute(
            select(UserSession).where(
                UserSession.organization_id == registered.organization_id,
                UserSession.user_id == registered.user_id,
            ).order_by(UserSession.id.desc())
        ).scalars().first()
        assert persisted is not None
        assert persisted.token_hash == hashlib.sha256(
            logged_in.session_token.encode("utf-8")
        ).hexdigest()
        assert persisted.token_hash != logged_in.session_token
    finally:
        session.rollback()
        session.close()

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
