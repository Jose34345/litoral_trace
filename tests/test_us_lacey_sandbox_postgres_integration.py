from __future__ import annotations

from datetime import datetime, timezone
import os

import pytest
from sqlalchemy import create_engine, text

from litoral_trace.us_lacey.db import reset_us_lacey_engine_state
from litoral_trace.us_lacey.portal_auth import (
    UsLaceyPortalAuthError,
    login_us_lacey_user,
    resolve_us_lacey_session,
    start_us_lacey_sandbox_session,
)


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_DATABASE_URL")
    or not os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL"),
    reason="requires the isolated U.S. PostgreSQL runtime and migration databases",
)


def test_sandbox_provisioning_is_ephemeral_rls_tenant_with_opaque_session(monkeypatch):
    monkeypatch.setenv("US_LACEY_SANDBOX_TTL_MINUTES", "60")
    reset_us_lacey_engine_state()

    sandbox = start_us_lacey_sandbox_session(
        client_ip="203.0.113.10",
        user_agent="pytest-sandbox",
    )
    assert sandbox.identity.account_status == "PILOT"
    assert sandbox.identity.email.endswith("@sandbox.invalid")
    assert sandbox.expires_at > datetime.now(timezone.utc)

    resolved = resolve_us_lacey_session(sandbox.session_token)
    assert resolved.organization_id == sandbox.identity.organization_id
    assert resolved.user_id == sandbox.identity.user_id

    # Synthetic principals must not become a second password-login surface.
    with pytest.raises(UsLaceyPortalAuthError) as login_error:
        login_us_lacey_user(
            email=resolved.email,
            password="not-a-real-sandbox-password",
        )
    assert login_error.value.code == "invalid_credentials"

    owner_engine = create_engine(
        os.environ["TEST_POSTGRES_MIGRATION_DATABASE_URL"],
        pool_pre_ping=True,
        hide_parameters=True,
    )
    try:
        with owner_engine.begin() as connection:
            org_id = sandbox.identity.organization_id
            connection.execute(
                text(
                    "SELECT set_config("
                    "'app.current_organization_id', :org_id, true)"
                ),
                {"org_id": str(org_id)},
            )
            organization = connection.execute(
                text(
                    """
                    SELECT is_sandbox, expires_at, tier
                    FROM public.organizations
                    WHERE id = :org_id
                    """
                ),
                {"org_id": org_id},
            ).mappings().one()
            subscription = connection.execute(
                text(
                    """
                    SELECT plan_code, price_cents, monthly_operation_limit,
                           used_operations, status
                    FROM public.us_lacey_subscriptions
                    WHERE organization_id = :org_id
                    """
                ),
                {"org_id": org_id},
            ).mappings().one()

            assert organization["is_sandbox"] is True
            assert organization["expires_at"] is not None
            assert organization["tier"] == "sandbox"
            assert subscription["plan_code"] == "SANDBOX"
            assert subscription["price_cents"] == 0
            assert subscription["monthly_operation_limit"] == 1
            assert subscription["used_operations"] == 0
            assert subscription["status"] == "ACTIVE"

            # Expiry at the organization boundary invalidates the opaque session
            # even if the user_session row itself has not been deleted yet.
            connection.execute(
                text(
                    """
                    UPDATE public.organizations
                    SET expires_at = now() - interval '1 minute'
                    WHERE id = :org_id
                    """
                ),
                {"org_id": org_id},
            )
    finally:
        owner_engine.dispose()

    with pytest.raises(UsLaceyPortalAuthError) as expired:
        resolve_us_lacey_session(sandbox.session_token)
    assert expired.value.code == "session_invalid"
