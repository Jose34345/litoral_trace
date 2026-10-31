from __future__ import annotations

from datetime import datetime, timezone
import os
import secrets
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from litoral_trace.auth.passwords import hash_password
from litoral_trace.us_lacey.db import reset_us_lacey_engine_state
from litoral_trace.us_lacey.portal_auth import resolve_us_lacey_session
from litoral_trace.us_lacey.sandbox import provision_us_lacey_sandbox


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_DATABASE_URL")
    or not os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL"),
    reason="requires isolated U.S. runtime and migration PostgreSQL roles",
)


def _engine(url: str):
    return create_engine(url, pool_pre_ping=True, hide_parameters=True)


def test_runtime_can_provision_sandbox_only_through_definer_and_rls_stays_tenant_scoped():
    reset_us_lacey_engine_state()

    created = provision_us_lacey_sandbox(
        client_ip=f"198.51.100.{secrets.randbelow(200) + 1}",
        user_agent="sandbox-postgres-gate",
    )
    assert created.organization_id > 0
    assert created.user_id > 0
    assert created.session_id > 0

    now = datetime.now(timezone.utc)
    ttl_seconds = (created.expires_at - now).total_seconds()
    assert 3 * 60 * 60 < ttl_seconds <= 4 * 60 * 60 + 30

    resolved = resolve_us_lacey_session(created.session_token)
    assert resolved.organization_id == created.organization_id
    assert resolved.account_status == "PILOT"

    migrator = _engine(os.environ["TEST_POSTGRES_MIGRATION_DATABASE_URL"])
    try:
        with migrator.begin() as connection:
            connection.execute(
                text(
                    "SELECT set_config("
                    "'app.current_organization_id', :org_id, true)"
                ),
                {"org_id": str(created.organization_id)},
            )
            provenance = connection.execute(
                text(
                    """
                    SELECT
                        created_as_sandbox,
                        sandbox_started_at,
                        sandbox_converted_at
                    FROM public.organizations
                    WHERE id = :organization_id
                    """
                ),
                {"organization_id": created.organization_id},
            ).mappings().one()
        assert provenance["created_as_sandbox"] is True
        assert provenance["sandbox_started_at"] is not None
        assert provenance["sandbox_converted_at"] is None
    finally:
        migrator.dispose()

    runtime = _engine(os.environ["US_LACEY_DATABASE_URL"])
    try:
        # The runtime principal deliberately cannot inspect organizations
        # globally. Verify the sandbox only through tenant-scoped tables after
        # installing the canonical tenant GUC.
        with runtime.begin() as connection:
            connection.execute(
                text(
                    "SELECT set_config("
                    "'app.current_organization_id', :org_id, true)"
                ),
                {"org_id": str(created.organization_id)},
            )
            row = connection.execute(
                text(
                    """
                    SELECT
                        plan_code,
                        price_cents,
                        monthly_operation_limit,
                        used_operations,
                        status,
                        renews_at
                    FROM public.us_lacey_subscriptions
                    WHERE organization_id = :org_id
                    """
                ),
                {"org_id": created.organization_id},
            ).mappings().one()

        assert row["plan_code"] == "SANDBOX"
        assert row["price_cents"] == 0
        assert row["monthly_operation_limit"] == 1
        assert row["used_operations"] == 0
        assert row["status"] == "ACTIVE"
        assert row["renews_at"] is not None

        # A different tenant context cannot read the sandbox row through FORCE RLS.
        with runtime.begin() as connection:
            connection.execute(
                text(
                    "SELECT set_config("
                    "'app.current_organization_id', :org_id, true)"
                ),
                {"org_id": str(created.organization_id + 1000000)},
            )
            hidden_count = connection.execute(
                text(
                    "SELECT count(*) FROM public.us_lacey_subscriptions "
                    "WHERE organization_id = :target_org"
                ),
                {"target_org": created.organization_id},
            ).scalar_one()
        assert hidden_count == 0

        # The runtime role must not possess direct cross-tenant insertion power.
        with runtime.begin() as connection:
            connection.execute(
                text(
                    "SELECT set_config("
                    "'app.current_organization_id', :org_id, true)"
                ),
                {"org_id": str(created.organization_id)},
            )
            with pytest.raises(Exception):
                connection.execute(
                    text(
                        """
                        INSERT INTO public.us_lacey_subscriptions (
                            public_id,
                            organization_id,
                            plan_code,
                            currency,
                            price_cents,
                            monthly_operation_limit,
                            used_operations,
                            status
                        ) VALUES (
                            gen_random_uuid(),
                            :other_org,
                            'SANDBOX',
                            'USD',
                            0,
                            1,
                            0,
                            'ACTIVE'
                        )
                        """
                    ),
                    {"other_org": created.organization_id + 1000000},
                )
    finally:
        runtime.dispose()


def test_non_sandbox_zero_price_subscription_remains_rejected():
    reset_us_lacey_engine_state()
    created = provision_us_lacey_sandbox(
        client_ip=f"203.0.113.{secrets.randbelow(200) + 1}",
        user_agent="sandbox-price-constraint-gate",
    )
    runtime = _engine(os.environ["US_LACEY_DATABASE_URL"])
    try:
        with pytest.raises(Exception):
            with runtime.begin() as connection:
                connection.execute(
                    text(
                        "SELECT set_config("
                        "'app.current_organization_id', :org_id, true)"
                    ),
                    {"org_id": str(created.organization_id)},
                )
                # Converting a zero-price sandbox subscription into a commercial
                # plan must fail the global paid-plan constraint.
                connection.execute(
                    text(
                        """
                        UPDATE public.us_lacey_subscriptions
                        SET plan_code = 'PRIVATE_BETA'
                        WHERE organization_id = :org_id
                        """
                    ),
                    {"org_id": created.organization_id},
                )
    finally:
        runtime.dispose()
