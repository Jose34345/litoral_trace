"""PostgreSQL acceptance for sandbox growth attribution (migration 056)."""
from __future__ import annotations

from datetime import timedelta
import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_TEST_AUDIT_DATABASE_URL")
    or not os.environ.get("US_LACEY_DATABASE_URL"),
    reason="requires the isolated PostgreSQL gate",
)


def _create_organization(
    connection,
    *,
    suffix: str,
    label: str,
    sandbox: bool,
) -> int:
    if sandbox:
        return int(
            connection.execute(
                text(
                    """
                    INSERT INTO public.organizations(
                        name,
                        slug,
                        tier,
                        description,
                        is_active,
                        is_sandbox,
                        sandbox_expires_at,
                        created_at,
                        updated_at
                    )
                    VALUES(
                        :name,
                        :slug,
                        'sandbox',
                        '056 growth attribution acceptance',
                        true,
                        true,
                        now() + interval '4 hours',
                        now(),
                        now()
                    )
                    RETURNING id
                    """
                ),
                {
                    "name": f"056 {label} {suffix}",
                    "slug": f"p056-{label}-{suffix}",
                },
            ).scalar_one()
        )

    return int(
        connection.execute(
            text(
                """
                INSERT INTO public.organizations(
                    name,
                    slug,
                    tier,
                    description,
                    is_active,
                    created_at,
                    updated_at
                )
                VALUES(
                    :name,
                    :slug,
                    'pro',
                    '056 platform actor',
                    true,
                    now(),
                    now()
                )
                RETURNING id
                """
            ),
            {
                "name": f"056 {label} {suffix}",
                "slug": f"p056-{label}-{suffix}",
            },
        ).scalar_one()
    )


def test_056_provenance_conversion_cohort_and_worker_race() -> None:
    audit = create_engine(
        os.environ["US_LACEY_TEST_AUDIT_DATABASE_URL"],
        pool_pre_ping=True,
        hide_parameters=True,
    )
    runtime = create_engine(
        os.environ["US_LACEY_DATABASE_URL"],
        pool_pre_ping=True,
        hide_parameters=True,
    )

    suffix = uuid4().hex[:10]
    actor_token_hash = "d" * 64

    try:
        with audit.begin() as connection:
            platform_org = _create_organization(
                connection,
                suffix=suffix,
                label="platform",
                sandbox=False,
            )
            actor_user_id = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.users(
                            organization_id,
                            email,
                            username,
                            password_hash,
                            role,
                            is_active,
                            created_at,
                            updated_at
                        )
                        VALUES(
                            :organization_id,
                            :email,
                            :username,
                            'unchanged-password-hash',
                            'superadmin',
                            true,
                            now(),
                            now()
                        )
                        RETURNING id
                        """
                    ),
                    {
                        "organization_id": platform_org,
                        "email": f"p056-actor-{suffix}@example.com",
                        "username": f"p056-actor-{suffix}",
                    },
                ).scalar_one()
            )
            connection.execute(
                text(
                    """
                    INSERT INTO public.user_sessions(
                        user_id,
                        organization_id,
                        family_id,
                        token_hash,
                        issued_at,
                        expires_at
                    )
                    VALUES(
                        :user_id,
                        :organization_id,
                        :family_id,
                        :token_hash,
                        now(),
                        now() + interval '1 hour'
                    )
                    """
                ),
                {
                    "user_id": actor_user_id,
                    "organization_id": platform_org,
                    "family_id": str(uuid4()),
                    "token_hash": actor_token_hash,
                },
            )

            converted_org = _create_organization(
                connection,
                suffix=suffix,
                label="converted",
                sandbox=True,
            )
            provenance = connection.execute(
                text(
                    """
                    SELECT
                        created_as_sandbox,
                        is_sandbox,
                        sandbox_started_at,
                        sandbox_converted_at
                    FROM public.organizations
                    WHERE id = :organization_id
                    """
                ),
                {"organization_id": converted_org},
            ).mappings().one()
            assert provenance["created_as_sandbox"] is True
            assert provenance["is_sandbox"] is True
            assert provenance["sandbox_started_at"] is not None
            assert provenance["sandbox_converted_at"] is None

            pending_job = connection.execute(
                text(
                    """
                    SELECT id, state
                    FROM public.us_lacey_sandbox_purge_jobs
                    WHERE organization_id = :organization_id
                    """
                ),
                {"organization_id": converted_org},
            ).mappings().one()
            assert pending_job["state"] == "PENDING"

            connection.execute(
                text(
                    """
                    INSERT INTO public.us_lacey_subscriptions(
                        public_id,
                        organization_id,
                        plan_code,
                        currency,
                        price_cents,
                        monthly_operation_limit,
                        used_operations,
                        status,
                        started_at,
                        renews_at,
                        created_at,
                        updated_at
                    )
                    VALUES(
                        gen_random_uuid(),
                        :organization_id,
                        'PRIVATE_BETA',
                        'USD',
                        10000,
                        25,
                        0,
                        'ACTIVE',
                        now(),
                        now() + interval '1 month',
                        now(),
                        now()
                    )
                    """
                ),
                {"organization_id": converted_org},
            )

        with runtime.begin() as connection:
            converted = connection.execute(
                text(
                    """
                    SELECT *
                    FROM public.platform_admin_convert_sandbox_to_commercial(
                        :actor_refresh_token_hash,
                        :organization_id
                    )
                    """
                ),
                {
                    "actor_refresh_token_hash": actor_token_hash,
                    "organization_id": converted_org,
                },
            ).mappings().one()

        assert converted["organization_id"] == converted_org
        assert converted["converted_at"] is not None
        assert converted["purge_job_state"] == "CANCELED"

        with audit.connect() as connection:
            converted_state = connection.execute(
                text(
                    """
                    SELECT
                        created_as_sandbox,
                        is_sandbox,
                        sandbox_expires_at,
                        sandbox_started_at,
                        sandbox_converted_at
                    FROM public.organizations
                    WHERE id = :organization_id
                    """
                ),
                {"organization_id": converted_org},
            ).mappings().one()
            purge_state = connection.execute(
                text(
                    """
                    SELECT state
                    FROM public.us_lacey_sandbox_purge_jobs
                    WHERE organization_id = :organization_id
                    """
                ),
                {"organization_id": converted_org},
            ).scalar_one()
            audit_count = connection.execute(
                text(
                    """
                    SELECT count(*)
                    FROM public.us_lacey_admin_audit_logs
                    WHERE target_organization_id = :organization_id
                      AND action_type = 'SANDBOX_CONVERTED'
                    """
                ),
                {"organization_id": converted_org},
            ).scalar_one()

        assert converted_state["created_as_sandbox"] is True
        assert converted_state["is_sandbox"] is False
        assert converted_state["sandbox_expires_at"] is None
        assert converted_state["sandbox_started_at"] is not None
        assert converted_state["sandbox_converted_at"] is not None
        assert purge_state == "CANCELED"
        assert audit_count == 1

        cohort_from = converted_state["sandbox_started_at"] - timedelta(seconds=1)
        cohort_to = converted_state["sandbox_started_at"] + timedelta(seconds=1)
        with runtime.begin() as connection:
            cohort = connection.execute(
                text(
                    """
                    SELECT *
                    FROM public.platform_admin_sandbox_conversion_cohorts(
                        :actor_refresh_token_hash,
                        :requested_from,
                        :requested_to
                    )
                    """
                ),
                {
                    "actor_refresh_token_hash": actor_token_hash,
                    "requested_from": cohort_from,
                    "requested_to": cohort_to,
                },
            ).mappings().one()

        assert cohort["sandboxes_created"] == 1
        assert cohort["converted_to_commercial"] == 1
        assert float(cohort["conversion_rate_pct"]) == 100.0

        with audit.begin() as connection:
            worker_owned_org = _create_organization(
                connection,
                suffix=suffix,
                label="worker-owned",
                sandbox=True,
            )
            connection.execute(
                text(
                    """
                    UPDATE public.us_lacey_sandbox_purge_jobs
                    SET
                        state = 'STORAGE_DELETING',
                        attempt_count = attempt_count + 1,
                        locked_by = 'p056-simulated-worker',
                        locked_at = now(),
                        heartbeat_at = now(),
                        updated_at = now()
                    WHERE organization_id = :organization_id
                      AND state = 'PENDING'
                    """
                ),
                {"organization_id": worker_owned_org},
            )

        with pytest.raises(
            DBAPIError,
            match="sandbox purge has already started",
        ):
            with runtime.begin() as connection:
                connection.execute(
                    text(
                        """
                        SELECT *
                        FROM public.platform_admin_convert_sandbox_to_commercial(
                            :actor_refresh_token_hash,
                            :organization_id
                        )
                        """
                    ),
                    {
                        "actor_refresh_token_hash": actor_token_hash,
                        "organization_id": worker_owned_org,
                    },
                ).mappings().one()

        with audit.connect() as connection:
            worker_owned_state = connection.execute(
                text(
                    """
                    SELECT
                        org.is_sandbox,
                        org.sandbox_converted_at,
                        job.state
                    FROM public.organizations AS org
                    JOIN public.us_lacey_sandbox_purge_jobs AS job
                      ON job.organization_id = org.id
                    WHERE org.id = :organization_id
                    """
                ),
                {"organization_id": worker_owned_org},
            ).mappings().one()

            function_acl = connection.execute(
                text(
                    """
                    SELECT
                        p.proname,
                        p.prosecdef,
                        owner.rolname AS owner_role,
                        has_function_privilege(
                            'litoral_trace_app',
                            p.oid,
                            'EXECUTE'
                        ) AS runtime_execute,
                        has_function_privilege(
                            'litoral_trace_worker_executor',
                            p.oid,
                            'EXECUTE'
                        ) AS worker_execute,
                        has_function_privilege(
                            'public',
                            p.oid,
                            'EXECUTE'
                        ) AS public_execute
                    FROM pg_proc AS p
                    JOIN pg_namespace AS n
                      ON n.oid = p.pronamespace
                    JOIN pg_roles AS owner
                      ON owner.oid = p.proowner
                    WHERE n.nspname = 'public'
                      AND p.proname IN (
                          'platform_admin_convert_sandbox_to_commercial',
                          'platform_admin_sandbox_conversion_cohorts'
                      )
                    ORDER BY p.proname
                    """
                )
            ).mappings().all()

        assert dict(worker_owned_state) == {
            "is_sandbox": True,
            "sandbox_converted_at": None,
            "state": "STORAGE_DELETING",
        }
        assert len(function_acl) == 2
        for row in function_acl:
            assert row["prosecdef"] is True
            assert row["owner_role"] == "litoral_trace_platform_definer"
            assert row["runtime_execute"] is True
            assert row["worker_execute"] is False
            assert row["public_execute"] is False

    finally:
        runtime.dispose()
        audit.dispose()
