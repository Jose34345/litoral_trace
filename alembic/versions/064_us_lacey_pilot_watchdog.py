"""Add least-privilege pilot reliability watchdog discovery functions.

Revision ID: 064_us_lacey_pilot_watchdog
Revises: 063_us_lacey_pilot_reliability
"""
from __future__ import annotations

from alembic import op


revision = "064_us_lacey_pilot_watchdog"
down_revision = "063_us_lacey_pilot_reliability"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"

ATTRIBUTION_FUNCTION = (
    "public.us_lacey_pilot_operation_attribution(integer,integer)"
)
WATCHDOG_FUNCTION = (
    "public.us_lacey_pilot_watchdog_candidates(timestamp with time zone)"
)


def _grant_temp_platform_set() -> None:
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )


def _revoke_temp_platform_set() -> None:
    op.execute(
        f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER"
    )


def upgrade() -> None:
    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(
        f"GRANT SELECT (id, organization_id, public_id, status, updated_at) "
        f"ON TABLE public.us_lacey_operations TO {PLATFORM_ROLE}"
    )
    op.execute(
        f"GRANT SELECT (id, sandbox_attribution_session_id) "
        f"ON TABLE public.organizations TO {PLATFORM_ROLE}"
    )
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_pilot_operation_attribution(
            requested_organization_id integer,
            requested_operation_id integer
        )
        RETURNS uuid
        LANGUAGE sql
        STABLE
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
            SELECT org.sandbox_attribution_session_id
            FROM public.us_lacey_operations AS operation
            JOIN public.organizations AS org
              ON org.id = operation.organization_id
            WHERE operation.id = requested_operation_id
              AND operation.organization_id = requested_organization_id
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_pilot_watchdog_candidates(
            requested_stale_before timestamp with time zone
        )
        RETURNS TABLE(
            organization_id integer,
            operation_id integer,
            operation_public_id uuid,
            attribution_session_id uuid,
            operation_updated_at timestamp with time zone
        )
        LANGUAGE sql
        STABLE
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
            SELECT
                operation.organization_id,
                operation.id,
                operation.public_id,
                org.sandbox_attribution_session_id,
                operation.updated_at
            FROM public.us_lacey_operations AS operation
            JOIN public.organizations AS org
              ON org.id = operation.organization_id
            WHERE org.sandbox_attribution_session_id IS NOT NULL
              AND operation.status = 'PROCESSING'
              AND operation.updated_at < requested_stale_before
            ORDER BY operation.updated_at ASC, operation.id ASC
        $$;
        """
    )

    for function_name in (ATTRIBUTION_FUNCTION, WATCHDOG_FUNCTION):
        op.execute(f"REVOKE ALL ON FUNCTION {function_name} FROM PUBLIC")
        op.execute(f"REVOKE ALL ON FUNCTION {function_name} FROM {RUNTIME_ROLE}")
        op.execute(f"GRANT EXECUTE ON FUNCTION {function_name} TO {WORKER_ROLE}")

    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()


def downgrade() -> None:
    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")
    op.execute(f"DROP FUNCTION IF EXISTS {WATCHDOG_FUNCTION}")
    op.execute(f"DROP FUNCTION IF EXISTS {ATTRIBUTION_FUNCTION}")
    op.execute("RESET ROLE")
    op.execute(
        f"REVOKE SELECT (id, organization_id, public_id, status, updated_at) "
        f"ON TABLE public.us_lacey_operations FROM {PLATFORM_ROLE}"
    )
    op.execute(
        f"REVOKE SELECT (id, sandbox_attribution_session_id) "
        f"ON TABLE public.organizations FROM {PLATFORM_ROLE}"
    )
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()
