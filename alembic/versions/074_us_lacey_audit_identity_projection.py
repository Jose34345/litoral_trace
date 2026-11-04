"""Add tenant-scoped audit actor identity projection.

Revision ID: 074_us_lacey_audit_identity_projection
Revises: 073_us_lacey_operation_audit_trail
"""
from __future__ import annotations

from alembic import op


revision = "074_us_lacey_audit_identity_projection"
down_revision = "073_us_lacey_operation_audit_trail"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
FUNCTION = "public.us_lacey_audit_user_identity(integer)"


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
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_audit_user_identity(
            requested_user_id integer
        )
        RETURNS TABLE (
            user_id integer,
            display_identity text
        )
        LANGUAGE sql
        STABLE
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
            SELECT
                users.id,
                coalesce(
                    nullif(btrim(users.full_name), ''),
                    users.email
                )::text
            FROM public.users AS users
            WHERE users.id = requested_user_id
              AND users.organization_id = NULLIF(
                    current_setting(
                        'app.current_organization_id',
                        true
                    ),
                    ''
                  )::integer
            LIMIT 1
        $$;
        """
    )

    op.execute(f"REVOKE ALL ON FUNCTION {FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {FUNCTION} TO {RUNTIME_ROLE}")

    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()


def downgrade() -> None:
    _grant_temp_platform_set()
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")
    op.execute(f"DROP FUNCTION IF EXISTS {FUNCTION}")
    op.execute("RESET ROLE")
    _revoke_temp_platform_set()
