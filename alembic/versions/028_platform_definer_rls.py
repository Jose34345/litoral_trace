"""Isolate platform control-plane with a dedicated FORCE-RLS definer.

Revision ID: 028_platform_definer_rls
Revises: 027_fix_platform_rls_bootstrap
Create Date: 2026-08-23 19:15:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "028_platform_definer_rls"
down_revision: Union[str, Sequence[str], None] = "027_fix_platform_rls_bootstrap"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


PLATFORM_ROLE = "litoral_trace_platform_definer"
RUNTIME_ROLE = "litoral_trace_app"

PLATFORM_FUNCTIONS: tuple[str, ...] = (
    "public._platform_superadmin_session_actor(text)",
    "public._platform_insert_audit_log(integer, text, text, integer, integer, text, text, integer, jsonb)",
    "public.platform_list_organizations(text)",
    "public.platform_create_organization(text, text, text, text, text, text, text, text, text, text, text, integer, double precision, integer, timestamptz, boolean)",
    "public.platform_toggle_organization_status(text, integer)",
    "public.platform_upsert_license(text, integer, text, integer, double precision, integer, timestamptz, boolean)",
)

PUBLIC_PLATFORM_FUNCTIONS: tuple[str, ...] = (
    "public.platform_list_organizations(text)",
    "public.platform_create_organization(text, text, text, text, text, text, text, text, text, text, text, integer, double precision, integer, timestamptz, boolean)",
    "public.platform_toggle_organization_status(text, integer)",
    "public.platform_upsert_license(text, integer, text, integer, double precision, integer, timestamptz, boolean)",
)

INTERNAL_PLATFORM_FUNCTIONS: tuple[str, ...] = (
    "public._platform_superadmin_session_actor(text)",
    "public._platform_insert_audit_log(integer, text, text, integer, integer, text, text, integer, jsonb)",
)

PLATFORM_POLICIES: tuple[tuple[str, str, str, str], ...] = (
    ("organizations", "organizations_platform_select", "SELECT", "USING (true)"),
    ("organizations", "organizations_platform_insert", "INSERT", "WITH CHECK (true)"),
    ("organizations", "organizations_platform_update", "UPDATE", "USING (true) WITH CHECK (true)"),
    ("users", "users_platform_select", "SELECT", "USING (true)"),
    ("users", "users_platform_insert", "INSERT", "WITH CHECK (true)"),
    ("user_sessions", "user_sessions_platform_select", "SELECT", "USING (true)"),
    ("user_sessions", "user_sessions_platform_update", "UPDATE", "USING (true) WITH CHECK (true)"),
    ("licenses", "licenses_platform_select", "SELECT", "USING (true)"),
    ("licenses", "licenses_platform_insert", "INSERT", "WITH CHECK (true)"),
    ("licenses", "licenses_platform_update", "UPDATE", "USING (true) WITH CHECK (true)"),
    ("audit_logs", "audit_logs_platform_insert", "INSERT", "WITH CHECK (true)"),
)


def _ensure_platform_role() -> None:
    op.execute(
        f"""
        DO $$
        DECLARE
            migration_role name := current_user;
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_roles
                WHERE rolname = '{PLATFORM_ROLE}'
            ) THEN
                CREATE ROLE {PLATFORM_ROLE}
                    NOLOGIN
                    NOSUPERUSER
                    NOCREATEDB
                    NOCREATEROLE
                    NOINHERIT
                    NOREPLICATION
                    NOBYPASSRLS;
            END IF;

            EXECUTE format(
                'GRANT {PLATFORM_ROLE} TO %I',
                migration_role
            );
        END;
        $$;
        """
    )


def _grant_platform_capabilities() -> None:
    op.execute(f"GRANT USAGE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE ON TABLE public.organizations TO {PLATFORM_ROLE}"
    )
    op.execute(f"GRANT SELECT, INSERT ON TABLE public.users TO {PLATFORM_ROLE}")
    op.execute(
        f"GRANT SELECT, UPDATE ON TABLE public.user_sessions TO {PLATFORM_ROLE}"
    )
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE ON TABLE public.licenses TO {PLATFORM_ROLE}"
    )
    op.execute(f"GRANT INSERT ON TABLE public.audit_logs TO {PLATFORM_ROLE}")

    op.execute(
        f"""
        DO $$
        DECLARE
            sequence_name text;
            relation_name text;
        BEGIN
            FOREACH relation_name IN ARRAY ARRAY[
                'organizations',
                'users',
                'licenses',
                'audit_logs'
            ]
            LOOP
                SELECT pg_get_serial_sequence(
                    format('public.%I', relation_name),
                    'id'
                )
                INTO sequence_name;

                IF sequence_name IS NOT NULL THEN
                    EXECUTE format(
                        'GRANT USAGE, SELECT ON SEQUENCE %s TO {PLATFORM_ROLE}',
                        sequence_name
                    );
                END IF;
            END LOOP;
        END;
        $$;
        """
    )


def _create_platform_rls_policies() -> None:
    for table_name, policy_name, command, predicate in PLATFORM_POLICIES:
        op.execute(f"DROP POLICY IF EXISTS {policy_name} ON public.{table_name}")
        op.execute(
            f"CREATE POLICY {policy_name} "
            f"ON public.{table_name} "
            f"FOR {command} "
            f"TO {PLATFORM_ROLE} "
            f"{predicate}"
        )


def _restore_platform_actor_without_bootstrap_dependency() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public._platform_superadmin_session_actor(
            actor_refresh_token_hash text
        )
        RETURNS TABLE (
            actor_user_id integer,
            actor_session_id integer,
            actor_organization_id integer
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        BEGIN
            IF actor_refresh_token_hash IS NULL OR btrim(actor_refresh_token_hash) = '' THEN
                RAISE EXCEPTION 'invalid platform session'
                    USING ERRCODE = '28000';
            END IF;

            RETURN QUERY
            SELECT
                users.id,
                user_sessions.id,
                users.organization_id
            FROM public.user_sessions AS user_sessions
            JOIN public.users AS users
                ON users.id = user_sessions.user_id
               AND users.organization_id = user_sessions.organization_id
            JOIN public.organizations AS organizations
                ON organizations.id = users.organization_id
            WHERE user_sessions.token_hash = actor_refresh_token_hash
              AND user_sessions.revoked_at IS NULL
              AND user_sessions.expires_at > now()
              AND users.is_active
              AND organizations.is_active
              AND users.role = 'superadmin';

            IF NOT FOUND THEN
                IF EXISTS (
                    SELECT 1
                    FROM public.user_sessions AS user_sessions
                    JOIN public.users AS users
                        ON users.id = user_sessions.user_id
                       AND users.organization_id = user_sessions.organization_id
                    JOIN public.organizations AS organizations
                        ON organizations.id = users.organization_id
                    WHERE user_sessions.token_hash = actor_refresh_token_hash
                      AND user_sessions.revoked_at IS NULL
                      AND user_sessions.expires_at > now()
                      AND users.is_active
                      AND organizations.is_active
                ) THEN
                    RAISE EXCEPTION 'platform permission denied'
                        USING ERRCODE = '42501';
                END IF;

                RAISE EXCEPTION 'invalid platform session'
                    USING ERRCODE = '28000';
            END IF;
        END;
        $$;
        """
    )


def _transfer_platform_function_ownership() -> None:
    for function_signature in PLATFORM_FUNCTIONS:
        op.execute(
            f"ALTER FUNCTION {function_signature} OWNER TO {PLATFORM_ROLE}"
        )

    for function_signature in INTERNAL_PLATFORM_FUNCTIONS:
        op.execute(f"REVOKE ALL ON FUNCTION {function_signature} FROM PUBLIC")
        op.execute(
            f"REVOKE ALL ON FUNCTION {function_signature} FROM {RUNTIME_ROLE}"
        )

    for function_signature in PUBLIC_PLATFORM_FUNCTIONS:
        op.execute(f"REVOKE ALL ON FUNCTION {function_signature} FROM PUBLIC")
        op.execute(
            f"GRANT EXECUTE ON FUNCTION {function_signature} TO {RUNTIME_ROLE}"
        )


def _restore_027_actor() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public._platform_superadmin_session_actor(
            actor_refresh_token_hash text
        )
        RETURNS TABLE (
            actor_user_id integer,
            actor_session_id integer,
            actor_organization_id integer
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            bootstrap_organization_id integer;
        BEGIN
            IF actor_refresh_token_hash IS NULL OR btrim(actor_refresh_token_hash) = '' THEN
                RAISE EXCEPTION 'invalid platform session'
                    USING ERRCODE = '28000';
            END IF;

            SELECT bootstrap.organization_id
            INTO bootstrap_organization_id
            FROM public.bootstrap_auth_session_by_token_hash(
                actor_refresh_token_hash
            ) AS bootstrap
            LIMIT 1;

            IF bootstrap_organization_id IS NULL THEN
                RAISE EXCEPTION 'invalid platform session'
                    USING ERRCODE = '28000';
            END IF;

            PERFORM set_config(
                'app.current_organization_id',
                bootstrap_organization_id::text,
                true
            );

            RETURN QUERY
            SELECT
                users.id,
                user_sessions.id,
                users.organization_id
            FROM public.user_sessions AS user_sessions
            JOIN public.users AS users
                ON users.id = user_sessions.user_id
               AND users.organization_id = user_sessions.organization_id
            JOIN public.organizations AS organizations
                ON organizations.id = users.organization_id
            WHERE user_sessions.token_hash = actor_refresh_token_hash
              AND user_sessions.organization_id = bootstrap_organization_id
              AND user_sessions.revoked_at IS NULL
              AND user_sessions.expires_at > now()
              AND users.is_active
              AND organizations.is_active
              AND users.role = 'superadmin';

            IF NOT FOUND THEN
                IF EXISTS (
                    SELECT 1
                    FROM public.user_sessions AS user_sessions
                    JOIN public.users AS users
                        ON users.id = user_sessions.user_id
                       AND users.organization_id = user_sessions.organization_id
                    JOIN public.organizations AS organizations
                        ON organizations.id = users.organization_id
                    WHERE user_sessions.token_hash = actor_refresh_token_hash
                      AND user_sessions.organization_id = bootstrap_organization_id
                      AND user_sessions.revoked_at IS NULL
                      AND user_sessions.expires_at > now()
                      AND users.is_active
                      AND organizations.is_active
                ) THEN
                    RAISE EXCEPTION 'platform permission denied'
                        USING ERRCODE = '42501';
                END IF;

                RAISE EXCEPTION 'invalid platform session'
                    USING ERRCODE = '28000';
            END IF;
        END;
        $$;
        """
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public._platform_superadmin_session_actor(text) FROM PUBLIC"
    )


def _transfer_platform_functions_back_to_migration_role() -> None:
    signatures = "\n".join(
        f"EXECUTE format('ALTER FUNCTION {signature} OWNER TO %I', migration_role);"
        for signature in PLATFORM_FUNCTIONS
    )
    op.execute(
        f"""
        DO $$
        DECLARE
            migration_role name := current_user;
        BEGIN
            {signatures}
        END;
        $$;
        """
    )


def _drop_platform_rls_policies() -> None:
    for table_name, policy_name, _, _ in reversed(PLATFORM_POLICIES):
        op.execute(f"DROP POLICY IF EXISTS {policy_name} ON public.{table_name}")


def _revoke_platform_capabilities_and_drop_role() -> None:
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE public.organizations FROM {PLATFORM_ROLE}"
    )
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.users FROM {PLATFORM_ROLE}")
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE public.user_sessions FROM {PLATFORM_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE public.licenses FROM {PLATFORM_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE public.audit_logs FROM {PLATFORM_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM {PLATFORM_ROLE}"
    )
    op.execute(f"REVOKE USAGE ON SCHEMA public FROM {PLATFORM_ROLE}")
    op.execute(
        f"""
        DO $$
        DECLARE
            migration_role name := current_user;
        BEGIN
            EXECUTE format(
                'REVOKE {PLATFORM_ROLE} FROM %I',
                migration_role
            );
        END;
        $$;
        """
    )
    op.execute(f"DROP ROLE IF EXISTS {PLATFORM_ROLE}")


def upgrade() -> None:
    _ensure_platform_role()
    _grant_platform_capabilities()
    _create_platform_rls_policies()
    _restore_platform_actor_without_bootstrap_dependency()
    _transfer_platform_function_ownership()


def downgrade() -> None:
    _transfer_platform_functions_back_to_migration_role()
    _restore_027_actor()
    _drop_platform_rls_policies()
    _revoke_platform_capabilities_and_drop_role()
