"""Seal Supabase Data API exposure for internal public-schema objects.

Revision ID: 068_supabase_public_api_hardening
Revises: 067_us_lacey_outreach_audit_hotfix
"""
from __future__ import annotations

from alembic import op


revision = "068_supabase_public_api_hardening"
down_revision = "067_us_lacey_outreach_audit_hotfix"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"

RUNTIME_DEBUG_FUNCTIONS = (
    "public.us_lacey_sandbox_set_debug_consent(text,integer,boolean)",
    "public.us_lacey_sandbox_debug_policy(integer)",
)
WORKER_PURGE_FUNCTIONS = (
    "public.us_lacey_sandbox_purge_manifest(bigint,text)",
    "public.us_lacey_sandbox_purge_database(bigint,text,jsonb)",
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


def _enable_rls_and_revoke_data_api() -> None:
    op.execute(
        """
        DO $$
        DECLARE obj record;
        BEGIN
            FOR obj IN
                SELECT n.nspname, c.relname, c.relkind
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pg_depend d
                      WHERE d.classid = 'pg_class'::regclass
                        AND d.objid = c.oid
                        AND d.deptype = 'e'
                  )
            LOOP
                EXECUTE format(
                    'REVOKE ALL PRIVILEGES ON TABLE %I.%I FROM PUBLIC, anon, authenticated',
                    obj.nspname,
                    obj.relname
                );
                IF obj.relkind IN ('r', 'p') THEN
                    EXECUTE format(
                        'ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY',
                        obj.nspname,
                        obj.relname
                    );
                END IF;
            END LOOP;

            FOR obj IN
                SELECT n.nspname, c.relname
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relkind = 'S'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pg_depend d
                      WHERE d.classid = 'pg_class'::regclass
                        AND d.objid = c.oid
                        AND d.deptype = 'e'
                  )
            LOOP
                EXECUTE format(
                    'REVOKE ALL PRIVILEGES ON SEQUENCE %I.%I FROM PUBLIC, anon, authenticated',
                    obj.nspname,
                    obj.relname
                );
            END LOOP;
        END
        $$;
        """
    )


def _revoke_security_definers_owned_by_current_role() -> None:
    op.execute(
        """
        DO $$
        DECLARE fn record;
        BEGIN
            FOR fn IN
                SELECT p.oid::regprocedure AS signature
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = 'public'
                  AND p.prosecdef
                  AND p.proowner = (SELECT oid FROM pg_roles WHERE rolname = current_user)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pg_depend d
                      WHERE d.classid = 'pg_proc'::regclass
                        AND d.objid = p.oid
                        AND d.deptype = 'e'
                  )
            LOOP
                EXECUTE format(
                    'REVOKE EXECUTE ON FUNCTION %s FROM PUBLIC, anon, authenticated',
                    fn.signature
                );
            END LOOP;
        END
        $$;
        """
    )


def _harden_security_definers() -> None:
    # Functions owned by the migration principal can be hardened directly.
    _revoke_security_definers_owned_by_current_role()

    # Platform capabilities intentionally keep their non-login owner. Temporarily
    # SET that role so ACL changes are made by the actual function owner.
    _grant_temp_platform_set()
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")
    _revoke_security_definers_owned_by_current_role()

    for signature in RUNTIME_DEBUG_FUNCTIONS:
        op.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {RUNTIME_ROLE}")
    for signature in WORKER_PURGE_FUNCTIONS:
        op.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {WORKER_ROLE}")

    op.execute("RESET ROLE")
    _revoke_temp_platform_set()


def _create_policy(
    table: str,
    suffix: str,
    command: str,
    role: str,
    *,
    using: bool = False,
    check: bool = False,
) -> None:
    name = f"{table}_{suffix}_068"
    op.execute(f"DROP POLICY IF EXISTS {name} ON public.{table}")
    clauses = [f"CREATE POLICY {name} ON public.{table} FOR {command} TO {role}"]
    if using:
        clauses.append("USING (true)")
    if check:
        clauses.append("WITH CHECK (true)")
    op.execute(" ".join(clauses))


def _install_internal_policies() -> None:
    _create_policy(
        "alembic_version", "runtime_select", "SELECT", RUNTIME_ROLE, using=True
    )

    for command, using, check in (
        ("SELECT", True, False),
        ("INSERT", False, True),
        ("UPDATE", True, True),
        ("DELETE", True, False),
    ):
        _create_policy(
            "us_lacey_email_verifications",
            f"platform_{command.lower()}",
            command,
            PLATFORM_ROLE,
            using=using,
            check=check,
        )

    for table in (
        "us_lacey_outreach_links",
        "us_lacey_outreach_sessions",
        "us_lacey_outreach_events",
    ):
        _create_policy(table, "platform_select", "SELECT", PLATFORM_ROLE, using=True)
        _create_policy(table, "platform_insert", "INSERT", PLATFORM_ROLE, check=True)
        _create_policy(
            table,
            "platform_update",
            "UPDATE",
            PLATFORM_ROLE,
            using=True,
            check=True,
        )

    _create_policy(
        "us_lacey_pilot_quality_snapshots",
        "platform_select",
        "SELECT",
        PLATFORM_ROLE,
        using=True,
    )
    _create_policy(
        "us_lacey_pilot_quality_snapshots",
        "worker_select",
        "SELECT",
        WORKER_ROLE,
        using=True,
    )
    _create_policy(
        "us_lacey_pilot_quality_snapshots",
        "worker_insert",
        "INSERT",
        WORKER_ROLE,
        check=True,
    )

    _create_policy(
        "us_lacey_pilot_incidents",
        "platform_select",
        "SELECT",
        PLATFORM_ROLE,
        using=True,
    )
    _create_policy(
        "us_lacey_pilot_incidents",
        "worker_select",
        "SELECT",
        WORKER_ROLE,
        using=True,
    )
    _create_policy(
        "us_lacey_pilot_incidents",
        "worker_insert",
        "INSERT",
        WORKER_ROLE,
        check=True,
    )
    _create_policy(
        "us_lacey_pilot_incidents",
        "worker_update",
        "UPDATE",
        WORKER_ROLE,
        using=True,
        check=True,
    )

    _create_policy(
        "us_lacey_telemetry_runs",
        "platform_select",
        "SELECT",
        PLATFORM_ROLE,
        using=True,
    )
    _create_policy(
        "us_lacey_telemetry_runs",
        "worker_select",
        "SELECT",
        WORKER_ROLE,
        using=True,
    )
    _create_policy(
        "us_lacey_telemetry_runs",
        "worker_insert",
        "INSERT",
        WORKER_ROLE,
        check=True,
    )
    _create_policy(
        "us_lacey_telemetry_runs",
        "worker_update",
        "UPDATE",
        WORKER_ROLE,
        using=True,
        check=True,
    )

    _create_policy(
        "us_lacey_telemetry_field_actions",
        "worker_select",
        "SELECT",
        WORKER_ROLE,
        using=True,
    )
    _create_policy(
        "us_lacey_telemetry_field_actions",
        "worker_insert",
        "INSERT",
        WORKER_ROLE,
        check=True,
    )
    _create_policy(
        "us_lacey_telemetry_field_actions",
        "worker_update",
        "UPDATE",
        WORKER_ROLE,
        using=True,
        check=True,
    )


def _harden_default_privileges() -> None:
    op.execute(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        "REVOKE ALL PRIVILEGES ON TABLES FROM anon, authenticated"
    )
    op.execute(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        "REVOKE ALL PRIVILEGES ON SEQUENCES FROM anon, authenticated"
    )
    op.execute(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        "REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC, anon, authenticated"
    )

    _grant_temp_platform_set()
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")
    op.execute(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        "REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC, anon, authenticated"
    )
    op.execute("RESET ROLE")
    _revoke_temp_platform_set()


def upgrade() -> None:
    _enable_rls_and_revoke_data_api()
    _install_internal_policies()
    _harden_security_definers()
    _harden_default_privileges()


def downgrade() -> None:
    # Deliberately fail secure: rollback may remove the RLS policies introduced
    # by this revision, but it never restores Data API or PUBLIC grants.
    policy_specs = (
        ("alembic_version", "runtime_select"),
        ("us_lacey_email_verifications", "platform_select"),
        ("us_lacey_email_verifications", "platform_insert"),
        ("us_lacey_email_verifications", "platform_update"),
        ("us_lacey_email_verifications", "platform_delete"),
        ("us_lacey_outreach_links", "platform_select"),
        ("us_lacey_outreach_links", "platform_insert"),
        ("us_lacey_outreach_links", "platform_update"),
        ("us_lacey_outreach_sessions", "platform_select"),
        ("us_lacey_outreach_sessions", "platform_insert"),
        ("us_lacey_outreach_sessions", "platform_update"),
        ("us_lacey_outreach_events", "platform_select"),
        ("us_lacey_outreach_events", "platform_insert"),
        ("us_lacey_outreach_events", "platform_update"),
        ("us_lacey_pilot_quality_snapshots", "platform_select"),
        ("us_lacey_pilot_quality_snapshots", "worker_select"),
        ("us_lacey_pilot_quality_snapshots", "worker_insert"),
        ("us_lacey_pilot_incidents", "platform_select"),
        ("us_lacey_pilot_incidents", "worker_select"),
        ("us_lacey_pilot_incidents", "worker_insert"),
        ("us_lacey_pilot_incidents", "worker_update"),
        ("us_lacey_telemetry_runs", "platform_select"),
        ("us_lacey_telemetry_runs", "worker_select"),
        ("us_lacey_telemetry_runs", "worker_insert"),
        ("us_lacey_telemetry_runs", "worker_update"),
        ("us_lacey_telemetry_field_actions", "worker_select"),
        ("us_lacey_telemetry_field_actions", "worker_insert"),
        ("us_lacey_telemetry_field_actions", "worker_update"),
    )
    for table, suffix in policy_specs:
        op.execute(
            f"DROP POLICY IF EXISTS {table}_{suffix}_068 ON public.{table}"
        )

    for table in (
        "alembic_version",
        "us_lacey_email_verifications",
        "us_lacey_outreach_links",
        "us_lacey_outreach_sessions",
        "us_lacey_outreach_events",
        "us_lacey_pilot_quality_snapshots",
        "us_lacey_pilot_incidents",
        "us_lacey_telemetry_runs",
        "us_lacey_telemetry_field_actions",
    ):
        op.execute(f"ALTER TABLE public.{table} DISABLE ROW LEVEL SECURITY")
