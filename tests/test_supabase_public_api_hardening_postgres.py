from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_POSTGRES_TEST_DATABASE_URL"),
    reason="requires isolated U.S. PostgreSQL root credentials",
)


def _root_engine():
    return create_engine(
        os.environ["US_LACEY_POSTGRES_TEST_DATABASE_URL"],
        pool_pre_ping=True,
        hide_parameters=True,
    )


def test_supabase_data_api_roles_cannot_reach_litoral_trace_relations() -> None:
    engine = _root_engine()
    with engine.connect() as connection:
        leaked = connection.execute(
            text(
                """
                SELECT c.relname
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relkind IN ('r', 'p')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pg_depend d
                      WHERE d.classid = 'pg_class'::regclass
                        AND d.objid = c.oid
                        AND d.deptype = 'e'
                  )
                  AND (
                      has_table_privilege('anon', c.oid, 'SELECT')
                      OR has_table_privilege('anon', c.oid, 'INSERT')
                      OR has_table_privilege('anon', c.oid, 'UPDATE')
                      OR has_table_privilege('anon', c.oid, 'DELETE')
                      OR has_table_privilege('authenticated', c.oid, 'SELECT')
                      OR has_table_privilege('authenticated', c.oid, 'INSERT')
                      OR has_table_privilege('authenticated', c.oid, 'UPDATE')
                      OR has_table_privilege('authenticated', c.oid, 'DELETE')
                  )
                ORDER BY c.relname
                """
            )
        ).scalars().all()
    engine.dispose()

    assert leaked == []


def test_all_litoral_trace_public_tables_have_rls_enabled() -> None:
    engine = _root_engine()
    with engine.connect() as connection:
        unprotected = connection.execute(
            text(
                """
                SELECT c.relname
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relkind IN ('r', 'p')
                  AND NOT c.relrowsecurity
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pg_depend d
                      WHERE d.classid = 'pg_class'::regclass
                        AND d.objid = c.oid
                        AND d.deptype = 'e'
                  )
                ORDER BY c.relname
                """
            )
        ).scalars().all()
    engine.dispose()

    assert unprotected == []


def test_non_extension_security_definer_functions_are_not_data_api_callable() -> None:
    engine = _root_engine()
    with engine.connect() as connection:
        leaked = connection.execute(
            text(
                """
                SELECT p.oid::regprocedure::text
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = 'public'
                  AND p.prosecdef
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pg_depend d
                      WHERE d.classid = 'pg_proc'::regclass
                        AND d.objid = p.oid
                        AND d.deptype = 'e'
                  )
                  AND (
                      has_function_privilege('anon', p.oid, 'EXECUTE')
                      OR has_function_privilege('authenticated', p.oid, 'EXECUTE')
                      OR has_function_privilege('public', p.oid, 'EXECUTE')
                  )
                ORDER BY p.oid::regprocedure::text
                """
            )
        ).scalars().all()
    engine.dispose()

    assert leaked == []


def test_internal_capabilities_remain_explicit_after_public_api_hardening() -> None:
    engine = _root_engine()
    with engine.connect() as connection:
        privileges = connection.execute(
            text(
                """
                SELECT
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.alembic_version',
                        'SELECT'
                    ) AS runtime_schema_read,
                    has_function_privilege(
                        'litoral_trace_app',
                        'public.us_lacey_sandbox_set_debug_consent(text,integer,boolean)',
                        'EXECUTE'
                    ) AS runtime_debug_consent,
                    has_function_privilege(
                        'litoral_trace_app',
                        'public.us_lacey_sandbox_debug_policy(integer)',
                        'EXECUTE'
                    ) AS runtime_debug_policy,
                    has_function_privilege(
                        'litoral_trace_worker_executor',
                        'public.us_lacey_sandbox_purge_manifest(bigint,text)',
                        'EXECUTE'
                    ) AS worker_purge_manifest,
                    has_function_privilege(
                        'litoral_trace_worker_executor',
                        'public.us_lacey_sandbox_purge_database(bigint,text,jsonb)',
                        'EXECUTE'
                    ) AS worker_purge_database,
                    has_table_privilege(
                        'litoral_trace_worker_executor',
                        'public.us_lacey_pilot_quality_snapshots',
                        'INSERT'
                    ) AS worker_snapshot_insert,
                    has_table_privilege(
                        'litoral_trace_worker_executor',
                        'public.us_lacey_pilot_incidents',
                        'UPDATE'
                    ) AS worker_incident_update,
                    has_table_privilege(
                        'litoral_trace_platform_definer',
                        'public.us_lacey_outreach_links',
                        'SELECT'
                    ) AS platform_outreach_select
                """
            )
        ).mappings().one()

        policy_roles = connection.execute(
            text(
                """
                SELECT tablename, cmd, roles
                FROM pg_policies
                WHERE schemaname = 'public'
                  AND tablename IN (
                      'alembic_version',
                      'us_lacey_email_verifications',
                      'us_lacey_outreach_links',
                      'us_lacey_outreach_sessions',
                      'us_lacey_outreach_events',
                      'us_lacey_pilot_quality_snapshots',
                      'us_lacey_pilot_incidents',
                      'us_lacey_telemetry_runs',
                      'us_lacey_telemetry_field_actions'
                  )
                """
            )
        ).mappings().all()

    engine.dispose()

    assert dict(privileges) == {
        "runtime_schema_read": True,
        "runtime_debug_consent": True,
        "runtime_debug_policy": True,
        "worker_purge_manifest": True,
        "worker_purge_database": True,
        "worker_snapshot_insert": True,
        "worker_incident_update": True,
        "platform_outreach_select": True,
    }

    forbidden_roles = {"anon", "authenticated", "public"}
    for row in policy_roles:
        assert forbidden_roles.isdisjoint({role.lower() for role in row["roles"]})


def test_future_migration_objects_do_not_auto_grant_data_api_roles() -> None:
    engine = _root_engine()
    with engine.connect() as connection:
        default_acl = connection.execute(
            text(
                """
                SELECT
                    pg_get_userbyid(d.defaclrole) AS owner_role,
                    d.defaclobjtype,
                    COALESCE(grantee.rolname, 'PUBLIC') AS grantee,
                    a.privilege_type
                FROM pg_default_acl d
                CROSS JOIN LATERAL aclexplode(d.defaclacl) a
                LEFT JOIN pg_roles grantee ON grantee.oid = a.grantee
                LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace
                WHERE COALESCE(n.nspname, 'public') = 'public'
                  AND pg_get_userbyid(d.defaclrole) IN (
                      current_user,
                      'litoral_trace_platform_definer'
                  )
                  AND COALESCE(grantee.rolname, 'PUBLIC') IN (
                      'anon', 'authenticated', 'PUBLIC'
                  )
                ORDER BY owner_role, d.defaclobjtype, grantee, a.privilege_type
                """
            )
        ).mappings().all()
    engine.dispose()

    assert default_acl == []
