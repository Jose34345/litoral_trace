from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from litoral_trace.config.settings import normalize_database_url


PLATFORM_ROLE = "litoral_trace_platform_definer"
POSTGRES_TESTS_ENABLED = (os.environ.get("ENABLE_POSTGRES_TESTS") or "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
MIGRATION_DATABASE_URL = (
    os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")
    or os.environ.get("MIGRATION_DATABASE_URL")
)

pytestmark = pytest.mark.skipif(
    not (POSTGRES_TESTS_ENABLED and MIGRATION_DATABASE_URL),
    reason="P1.7-K platform definer integration requires PostgreSQL owner URL.",
)


def _owner_engine():
    return create_engine(
        normalize_database_url(MIGRATION_DATABASE_URL),
        pool_pre_ping=True,
    )


def test_platform_definer_is_non_login_non_superuser_non_bypass() -> None:
    engine = _owner_engine()
    try:
        with engine.connect() as conn:
            role = conn.execute(
                text(
                    """
                    SELECT
                        rolname,
                        rolcanlogin,
                        rolsuper,
                        rolcreatedb,
                        rolcreaterole,
                        rolinherit,
                        rolreplication,
                        rolbypassrls
                    FROM pg_catalog.pg_roles
                    WHERE rolname = :role_name
                    """
                ),
                {"role_name": PLATFORM_ROLE},
            ).mappings().one()

        assert role["rolname"] == PLATFORM_ROLE
        assert role["rolcanlogin"] is False
        assert role["rolsuper"] is False
        assert role["rolcreatedb"] is False
        assert role["rolcreaterole"] is False
        assert role["rolinherit"] is False
        assert role["rolreplication"] is False
        assert role["rolbypassrls"] is False
    finally:
        engine.dispose()


def test_platform_definer_keeps_usage_but_not_create_on_public_schema() -> None:
    engine = _owner_engine()
    try:
        with engine.connect() as conn:
            privileges = conn.execute(
                text(
                    """
                    SELECT
                        has_schema_privilege(:role_name, 'public', 'USAGE') AS has_usage,
                        has_schema_privilege(:role_name, 'public', 'CREATE') AS has_create
                    """
                ),
                {"role_name": PLATFORM_ROLE},
            ).mappings().one()

        assert privileges["has_usage"] is True
        assert privileges["has_create"] is False
    finally:
        engine.dispose()


def test_platform_functions_are_owned_by_dedicated_definer() -> None:
    expected = {
        "_platform_insert_audit_log",
        "_platform_superadmin_session_actor",
        "platform_create_organization",
        "platform_list_organizations",
        "platform_toggle_organization_status",
        "platform_upsert_license",
    }

    engine = _owner_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        p.proname,
                        pg_get_userbyid(p.proowner) AS owner_name,
                        p.prosecdef
                    FROM pg_catalog.pg_proc AS p
                    JOIN pg_catalog.pg_namespace AS n
                      ON n.oid = p.pronamespace
                    WHERE n.nspname = 'public'
                      AND p.proname = ANY(:function_names)
                    ORDER BY p.proname
                    """
                ),
                {"function_names": sorted(expected)},
            ).mappings().all()

        assert {row["proname"] for row in rows} == expected
        for row in rows:
            assert row["owner_name"] == PLATFORM_ROLE
            assert row["prosecdef"] is True
    finally:
        engine.dispose()


def test_platform_definer_has_explicit_force_rls_policies_only_for_required_commands() -> None:
    expected = {
        ("organizations", "organizations_platform_select", "SELECT"),
        ("organizations", "organizations_platform_insert", "INSERT"),
        ("organizations", "organizations_platform_update", "UPDATE"),
        ("users", "users_platform_select", "SELECT"),
        ("users", "users_platform_insert", "INSERT"),
        ("user_sessions", "user_sessions_platform_select", "SELECT"),
        ("user_sessions", "user_sessions_platform_update", "UPDATE"),
        ("licenses", "licenses_platform_select", "SELECT"),
        ("licenses", "licenses_platform_insert", "INSERT"),
        ("licenses", "licenses_platform_update", "UPDATE"),
        ("audit_logs", "audit_logs_platform_insert", "INSERT"),
    }

    engine = _owner_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        tablename,
                        policyname,
                        cmd,
                        roles
                    FROM pg_catalog.pg_policies
                    WHERE schemaname = 'public'
                      AND policyname LIKE '%_platform_%'
                    ORDER BY tablename, policyname
                    """
                )
            ).mappings().all()

            force_rls = conn.execute(
                text(
                    """
                    SELECT c.relname, c.relforcerowsecurity
                    FROM pg_catalog.pg_class AS c
                    JOIN pg_catalog.pg_namespace AS n
                      ON n.oid = c.relnamespace
                    WHERE n.nspname = 'public'
                      AND c.relname = ANY(:table_names)
                    ORDER BY c.relname
                    """
                ),
                {"table_names": sorted({item[0] for item in expected})},
            ).mappings().all()

        actual = {
            (row["tablename"], row["policyname"], row["cmd"])
            for row in rows
            if PLATFORM_ROLE in row["roles"]
        }
        assert actual == expected
        assert force_rls
        assert all(row["relforcerowsecurity"] is True for row in force_rls)
        assert not any(item[2] == "DELETE" for item in actual)
    finally:
        engine.dispose()


def test_platform_definer_membership_graph_keeps_only_safe_creator_admin_edge() -> None:
    engine = _owner_engine()
    try:
        with engine.connect() as conn:
            migration_role = conn.execute(text("SELECT current_user")).scalar_one()
            memberships = conn.execute(
                text(
                    """
                    SELECT
                        granted_role.rolname AS granted_role,
                        member_role.rolname AS member_role,
                        grantor_role.rolname AS grantor_role,
                        membership.admin_option,
                        membership.inherit_option,
                        membership.set_option
                    FROM pg_catalog.pg_auth_members AS membership
                    JOIN pg_catalog.pg_roles AS granted_role
                      ON granted_role.oid = membership.roleid
                    JOIN pg_catalog.pg_roles AS member_role
                      ON member_role.oid = membership.member
                    JOIN pg_catalog.pg_roles AS grantor_role
                      ON grantor_role.oid = membership.grantor
                    WHERE granted_role.rolname = :platform_role
                       OR member_role.rolname = :platform_role
                    ORDER BY granted_role.rolname, member_role.rolname, grantor_role.rolname
                    """
                ),
                {"platform_role": PLATFORM_ROLE},
            ).mappings().all()

        assert len(memberships) == 1
        membership = memberships[0]
        assert membership["granted_role"] == PLATFORM_ROLE
        assert membership["member_role"] == migration_role
        assert membership["grantor_role"] != migration_role
        assert membership["admin_option"] is True
        assert membership["inherit_option"] is False
        assert membership["set_option"] is False
    finally:
        engine.dispose()


def test_migration_role_cannot_assume_or_inherit_platform_definer_after_upgrade() -> None:
    engine = _owner_engine()
    try:
        with engine.connect() as conn:
            capabilities = conn.execute(
                text(
                    """
                    SELECT
                        pg_has_role(current_user, :role_name, 'MEMBER') AS is_member,
                        pg_has_role(current_user, :role_name, 'USAGE') AS can_inherit,
                        pg_has_role(current_user, :role_name, 'SET') AS can_set
                    """
                ),
                {"role_name": PLATFORM_ROLE},
            ).mappings().one()

            assert capabilities["is_member"] is True
            assert capabilities["can_inherit"] is False
            assert capabilities["can_set"] is False

            with pytest.raises(DBAPIError) as exc_info:
                conn.execute(text(f"SET ROLE {PLATFORM_ROLE}"))

            assert getattr(exc_info.value.orig, "sqlstate", None) == "42501"
    finally:
        engine.dispose()


def test_runtime_role_cannot_assume_platform_definer() -> None:
    engine = _owner_engine()
    try:
        with engine.connect() as conn:
            runtime_is_member = conn.execute(
                text(
                    "SELECT pg_has_role('litoral_trace_app', :role_name, 'MEMBER')"
                ),
                {"role_name": PLATFORM_ROLE},
            ).scalar_one()

        assert runtime_is_member is False
    finally:
        engine.dispose()
