from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "alembic" / "versions" / "028_platform_definer_rls.py"
NON_SUPERUSER_GATE = (
    ROOT / ".github" / "workflows" / "p17k-nonsuperuser-migration-gate.yml"
)


def test_preexisting_platform_role_is_hardened_before_capabilities() -> None:
    source = MIGRATION.read_text(encoding="utf-8")
    ensure_source = source.split("def _ensure_platform_role()", 1)[1].split(
        "def _grant_platform_capabilities()", 1
    )[0]
    existing_role_path = ensure_source.split("ELSE", 1)[1]
    alter_block = existing_role_path.split("ALTER ROLE {PLATFORM_ROLE}", 1)[1].split(
        ";", 1
    )[0]

    assert "ALTER ROLE {PLATFORM_ROLE}" in ensure_source
    assert "NOLOGIN" in alter_block
    assert "NOINHERIT" in alter_block
    assert "NOCREATEDB" not in alter_block
    assert "NOCREATEROLE" not in alter_block
    assert "rolsuper" in existing_role_path
    assert "rolcreatedb" in existing_role_path
    assert "rolcreaterole" in existing_role_path
    assert "rolreplication" in existing_role_path
    assert "rolbypassrls" in existing_role_path
    assert "pg_catalog.pg_stat_activity" in ensure_source
    assert "pg_catalog.pg_auth_members" in ensure_source
    assert "member_role.rolname <> migration_role" in ensure_source
    assert "pre-existing platform definer role has unsafe cluster privileges" in ensure_source
    assert "pre-existing platform definer role has active sessions" in ensure_source
    assert "platform definer role has unexpected inherited memberships" in ensure_source
    assert "platform definer role is assumable by an unexpected member" in ensure_source


def test_non_superuser_gate_exercises_preexisting_login_role_path() -> None:
    workflow = NON_SUPERUSER_GATE.read_text(encoding="utf-8")

    assert "Seed intentionally unsafe pre-existing platform role" in workflow
    assert "CREATE ROLE litoral_trace_platform_definer" in workflow
    assert "LOGIN" in workflow
    assert "INHERIT" in workflow
    assert 'assert role["rolcanlogin"] is True' in workflow
    assert 'assert role["rolinherit"] is True' in workflow
