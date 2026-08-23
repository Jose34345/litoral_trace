from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "alembic" / "versions" / "028_platform_definer_rls.py"


def _migration_source() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def _upgrade_source() -> str:
    source = _migration_source()
    return source.split("def downgrade()", 1)[0]


def test_platform_control_plane_uses_dedicated_non_login_non_bypass_definer() -> None:
    source = _migration_source()

    assert 'revision: str = "028_platform_definer_rls"' in source
    assert 'down_revision: Union[str, Sequence[str], None] = "027_fix_platform_rls_bootstrap"' in source
    assert 'PLATFORM_ROLE = "litoral_trace_platform_definer"' in source
    assert "NOLOGIN" in source
    assert "NOSUPERUSER" in source
    assert "NOCREATEROLE" in source
    assert "NOINHERIT" in source
    assert "NOBYPASSRLS" in source


def test_platform_control_plane_keeps_force_rls_and_scopes_cross_tenant_policies() -> None:
    source = _upgrade_source()

    assert "organizations_platform_select" in source
    assert "organizations_platform_insert" in source
    assert "organizations_platform_update" in source
    assert "users_platform_select" in source
    assert "users_platform_insert" in source
    assert "user_sessions_platform_select" in source
    assert "user_sessions_platform_update" in source
    assert "licenses_platform_select" in source
    assert "licenses_platform_insert" in source
    assert "licenses_platform_update" in source
    assert "audit_logs_platform_insert" in source
    assert "platform_delete" not in source
    assert "DISABLE ROW LEVEL SECURITY" not in source.upper()
    assert "NO FORCE ROW LEVEL SECURITY" not in source.upper()


def test_platform_actor_no_longer_depends_on_foreign_owned_auth_bootstrap() -> None:
    source = _upgrade_source()
    actor_source = source.split(
        "def _restore_platform_actor_without_bootstrap_dependency()",
        1,
    )[1].split("def _transfer_platform_function_ownership()", 1)[0]

    assert "bootstrap_auth_session_by_token_hash" not in actor_source
    assert "user_sessions.revoked_at IS NULL" in actor_source
    assert "user_sessions.expires_at > now()" in actor_source
    assert "users.is_active" in actor_source
    assert "organizations.is_active" in actor_source
    assert "users.role = 'superadmin'" in actor_source
    assert "ERRCODE = '28000'" in actor_source
    assert "ERRCODE = '42501'" in actor_source


def test_platform_function_ownership_and_runtime_execute_are_fail_closed() -> None:
    source = _upgrade_source()

    assert "ALTER FUNCTION" in source
    assert "OWNER TO {PLATFORM_ROLE}" in source
    assert "REVOKE ALL ON FUNCTION" in source
    assert "FROM PUBLIC" in source
    assert "GRANT EXECUTE ON FUNCTION" in source
    assert "TO {RUNTIME_ROLE}" in source
    assert "REVOKE ALL ON FUNCTION {function_signature} FROM {RUNTIME_ROLE}" in source
