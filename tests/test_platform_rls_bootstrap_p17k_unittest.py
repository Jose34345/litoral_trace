from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "alembic" / "versions" / "027_fix_platform_control_plane_rls_bootstrap.py"


def _migration_source() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def test_platform_control_plane_bootstraps_tenant_before_forced_rls_reads() -> None:
    source = _migration_source()

    assert 'revision: str = "027_fix_platform_control_plane_rls_bootstrap"' in source
    assert 'down_revision: Union[str, Sequence[str], None] = "026_add_eudr_acceptance_attempts"' in source
    assert "bootstrap_auth_session_by_token_hash" in source
    assert "bootstrap_organization_id" in source
    assert "set_config(" in source
    assert "'app.current_organization_id'" in source
    assert "bootstrap_organization_id::text" in source
    assert "true" in source


def test_platform_control_plane_keeps_fail_closed_superadmin_checks() -> None:
    source = _migration_source()

    assert "user_sessions.revoked_at IS NULL" in source
    assert "user_sessions.expires_at > now()" in source
    assert "users.is_active" in source
    assert "organizations.is_active" in source
    assert "users.role = 'superadmin'" in source
    assert "ERRCODE = '28000'" in source
    assert "ERRCODE = '42501'" in source


def test_platform_rls_fix_does_not_disable_or_bypass_rls() -> None:
    source = _migration_source().upper()

    assert "DISABLE ROW LEVEL SECURITY" not in source
    assert "NO FORCE ROW LEVEL SECURITY" not in source
    assert "BYPASSRLS" not in source
    assert "ALTER ROLE" not in source
