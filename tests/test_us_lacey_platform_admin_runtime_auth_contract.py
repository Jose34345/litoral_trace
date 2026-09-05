from __future__ import annotations

from pathlib import Path


ADMIN_SURFACE = Path("src/litoral_trace/web/us_lacey_platform_admin.py")


def test_admin_runtime_authorization_stays_inside_security_definer_capabilities():
    source = ADMIN_SURFACE.read_text(encoding="utf-8")

    assert "get_us_lacey_db_session" in source
    assert "platform_us_lacey_account_overview" in source
    assert "platform_admin_users" in source
    assert "platform_admin_failed_jobs" in source

    # The production U.S. runtime role intentionally has no direct SELECT on
    # protected identity tables. Authorization must therefore be enforced by
    # the 042/044 SECURITY DEFINER control-plane capabilities, not ORM reads.
    assert "select(User)" not in source
    assert "select(Organization)" not in source
    assert "from litoral_trace.db.models" not in source
    assert "set_tenant_db_context" not in source
    assert "Permission.PLATFORM_ADMIN" not in source
    assert "has_permission" not in source


def test_admin_runtime_does_not_bypass_database_isolation_or_session_boundary():
    source = ADMIN_SURFACE.read_text(encoding="utf-8")

    assert "resolve_us_lacey_session(us_session)" in source
    assert "_require_platform_refresh_token_hash" in source
    assert "DATABASE_URL" in source  # only the explicit no-alias/no-direct-access documentation
    assert 'os.environ["DATABASE_URL"]' not in source
    assert "get_db_session" not in source
    assert "create_user_session" not in source
