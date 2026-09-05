from __future__ import annotations

from pathlib import Path


MIGRATION = Path("alembic/versions/044_platform_admin_control_plane.py")
SERVICE = Path("src/litoral_trace/services/us_lacey_admin.py")
COMMAND = Path("src/litoral_trace/admin/provision_founder.py")


def test_044_is_a_single_hardened_control_plane_head() -> None:
    source = MIGRATION.read_text(encoding="utf-8")
    assert 'revision: str = "044_platform_admin_control_plane"' in source
    assert '"043_us_lacey_engine2_shadow"' in source
    assert "SECURITY DEFINER" in source
    assert "SET search_path = public, pg_temp" in source
    assert "_platform_superadmin_session_actor" in source
    assert "GRANT EXECUTE ON FUNCTION" in source
    assert "BYPASSRLS" not in source
    assert "SUPERUSER" not in source


def test_044_has_only_capability_specific_mutations_and_pilot_guard() -> None:
    source = MIGRATION.read_text(encoding="utf-8")
    for capability in (
        "platform_admin_promote_existing_user",
        "platform_admin_set_us_lacey_account_status",
        "platform_admin_set_us_lacey_operation_limit",
        "platform_admin_revoke_user_sessions",
        "platform_admin_reset_pilot_account",
        "platform_admin_users",
        "platform_admin_failed_jobs",
    ):
        assert capability in source
    assert "account_status == PILOT" not in source  # SQL compares the fixed value below.
    assert "status_value IS DISTINCT FROM 'PILOT'" in source
    assert "provider='LEMON_SQUEEZY' AND status='VERIFIED'" in source
    assert "admin_update_anything" not in source


def test_control_plane_service_uses_functions_not_tenant_orm_models() -> None:
    source = SERVICE.read_text(encoding="utf-8")
    assert "_platform_admin_call" in source
    assert "bind.dialect.name != \"postgresql\"" in source
    assert "platform_admin_reset_pilot_account" in source
    assert "platform_admin_set_us_lacey_account_status" in source
    assert "from litoral_trace.db.models" not in source


def test_founder_command_uses_existing_session_and_never_handles_passwords() -> None:
    source = COMMAND.read_text(encoding="utf-8")
    assert "PLATFORM_PROVISIONER_REFRESH_TOKEN" in source
    assert "platform_admin_promote_existing_user" in source
    assert "platform_admin_set_us_lacey_account_status" in source
    assert "password_hash" not in source
    assert "password=" not in source
