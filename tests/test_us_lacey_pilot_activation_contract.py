from pathlib import Path

import pytest

from litoral_trace.us_lacey.self_service import UsLaceySelfServiceError, activate_us_lacey_pilot


MIGRATION = Path("alembic/versions/038_us_lacey_pilot_activation.py")


def test_pilot_activation_migration_is_control_plane_scoped_and_reversible():
    text = MIGRATION.read_text(encoding="utf-8")
    assert 'down_revision = "037_us_lacey_portal_auth"' in text
    assert "us_lacey_activate_pilot" in text
    assert "_platform_superadmin_session_actor" in text
    assert "_platform_insert_audit_log" in text
    assert "REVOKE ALL ON FUNCTION" in text
    assert "GRANT EXECUTE" in text
    assert "DROP FUNCTION IF EXISTS" in text


def test_pilot_activation_only_changes_profile_status_and_audits_transition():
    text = MIGRATION.read_text(encoding="utf-8")
    update = text.split("UPDATE public.us_lacey_organization_profiles", 1)[1].split("WHERE id = profile_id", 1)[0]
    assert "account_status = 'PILOT'" in update
    assert "us_lacey_payments" not in update
    assert "us_lacey_subscriptions" not in update
    for status in ("PAYMENT_PENDING", "PILOT", "previous_status", "new_status", "reason", "idempotent"):
        assert status in text
    for forbidden in ("ACTIVE' THEN", "PENDING_EMAIL' THEN", "SUSPENDED' THEN"):
        assert forbidden not in text


def test_pilot_reason_requires_non_empty_normalized_value_before_database_access():
    with pytest.raises(UsLaceySelfServiceError):
        activate_us_lacey_pilot(platform_refresh_token="x", organization_id=1, reason=" \n ")
    with pytest.raises(UsLaceySelfServiceError):
        activate_us_lacey_pilot(platform_refresh_token="x", organization_id=0, reason="valid")
