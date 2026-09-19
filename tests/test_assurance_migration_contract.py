from pathlib import Path


MIGRATION = Path("alembic/versions/030_add_assurance_document_intelligence.py")
RECONCILIATION_MIGRATION = Path("alembic/versions/031_add_assurance_reconciliation.py")
EXCEPTIONS_MIGRATION = Path("alembic/versions/032_add_assurance_operational_exceptions.py")
SUPPLIERS_MIGRATION = Path("alembic/versions/033_add_assurance_suppliers.py")
US_LACEY_MIGRATION = Path("alembic/versions/034_add_us_lacey_pilot_core.py")
US_LACEY_SELF_SERVICE_MIGRATION = Path("alembic/versions/035_add_us_lacey_self_service.py")
US_LACEY_STATUS_FIX_MIGRATION = Path("alembic/versions/036_fix_us_lacey_status_ambiguity.py")
US_LACEY_PORTAL_AUTH_MIGRATION = Path("alembic/versions/037_add_us_lacey_portal_auth_functions.py")
US_LACEY_PILOT_ACTIVATION_MIGRATION = Path("alembic/versions/038_us_lacey_pilot_activation.py")
US_LACEY_OWNER_ADMIN_MIGRATION = Path("alembic/versions/042_add_us_lacey_owner_admin_overview.py")
US_LACEY_CONTROL_PLANE_AUDIT_BILLING_MIGRATION = Path(
    "alembic/versions/054_us_lacey_control_plane_audit_billing.py"
)
US_LACEY_READONLY_IMPERSONATION_MIGRATION = Path(
    "alembic/versions/055_us_lacey_readonly_impersonation.py"
)


def test_assurance_migration_has_expected_parent_and_tables():
    text = MIGRATION.read_text(encoding="utf-8")

    assert 'revision: str = "030_assurance_document_intelligence"' in text
    assert '"029_add_smart_import_profiles"' in text
    for table in (
        "assurance_documents",
        "document_extraction_runs",
        "extracted_document_fields",
        "document_claims",
        "document_entity_links",
    ):
        assert table in text


def test_assurance_tables_use_forced_rls_and_runtime_least_privilege():
    text = MIGRATION.read_text(encoding="utf-8")

    assert "ENABLE ROW LEVEL SECURITY" in text
    assert "FORCE ROW LEVEL SECURITY" in text
    assert "app.current_organization_id" in text
    assert "GRANT SELECT, INSERT, UPDATE" in text
    assert "REVOKE ALL PRIVILEGES" in text


def test_reconciliation_migration_is_chained_after_document_intelligence():
    text = RECONCILIATION_MIGRATION.read_text(encoding="utf-8")
    assert 'revision: str = "031_assurance_reconciliation"' in text
    assert '"030_assurance_document_intelligence"' in text
    assert "reconciliation_issues" in text
    assert "FORCE ROW LEVEL SECURITY" in text


def test_operational_exceptions_migration_is_chained_and_tenant_hardened():
    text = EXCEPTIONS_MIGRATION.read_text(encoding="utf-8")
    assert 'revision: str = "032_assurance_operational_exceptions"' in text
    assert '"031_assurance_reconciliation"' in text
    assert "operational_exceptions" in text
    assert "ENABLE ROW LEVEL SECURITY" in text
    assert "FORCE ROW LEVEL SECURITY" in text
    assert "app.current_organization_id" in text
    assert "GRANT SELECT, INSERT, UPDATE" in text
    assert "REVOKE ALL PRIVILEGES" in text


def test_assurance_suppliers_migration_is_chained_and_tenant_hardened():
    text = SUPPLIERS_MIGRATION.read_text(encoding="utf-8")
    assert 'revision: str = "033_assurance_suppliers"' in text
    assert '"032_assurance_operational_exceptions"' in text
    assert "assurance_suppliers" in text
    assert "fk_assurance_suppliers_source_document_tenant" in text
    assert 'ondelete="RESTRICT"' in text
    assert "ENABLE ROW LEVEL SECURITY" in text
    assert "FORCE ROW LEVEL SECURITY" in text
    assert "app.current_organization_id" in text
    assert "GRANT SELECT, INSERT, UPDATE" in text
    assert "REVOKE ALL PRIVILEGES" in text


def test_us_lacey_migration_follows_assurance_suppliers():
    text = US_LACEY_MIGRATION.read_text(encoding="utf-8")
    assert 'revision: str = "034_us_lacey_pilot_core"' in text
    assert '"033_assurance_suppliers"' in text


def test_us_lacey_self_service_follows_pilot_core():
    text = US_LACEY_SELF_SERVICE_MIGRATION.read_text(encoding="utf-8")
    assert 'revision: str = "035_us_lacey_self_service"' in text
    assert '"034_us_lacey_pilot_core"' in text
    for table in (
        "us_lacey_subscriptions",
        "us_lacey_payments",
        "us_lacey_terms_acceptances",
        "us_lacey_processing_jobs",
    ):
        assert table in text
    assert "FORCE ROW LEVEL SECURITY" in text
    assert "us_lacey_self_register" in text
    assert "us_lacey_verify_email" in text


def test_us_lacey_portal_auth_is_chained_after_status_fix():
    status_fix = US_LACEY_STATUS_FIX_MIGRATION.read_text(encoding="utf-8")
    portal_auth = US_LACEY_PORTAL_AUTH_MIGRATION.read_text(encoding="utf-8")
    assert 'revision: str = "036_fix_us_lacey_status_ambiguity"' in status_fix
    assert '"035_us_lacey_self_service"' in status_fix
    assert 'revision: str = "037_us_lacey_portal_auth"' in portal_auth
    assert '"036_fix_us_lacey_status_ambiguity"' in portal_auth
    assert "us_lacey_portal_login_lookup" in portal_auth
    assert "us_lacey_portal_create_session" in portal_auth
    assert "us_lacey_portal_session_lookup" in portal_auth
    assert "us_lacey_portal_revoke_session" in portal_auth


def test_us_lacey_owner_admin_follows_lemon_head():
    text = US_LACEY_OWNER_ADMIN_MIGRATION.read_text(encoding="utf-8")
    assert 'revision: str = "042_us_lacey_owner_admin"' in text
    assert '"041_us_lacey_lemon"' in text
    assert "platform_us_lacey_account_overview" in text


def test_us_lacey_control_plane_audit_billing_follows_sandbox_purge():
    text = US_LACEY_CONTROL_PLANE_AUDIT_BILLING_MIGRATION.read_text(encoding="utf-8")

    assert 'revision: str = "054_us_lacey_control_plane_audit_billing"' in text
    assert '"053_sandbox_purge_queue"' in text
    for column in (
        "billing_provider",
        "provider_customer_id",
        "provider_subscription_id",
        "billing_sync_status",
        "billing_last_sync_attempt_at",
        "billing_last_synced_at",
        "billing_last_error_code",
        "provider_updated_at",
    ):
        assert column in text
    assert "us_lacey_admin_audit_logs" in text
    assert "ENABLE ROW LEVEL SECURITY" in text
    assert "FORCE ROW LEVEL SECURITY" in text
    assert "GRANT SELECT, INSERT" in text
    assert "public._us_lacey_admin_audit" in text
    assert "public._platform_insert_audit_log" in text
    assert "_restore_044_platform_mutations" in text
    assert "sa.ForeignKey" not in text


def test_us_lacey_readonly_impersonation_follows_control_plane_audit():
    text = US_LACEY_READONLY_IMPERSONATION_MIGRATION.read_text(encoding="utf-8")

    assert 'revision: str = "055_us_lacey_readonly_impersonation"' in text
    assert '"054_us_lacey_control_plane_audit_billing"' in text
    assert "litoral_trace_impersonation_reader" in text
    assert "NOLOGIN" in text
    assert "NOBYPASSRLS" in text
    assert "AS RESTRICTIVE" in text
    assert "us_lacey_admin_impersonation_sessions" in text
    assert "interval '15 minutes'" in text
    assert "READONLY_IMPERSONATION_STARTED" in text
    assert "READONLY_IMPERSONATION_ENDED" in text
    assert "_assert_reader_is_locked_down" in text
    assert "sa.ForeignKey" not in text


def test_ci_canonical_head_tracks_latest_platform_migration():
    text = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "055_us_lacey_readonly_impersonation (head)" in text


def test_us_lacey_pilot_activation_follows_portal_auth():
    text = US_LACEY_PILOT_ACTIVATION_MIGRATION.read_text(encoding="utf-8")
    assert 'revision = "038_us_lacey_pilot_activation"' in text
    assert 'down_revision = "037_us_lacey_portal_auth"' in text
