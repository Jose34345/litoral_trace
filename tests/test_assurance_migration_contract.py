from pathlib import Path


MIGRATION = Path("alembic/versions/030_add_assurance_document_intelligence.py")
RECONCILIATION_MIGRATION = Path("alembic/versions/031_add_assurance_reconciliation.py")
EXCEPTIONS_MIGRATION = Path("alembic/versions/032_add_assurance_operational_exceptions.py")


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


def test_ci_canonical_head_tracks_latest_assurance_migration():
    text = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "032_assurance_operational_exceptions (head)" in text
