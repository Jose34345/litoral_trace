from pathlib import Path


MIGRATION = Path("alembic/versions/071_us_lacey_reusable_supplier_evidence.py")


def test_reusable_evidence_migration_follows_paddle_head_and_creates_four_tables():
    text = MIGRATION.read_text(encoding="utf-8")

    assert 'revision = "071_us_lacey_reusable_supplier_evidence"' in text
    assert 'down_revision = "070_us_lacey_paddle_billing"' in text
    for table in (
        "us_lacey_supplier",
        "us_lacey_supplier_product",
        "us_lacey_supplier_evidence",
        "us_lacey_evidence_claim",
    ):
        assert table in text


def test_reusable_evidence_migration_is_tenant_hardened_and_blob_free():
    text = MIGRATION.read_text(encoding="utf-8")

    assert "ENABLE ROW LEVEL SECURITY" in text
    assert "FORCE ROW LEVEL SECURITY" in text
    assert "app.current_organization_id" in text
    assert "FROM PUBLIC, anon, authenticated" in text
    assert "GRANT SELECT, INSERT, UPDATE, DELETE" in text
    assert "fk_us_lacey_supplier_product_supplier_tenant" in text
    assert "fk_us_lacey_supplier_evidence_product_tenant" in text
    assert "fk_us_lacey_evidence_claim_evidence_tenant" in text
    assert "document_hash" in text
    assert "source_reference" in text
    assert "LargeBinary" not in text
    assert "BYTEA" not in text.upper()
    assert "document_bytes" not in text
