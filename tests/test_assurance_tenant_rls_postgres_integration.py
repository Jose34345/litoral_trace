from __future__ import annotations

import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from litoral_trace.config.settings import normalize_database_url


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


POSTGRES_RLS_TESTS_ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_TEST_DATABASE_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
MIGRATION_TEST_DATABASE_URL = (
    os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")
    or os.environ.get("MIGRATION_DATABASE_URL")
)

pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_RLS_TESTS_ENABLED
        and RUNTIME_TEST_DATABASE_URL
        and MIGRATION_TEST_DATABASE_URL
    ),
    reason=(
        "Assurance RLS tests require ENABLE_POSTGRES_TESTS=1, "
        "TEST_POSTGRES_DATABASE_URL and TEST_POSTGRES_MIGRATION_DATABASE_URL "
        "(or MIGRATION_DATABASE_URL)."
    ),
)


def _runtime_engine():
    return create_engine(
        normalize_database_url(RUNTIME_TEST_DATABASE_URL),
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )


def _owner_engine():
    return create_engine(
        normalize_database_url(MIGRATION_TEST_DATABASE_URL),
        pool_pre_ping=True,
    )


def _set_tenant_context(connection, organization_id: int) -> None:
    connection.execute(
        text(
            "SELECT set_config("
            "'app.current_organization_id', "
            ":organization_id, "
            "true"
            ")"
        ),
        {"organization_id": str(organization_id)},
    )


@pytest.fixture(scope="module")
def assurance_rls_fixture():
    suffix = uuid4().hex[:10]
    runtime_engine = _runtime_engine()
    owner_engine = _owner_engine()
    created: dict[str, int] = {}

    with owner_engine.begin() as conn:
        created["org_a_id"] = conn.execute(
            text(
                """
                INSERT INTO organizations (name, slug, tax_id, tier, description, is_active)
                VALUES (:name, :slug, :tax_id, 'pro', 'Assurance RLS org A', true)
                RETURNING id
                """
            ),
            {
                "name": f"Assurance RLS Org A {suffix}",
                "slug": f"assurance-rls-org-a-{suffix}",
                "tax_id": f"30-7{suffix[:8]}",
            },
        ).scalar_one()
        created["org_b_id"] = conn.execute(
            text(
                """
                INSERT INTO organizations (name, slug, tax_id, tier, description, is_active)
                VALUES (:name, :slug, :tax_id, 'pro', 'Assurance RLS org B', true)
                RETURNING id
                """
            ),
            {
                "name": f"Assurance RLS Org B {suffix}",
                "slug": f"assurance-rls-org-b-{suffix}",
                "tax_id": f"30-8{suffix[:8]}",
            },
        ).scalar_one()

        for label in ("a", "b"):
            org_id = created[f"org_{label}_id"]
            created[f"vault_{label}_id"] = conn.execute(
                text(
                    """
                    INSERT INTO vault_documents (
                        organization_id,
                        original_filename,
                        content_type,
                        size_bytes,
                        sha256,
                        object_key,
                        storage_backend,
                        storage_bucket,
                        document_type,
                        status,
                        idempotency_key
                    )
                    VALUES (
                        :organization_id,
                        :filename,
                        'application/pdf',
                        128,
                        :sha256,
                        :object_key,
                        's3',
                        'assurance-rls-test',
                        'OTHER_EVIDENCE',
                        'available',
                        :idempotency_key
                    )
                    RETURNING id
                    """
                ),
                {
                    "organization_id": org_id,
                    "filename": f"assurance-{label}-{suffix}.pdf",
                    "sha256": (label * 64),
                    "object_key": f"assurance/rls/{suffix}/{label}.pdf",
                    "idempotency_key": f"assurance-rls-{suffix}-{label}",
                },
            ).scalar_one()
            created[f"document_{label}_id"] = conn.execute(
                text(
                    """
                    INSERT INTO assurance_documents (
                        organization_id,
                        vault_document_id,
                        semantic_document_type,
                        type_confidence,
                        processing_status
                    )
                    VALUES (
                        :organization_id,
                        :vault_document_id,
                        'INVOICE',
                        0.99,
                        'EXTRACTED'
                    )
                    RETURNING id
                    """
                ),
                {
                    "organization_id": org_id,
                    "vault_document_id": created[f"vault_{label}_id"],
                },
            ).scalar_one()
            created[f"run_{label}_id"] = conn.execute(
                text(
                    """
                    INSERT INTO document_extraction_runs (
                        organization_id,
                        assurance_document_id,
                        engine,
                        engine_version,
                        status
                    )
                    VALUES (
                        :organization_id,
                        :assurance_document_id,
                        'fixture-extractor',
                        '1.0',
                        'SUCCEEDED'
                    )
                    RETURNING id
                    """
                ),
                {
                    "organization_id": org_id,
                    "assurance_document_id": created[f"document_{label}_id"],
                },
            ).scalar_one()
            created[f"field_{label}_id"] = conn.execute(
                text(
                    """
                    INSERT INTO extracted_document_fields (
                        organization_id,
                        assurance_document_id,
                        extraction_run_id,
                        field_name,
                        original_value,
                        normalized_value,
                        value_type,
                        confidence,
                        confidence_level,
                        source_page,
                        source_locator,
                        auto_accepted,
                        needs_review
                    )
                    VALUES (
                        :organization_id,
                        :assurance_document_id,
                        :extraction_run_id,
                        'invoice_number',
                        :value,
                        :value,
                        'text',
                        0.99,
                        'HIGH',
                        1,
                        'page=1;field=invoice_number',
                        true,
                        false
                    )
                    RETURNING id
                    """
                ),
                {
                    "organization_id": org_id,
                    "assurance_document_id": created[f"document_{label}_id"],
                    "extraction_run_id": created[f"run_{label}_id"],
                    "value": f"INV-{label.upper()}-{suffix}",
                },
            ).scalar_one()
            created[f"claim_{label}_id"] = conn.execute(
                text(
                    """
                    INSERT INTO document_claims (
                        organization_id,
                        assurance_document_id,
                        claim_type,
                        issuer,
                        subject_type,
                        subject_reference,
                        statement
                    )
                    VALUES (
                        :organization_id,
                        :assurance_document_id,
                        'DOCUMENT_EXISTS',
                        'Fixture issuer',
                        'OPERATION',
                        :subject_reference,
                        'Evidence exists for the operation.'
                    )
                    RETURNING id
                    """
                ),
                {
                    "organization_id": org_id,
                    "assurance_document_id": created[f"document_{label}_id"],
                    "subject_reference": f"OP-{label.upper()}-{suffix}",
                },
            ).scalar_one()
            created[f"link_{label}_id"] = conn.execute(
                text(
                    """
                    INSERT INTO document_entity_links (
                        organization_id,
                        assurance_document_id,
                        entity_type,
                        entity_reference,
                        link_confidence,
                        link_method,
                        human_confirmed
                    )
                    VALUES (
                        :organization_id,
                        :assurance_document_id,
                        'OPERATION',
                        :entity_reference,
                        1.0,
                        'EXACT_IDENTIFIER',
                        false
                    )
                    RETURNING id
                    """
                ),
                {
                    "organization_id": org_id,
                    "assurance_document_id": created[f"document_{label}_id"],
                    "entity_reference": f"OP-{label.upper()}-{suffix}",
                },
            ).scalar_one()

    yield {**created, "runtime_engine": runtime_engine, "owner_engine": owner_engine}

    with owner_engine.begin() as conn:
        for table in (
            "document_entity_links",
            "document_claims",
            "extracted_document_fields",
            "document_extraction_runs",
            "assurance_documents",
            "vault_documents",
        ):
            conn.execute(
                text(
                    f"DELETE FROM {table} "
                    "WHERE organization_id IN (:org_a_id, :org_b_id)"
                ),
                {
                    "org_a_id": created["org_a_id"],
                    "org_b_id": created["org_b_id"],
                },
            )
        conn.execute(
            text(
                "DELETE FROM organizations "
                "WHERE id IN (:org_a_id, :org_b_id)"
            ),
            {
                "org_a_id": created["org_a_id"],
                "org_b_id": created["org_b_id"],
            },
        )

    runtime_engine.dispose()
    owner_engine.dispose()


@pytest.mark.parametrize(
    "table_name",
    (
        "assurance_documents",
        "document_extraction_runs",
        "extracted_document_fields",
        "document_claims",
        "document_entity_links",
    ),
)
def test_assurance_tables_return_no_rows_without_tenant_context(
    assurance_rls_fixture,
    table_name,
):
    with assurance_rls_fixture["runtime_engine"].begin() as conn:
        rows = conn.execute(
            text(f"SELECT id, organization_id FROM {table_name}")
        ).fetchall()

    assert rows == []


@pytest.mark.parametrize(
    "table_name",
    (
        "assurance_documents",
        "document_extraction_runs",
        "extracted_document_fields",
        "document_claims",
        "document_entity_links",
    ),
)
def test_assurance_tables_are_scoped_to_current_tenant(
    assurance_rls_fixture,
    table_name,
):
    with assurance_rls_fixture["runtime_engine"].begin() as conn:
        _set_tenant_context(conn, assurance_rls_fixture["org_a_id"])
        rows = conn.execute(
            text(f"SELECT organization_id FROM {table_name} ORDER BY id")
        ).fetchall()

    assert rows
    assert {row[0] for row in rows} == {assurance_rls_fixture["org_a_id"]}


@pytest.mark.parametrize(
    ("table_name", "foreign_id_key"),
    (
        ("assurance_documents", "document_b_id"),
        ("document_extraction_runs", "run_b_id"),
        ("extracted_document_fields", "field_b_id"),
        ("document_claims", "claim_b_id"),
        ("document_entity_links", "link_b_id"),
    ),
)
def test_cross_tenant_primary_key_lookup_is_invisible(
    assurance_rls_fixture,
    table_name,
    foreign_id_key,
):
    with assurance_rls_fixture["runtime_engine"].begin() as conn:
        _set_tenant_context(conn, assurance_rls_fixture["org_a_id"])
        rows = conn.execute(
            text(f"SELECT id FROM {table_name} WHERE id = :id"),
            {"id": assurance_rls_fixture[foreign_id_key]},
        ).fetchall()

    assert rows == []


def test_runtime_cannot_insert_assurance_document_for_another_tenant(
    assurance_rls_fixture,
):
    with pytest.raises(DBAPIError):
        with assurance_rls_fixture["runtime_engine"].begin() as conn:
            _set_tenant_context(conn, assurance_rls_fixture["org_a_id"])
            conn.execute(
                text(
                    """
                    INSERT INTO assurance_documents (
                        organization_id,
                        vault_document_id,
                        semantic_document_type,
                        type_confidence,
                        processing_status
                    )
                    VALUES (
                        :organization_id,
                        :vault_document_id,
                        'INVOICE',
                        0.95,
                        'UPLOADED'
                    )
                    """
                ),
                {
                    "organization_id": assurance_rls_fixture["org_b_id"],
                    "vault_document_id": assurance_rls_fixture["vault_b_id"],
                },
            )
