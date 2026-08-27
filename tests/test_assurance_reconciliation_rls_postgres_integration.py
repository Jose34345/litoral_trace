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
        "Assurance reconciliation RLS tests require ENABLE_POSTGRES_TESTS=1, "
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
def reconciliation_rls_fixture():
    suffix = uuid4().hex[:10]
    runtime_engine = _runtime_engine()
    owner_engine = _owner_engine()
    created: dict[str, int] = {}

    with owner_engine.begin() as conn:
        for label, tax_prefix in (("a", "30-7"), ("b", "30-8")):
            org_id = conn.execute(
                text(
                    """
                    INSERT INTO organizations (name, slug, tax_id, tier, description, is_active)
                    VALUES (:name, :slug, :tax_id, 'pro', :description, true)
                    RETURNING id
                    """
                ),
                {
                    "name": f"Assurance Reconciliation RLS {label.upper()} {suffix}",
                    "slug": f"assurance-reconciliation-rls-{label}-{suffix}",
                    "tax_id": f"{tax_prefix}{suffix[:8]}",
                    "description": f"Assurance reconciliation RLS org {label.upper()}",
                },
            ).scalar_one()
            created[f"org_{label}_id"] = org_id

            vault_id = conn.execute(
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
                        'assurance-reconciliation-rls-test',
                        'OTHER_EVIDENCE',
                        'available',
                        :idempotency_key
                    )
                    RETURNING id
                    """
                ),
                {
                    "organization_id": org_id,
                    "filename": f"reconciliation-{label}-{suffix}.pdf",
                    "sha256": (label * 64),
                    "object_key": f"assurance/reconciliation/rls/{suffix}/{label}.pdf",
                    "idempotency_key": f"assurance-reconciliation-rls-{suffix}-{label}",
                },
            ).scalar_one()
            created[f"vault_{label}_id"] = vault_id

            document_id = conn.execute(
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
                    "vault_document_id": vault_id,
                },
            ).scalar_one()
            created[f"document_{label}_id"] = document_id

            issue_id = conn.execute(
                text(
                    """
                    INSERT INTO reconciliation_issues (
                        organization_id,
                        operation_reference,
                        fingerprint,
                        rule_code,
                        severity,
                        status,
                        left_document_id,
                        left_source,
                        left_value,
                        right_source,
                        right_value,
                        explanation,
                        evidence_json
                    )
                    VALUES (
                        :organization_id,
                        :operation_reference,
                        :fingerprint,
                        'TEST_RECONCILIATION_RULE',
                        'BLOCKING',
                        'OPEN',
                        :left_document_id,
                        :left_source,
                        '80',
                        'operation.quantity',
                        '75',
                        'Fixture reconciliation discrepancy.',
                        CAST(:evidence_json AS json)
                    )
                    RETURNING id
                    """
                ),
                {
                    "organization_id": org_id,
                    "operation_reference": f"OP-{label.upper()}-{suffix}",
                    "fingerprint": (("1" if label == "a" else "2") * 64),
                    "left_document_id": document_id,
                    "left_source": f"reconciliation-{label}-{suffix}.pdf [quantity]",
                    "evidence_json": '[{"source":"fixture","field_name":"quantity","value":"80"}]',
                },
            ).scalar_one()
            created[f"issue_{label}_id"] = issue_id

    yield {**created, "runtime_engine": runtime_engine, "owner_engine": owner_engine}

    with owner_engine.begin() as conn:
        params = {
            "org_a_id": created["org_a_id"],
            "org_b_id": created["org_b_id"],
        }
        conn.execute(
            text(
                "DELETE FROM reconciliation_issues "
                "WHERE organization_id IN (:org_a_id, :org_b_id)"
            ),
            params,
        )
        conn.execute(
            text(
                "DELETE FROM assurance_documents "
                "WHERE organization_id IN (:org_a_id, :org_b_id)"
            ),
            params,
        )
        conn.execute(
            text(
                "DELETE FROM vault_documents "
                "WHERE organization_id IN (:org_a_id, :org_b_id)"
            ),
            params,
        )
        conn.execute(
            text("DELETE FROM organizations WHERE id IN (:org_a_id, :org_b_id)"),
            params,
        )

    runtime_engine.dispose()
    owner_engine.dispose()


def test_reconciliation_rows_are_invisible_without_tenant_context(
    reconciliation_rls_fixture,
):
    with reconciliation_rls_fixture["runtime_engine"].begin() as conn:
        rows = conn.execute(
            text("SELECT id, organization_id FROM reconciliation_issues ORDER BY id")
        ).fetchall()

    assert rows == []


def test_reconciliation_rows_are_scoped_to_current_tenant(
    reconciliation_rls_fixture,
):
    with reconciliation_rls_fixture["runtime_engine"].begin() as conn:
        _set_tenant_context(conn, reconciliation_rls_fixture["org_a_id"])
        rows = conn.execute(
            text("SELECT id, organization_id FROM reconciliation_issues ORDER BY id")
        ).fetchall()

    assert rows
    assert {row[1] for row in rows} == {reconciliation_rls_fixture["org_a_id"]}
    assert reconciliation_rls_fixture["issue_b_id"] not in {row[0] for row in rows}


def test_cross_tenant_reconciliation_primary_key_is_invisible(
    reconciliation_rls_fixture,
):
    with reconciliation_rls_fixture["runtime_engine"].begin() as conn:
        _set_tenant_context(conn, reconciliation_rls_fixture["org_a_id"])
        rows = conn.execute(
            text("SELECT id FROM reconciliation_issues WHERE id = :id"),
            {"id": reconciliation_rls_fixture["issue_b_id"]},
        ).fetchall()

    assert rows == []


def test_runtime_cannot_insert_reconciliation_issue_for_another_tenant(
    reconciliation_rls_fixture,
):
    with pytest.raises(DBAPIError):
        with reconciliation_rls_fixture["runtime_engine"].begin() as conn:
            _set_tenant_context(conn, reconciliation_rls_fixture["org_a_id"])
            conn.execute(
                text(
                    """
                    INSERT INTO reconciliation_issues (
                        organization_id,
                        operation_reference,
                        fingerprint,
                        rule_code,
                        severity,
                        status,
                        left_document_id,
                        left_source,
                        explanation
                    )
                    VALUES (
                        :organization_id,
                        'CROSS-TENANT-ATTEMPT',
                        :fingerprint,
                        'TEST_CROSS_TENANT',
                        'BLOCKING',
                        'OPEN',
                        :left_document_id,
                        'forbidden-source',
                        'This insert must be rejected by tenant RLS.'
                    )
                    """
                ),
                {
                    "organization_id": reconciliation_rls_fixture["org_b_id"],
                    "fingerprint": "f" * 64,
                    "left_document_id": reconciliation_rls_fixture["document_b_id"],
                },
            )
