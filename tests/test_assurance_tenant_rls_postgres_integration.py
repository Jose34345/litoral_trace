from __future__ import annotations

import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from litoral_trace.config.settings import normalize_database_url


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
OWNER_URL = os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL") or os.environ.get("MIGRATION_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not (ENABLED and RUNTIME_URL and OWNER_URL),
    reason="Assurance PostgreSQL RLS acceptance requires isolated runtime and owner URLs.",
)


def _engine(url: str):
    return create_engine(normalize_database_url(url), pool_pre_ping=True)


def _tenant(conn, organization_id: int) -> None:
    conn.execute(
        text("SELECT set_config('app.current_organization_id', :organization_id, true)"),
        {"organization_id": str(organization_id)},
    )


def _create_tenant_org(conn, *, label: str, suffix: str) -> int:
    """Create a fixture tenant without bypassing FORCE RLS on organizations."""
    org_id = int(
        conn.execute(
            text("SELECT nextval(pg_get_serial_sequence('public.organizations', 'id'))")
        ).scalar_one()
    )
    _tenant(conn, org_id)
    conn.execute(
        text("""
            INSERT INTO organizations (
                id, name, slug, tax_id, tier, description, is_active
            ) VALUES (
                :id, :name, :slug, :tax_id, 'pro', 'Assurance RLS acceptance', true
            )
        """),
        {
            "id": org_id,
            "name": f"Assurance RLS {label.upper()} {suffix}",
            "slug": f"assurance-rls-{label}-{suffix}",
            "tax_id": f"30-{7 if label == 'a' else 8}{suffix[:8]}",
        },
    )
    return org_id


@pytest.fixture(scope="module")
def fixture():
    suffix = uuid4().hex[:10]
    owner = _engine(OWNER_URL)
    runtime = _engine(RUNTIME_URL)
    values: dict[str, int] = {}
    with owner.begin() as conn:
        for label in ("a", "b"):
            org_id = _create_tenant_org(conn, label=label, suffix=suffix)
            values[f"org_{label}"] = org_id
            vault_id = conn.execute(
                text("""
                    INSERT INTO vault_documents (
                        organization_id, original_filename, content_type, size_bytes, sha256,
                        object_key, storage_backend, storage_bucket, document_type, status,
                        idempotency_key
                    ) VALUES (
                        :org, :filename, 'application/pdf', 128, :sha, :object_key,
                        's3', 'assurance-rls', 'OTHER_EVIDENCE', 'available', :idem
                    ) RETURNING id
                """),
                {
                    "org": org_id,
                    "filename": f"{label}-{suffix}.pdf",
                    "sha": label * 64,
                    "object_key": f"rls/{suffix}/{label}.pdf",
                    "idem": f"rls-{suffix}-{label}",
                },
            ).scalar_one()
            values[f"vault_{label}"] = vault_id
            document_id = conn.execute(
                text("""
                    INSERT INTO assurance_documents (
                        organization_id, vault_document_id, semantic_document_type,
                        type_confidence, processing_status
                    ) VALUES (:org, :vault, 'INVOICE', 0.99, 'EXTRACTED') RETURNING id
                """),
                {"org": org_id, "vault": vault_id},
            ).scalar_one()
            values[f"document_{label}"] = document_id
            run_id = conn.execute(
                text("""
                    INSERT INTO document_extraction_runs (
                        organization_id, assurance_document_id, engine, engine_version, status
                    ) VALUES (:org, :document, 'fixture', '1.0', 'SUCCEEDED') RETURNING id
                """),
                {"org": org_id, "document": document_id},
            ).scalar_one()
            conn.execute(
                text("""
                    INSERT INTO extracted_document_fields (
                        organization_id, assurance_document_id, extraction_run_id, field_name,
                        original_value, normalized_value, value_type, confidence,
                        confidence_level, auto_accepted, needs_review
                    ) VALUES (
                        :org, :document, :run, 'invoice.number', :value, :value,
                        'identifier', 0.99, 'HIGH', true, false
                    )
                """),
                {"org": org_id, "document": document_id, "run": run_id, "value": f"INV-{label}"},
            )
            conn.execute(
                text("""
                    INSERT INTO document_claims (
                        organization_id, assurance_document_id, claim_type, subject_type,
                        subject_reference, statement
                    ) VALUES (:org, :document, 'DOCUMENT_EXISTS', 'OPERATION', :ref, 'Evidence exists')
                """),
                {"org": org_id, "document": document_id, "ref": f"OP-{label}-{suffix}"},
            )
            conn.execute(
                text("""
                    INSERT INTO document_entity_links (
                        organization_id, assurance_document_id, entity_type, entity_reference,
                        link_confidence, link_method, human_confirmed
                    ) VALUES (:org, :document, 'OPERATION', :ref, 1.0, 'EXACT_IDENTIFIER', false)
                """),
                {"org": org_id, "document": document_id, "ref": f"OP-{label}-{suffix}"},
            )
    yield {**values, "owner": owner, "runtime": runtime}
    runtime.dispose()
    owner.dispose()


@pytest.mark.parametrize(
    "table",
    (
        "assurance_documents",
        "document_extraction_runs",
        "extracted_document_fields",
        "document_claims",
        "document_entity_links",
    ),
)
def test_no_tenant_context_sees_no_assurance_rows(fixture, table):
    with fixture["runtime"].begin() as conn:
        assert conn.execute(text(f"SELECT id FROM {table}")).fetchall() == []


@pytest.mark.parametrize(
    "table",
    (
        "assurance_documents",
        "document_extraction_runs",
        "extracted_document_fields",
        "document_claims",
        "document_entity_links",
    ),
)
def test_current_tenant_never_sees_other_company_rows(fixture, table):
    with fixture["runtime"].begin() as conn:
        _tenant(conn, fixture["org_a"])
        rows = conn.execute(text(f"SELECT organization_id FROM {table}")).fetchall()
    assert rows
    assert {row[0] for row in rows} == {fixture["org_a"]}


def test_cross_tenant_assurance_insert_is_denied(fixture):
    with pytest.raises(DBAPIError):
        with fixture["runtime"].begin() as conn:
            _tenant(conn, fixture["org_a"])
            conn.execute(
                text("""
                    INSERT INTO assurance_documents (
                        organization_id, vault_document_id, semantic_document_type,
                        type_confidence, processing_status
                    ) VALUES (:org, :vault, 'INVOICE', 0.95, 'UPLOADED')
                """),
                {"org": fixture["org_b"], "vault": fixture["vault_b"]},
            )
