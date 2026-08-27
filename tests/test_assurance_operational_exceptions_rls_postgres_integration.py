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
        "Assurance operational exception RLS tests require ENABLE_POSTGRES_TESTS=1, "
        "TEST_POSTGRES_DATABASE_URL and TEST_POSTGRES_MIGRATION_DATABASE_URL "
        "(or MIGRATION_DATABASE_URL)."
    ),
)


def _engine(url: str):
    return create_engine(normalize_database_url(url), pool_pre_ping=True)


def _set_tenant_context(connection, organization_id: int) -> None:
    connection.execute(
        text(
            "SELECT set_config('app.current_organization_id', :organization_id, true)"
        ),
        {"organization_id": str(organization_id)},
    )


@pytest.fixture(scope="module")
def exception_rls_fixture():
    suffix = uuid4().hex[:10]
    runtime_engine = _engine(RUNTIME_TEST_DATABASE_URL)
    owner_engine = _engine(MIGRATION_TEST_DATABASE_URL)
    created: dict[str, int] = {}

    with owner_engine.begin() as conn:
        for label, tax_prefix in (("a", "30-5"), ("b", "30-6")):
            org_id = conn.execute(
                text(
                    """
                    INSERT INTO organizations (name, slug, tax_id, tier, description, is_active)
                    VALUES (:name, :slug, :tax_id, 'pro', :description, true)
                    RETURNING id
                    """
                ),
                {
                    "name": f"Assurance Exception RLS {label.upper()} {suffix}",
                    "slug": f"assurance-exception-rls-{label}-{suffix}",
                    "tax_id": f"{tax_prefix}{suffix[:8]}",
                    "description": f"Assurance exception RLS org {label.upper()}",
                },
            ).scalar_one()
            created[f"org_{label}_id"] = org_id
            exception_id = conn.execute(
                text(
                    """
                    INSERT INTO operational_exceptions (
                        organization_id, fingerprint, source_type, source_reference,
                        operation_reference, cause_code, entity_type, entity_reference,
                        title, description, impact, priority, status, recommended_action
                    ) VALUES (
                        :organization_id, :fingerprint, 'MANUAL', :source_reference,
                        :operation_reference, 'TEST_RLS', 'OPERATION', :operation_reference,
                        'RLS fixture', 'Tenant isolation fixture', 'BLOCKING', 'CRITICAL',
                        'OPEN', 'Review fixture'
                    )
                    RETURNING id
                    """
                ),
                {
                    "organization_id": org_id,
                    "fingerprint": (("a" if label == "a" else "b") * 64),
                    "source_reference": f"fixture-{label}-{suffix}",
                    "operation_reference": f"OP-{label.upper()}-{suffix}",
                },
            ).scalar_one()
            created[f"exception_{label}_id"] = exception_id

    yield {**created, "runtime_engine": runtime_engine, "owner_engine": owner_engine}

    with owner_engine.begin() as conn:
        params = {"a": created["org_a_id"], "b": created["org_b_id"]}
        conn.execute(
            text("DELETE FROM operational_exceptions WHERE organization_id IN (:a, :b)"),
            params,
        )
        conn.execute(text("DELETE FROM organizations WHERE id IN (:a, :b)"), params)
    runtime_engine.dispose()
    owner_engine.dispose()


def test_exception_rows_are_invisible_without_tenant_context(exception_rls_fixture):
    with exception_rls_fixture["runtime_engine"].begin() as conn:
        rows = conn.execute(
            text("SELECT id, organization_id FROM operational_exceptions ORDER BY id")
        ).fetchall()
    assert rows == []


def test_exception_rows_are_scoped_to_current_tenant(exception_rls_fixture):
    with exception_rls_fixture["runtime_engine"].begin() as conn:
        _set_tenant_context(conn, exception_rls_fixture["org_a_id"])
        rows = conn.execute(
            text("SELECT id, organization_id FROM operational_exceptions ORDER BY id")
        ).fetchall()
    assert rows
    assert {row[1] for row in rows} == {exception_rls_fixture["org_a_id"]}
    assert exception_rls_fixture["exception_b_id"] not in {row[0] for row in rows}


def test_cross_tenant_exception_primary_key_is_invisible(exception_rls_fixture):
    with exception_rls_fixture["runtime_engine"].begin() as conn:
        _set_tenant_context(conn, exception_rls_fixture["org_a_id"])
        rows = conn.execute(
            text("SELECT id FROM operational_exceptions WHERE id = :id"),
            {"id": exception_rls_fixture["exception_b_id"]},
        ).fetchall()
    assert rows == []


def test_runtime_cannot_insert_exception_for_another_tenant(exception_rls_fixture):
    with pytest.raises(DBAPIError):
        with exception_rls_fixture["runtime_engine"].begin() as conn:
            _set_tenant_context(conn, exception_rls_fixture["org_a_id"])
            conn.execute(
                text(
                    """
                    INSERT INTO operational_exceptions (
                        organization_id, fingerprint, source_type, operation_reference,
                        cause_code, entity_type, entity_reference, title, description,
                        impact, priority, status, recommended_action
                    ) VALUES (
                        :organization_id, :fingerprint, 'MANUAL', 'CROSS-TENANT',
                        'FORBIDDEN', 'OPERATION', 'CROSS-TENANT', 'Forbidden',
                        'Must fail RLS', 'BLOCKING', 'CRITICAL', 'OPEN', 'Reject'
                    )
                    """
                ),
                {
                    "organization_id": exception_rls_fixture["org_b_id"],
                    "fingerprint": "f" * 64,
                },
            )
