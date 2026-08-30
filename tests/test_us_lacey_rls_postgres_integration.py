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
    reason="US Lacey RLS acceptance requires isolated runtime and owner URLs.",
)


def _engine(url: str):
    return create_engine(normalize_database_url(url), pool_pre_ping=True)


def _tenant(conn, organization_id: int) -> None:
    conn.execute(
        text("SELECT set_config('app.current_organization_id', :organization_id, true)"),
        {"organization_id": str(organization_id)},
    )


def _create_org(conn, *, label: str, suffix: str) -> int:
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
                :id, :name, :slug, :tax_id, 'pro', 'US Lacey RLS acceptance', true
            )
        """),
        {
            "id": org_id,
            "name": f"US Lacey Tenant {label.upper()} {suffix}",
            "slug": f"us-lacey-rls-{label}-{suffix}",
            "tax_id": f"US-{label.upper()}-{suffix}",
        },
    )
    return org_id


@pytest.fixture(scope="module")
def fixture():
    suffix = uuid4().hex[:9]
    owner = _engine(OWNER_URL)
    runtime = _engine(RUNTIME_URL)
    values: dict[str, int] = {}
    with owner.begin() as conn:
        for label in ("a", "b"):
            org_id = _create_org(conn, label=label, suffix=suffix)
            values[f"org_{label}"] = org_id
            _tenant(conn, org_id)
            operation_id = conn.execute(
                text("""
                    INSERT INTO us_lacey_operations (
                        organization_id,
                        client_reference,
                        status,
                        document_count,
                        merchandise_line_count
                    ) VALUES (
                        :org,
                        :reference,
                        'NEW',
                        0,
                        1
                    ) RETURNING id
                """),
                {"org": org_id, "reference": f"RLS-{label.upper()}-{suffix}"},
            ).scalar_one()
            values[f"operation_{label}"] = int(operation_id)
    yield {**values, "owner": owner, "runtime": runtime}
    runtime.dispose()
    owner.dispose()


def test_without_tenant_context_runtime_sees_no_us_operations(fixture):
    with fixture["runtime"].begin() as conn:
        assert conn.execute(text("SELECT id FROM us_lacey_operations")).fetchall() == []


def test_company_a_cannot_read_company_b_operation(fixture):
    with fixture["runtime"].begin() as conn:
        _tenant(conn, fixture["org_a"])
        rows = conn.execute(
            text("SELECT id, organization_id FROM us_lacey_operations ORDER BY id")
        ).fetchall()
    assert rows
    assert {row[1] for row in rows} == {fixture["org_a"]}
    assert fixture["operation_b"] not in {row[0] for row in rows}


def test_company_a_cannot_insert_operation_for_company_b(fixture):
    with pytest.raises(DBAPIError):
        with fixture["runtime"].begin() as conn:
            _tenant(conn, fixture["org_a"])
            conn.execute(
                text("""
                    INSERT INTO us_lacey_operations (
                        organization_id,
                        client_reference,
                        status,
                        document_count,
                        merchandise_line_count
                    ) VALUES (
                        :other_org,
                        'CROSS-TENANT-DENIED',
                        'NEW',
                        0,
                        1
                    )
                """),
                {"other_org": fixture["org_b"]},
            )


def test_company_a_cannot_update_company_b_operation(fixture):
    with fixture["runtime"].begin() as conn:
        _tenant(conn, fixture["org_a"])
        result = conn.execute(
            text("""
                UPDATE us_lacey_operations
                SET review_result = 'SHOULD_NOT_WRITE'
                WHERE id = :operation_b
            """),
            {"operation_b": fixture["operation_b"]},
        )
        assert result.rowcount == 0
