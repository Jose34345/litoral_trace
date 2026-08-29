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
    reason="Assurance supplier RLS acceptance requires isolated runtime and owner URLs.",
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
                :id, :name, :slug, :tax_id, 'pro', 'Assurance supplier RLS acceptance', true
            )
        """),
        {
            "id": org_id,
            "name": f"Supplier RLS {label.upper()} {suffix}",
            "slug": f"supplier-rls-{label}-{suffix}",
            "tax_id": f"33-{7 if label == 'a' else 8}{suffix[:8]}",
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
        for index, label in enumerate(("a", "b"), start=1):
            org_id = _create_org(conn, label=label, suffix=suffix)
            values[f"org_{label}"] = org_id
            row_id = conn.execute(
                text("""
                    INSERT INTO assurance_suppliers (
                        organization_id, cuit, display_name, normalized_name, status
                    ) VALUES (
                        :org, :cuit, :name, :normalized, 'AUTO_CREATED'
                    ) RETURNING id
                """),
                {
                    "org": org_id,
                    "cuit": f"30708323{10 + index:03d}",
                    "name": f"Proveedor {label.upper()}",
                    "normalized": f"proveedor {label}",
                },
            ).scalar_one()
            values[f"supplier_{label}"] = row_id
    yield {**values, "owner": owner, "runtime": runtime}
    runtime.dispose()
    owner.dispose()


def test_no_tenant_context_hides_assurance_suppliers(fixture):
    with fixture["runtime"].begin() as conn:
        assert conn.execute(text("SELECT id FROM assurance_suppliers")).fetchall() == []


def test_assurance_suppliers_are_tenant_scoped(fixture):
    with fixture["runtime"].begin() as conn:
        _tenant(conn, fixture["org_a"])
        rows = conn.execute(text("SELECT id, organization_id FROM assurance_suppliers")).fetchall()
    assert rows
    assert {row[1] for row in rows} == {fixture["org_a"]}
    assert fixture["supplier_b"] not in {row[0] for row in rows}


def test_cross_tenant_assurance_supplier_insert_is_denied(fixture):
    with pytest.raises(DBAPIError):
        with fixture["runtime"].begin() as conn:
            _tenant(conn, fixture["org_a"])
            conn.execute(
                text("""
                    INSERT INTO assurance_suppliers (
                        organization_id, cuit, display_name, normalized_name, status
                    ) VALUES (
                        :org, '30708323999', 'Cross tenant', 'cross tenant', 'AUTO_CREATED'
                    )
                """),
                {"org": fixture["org_b"]},
            )
