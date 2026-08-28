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
    reason="Assurance reconciliation RLS acceptance requires isolated runtime and owner URLs.",
)


def _engine(url: str):
    return create_engine(normalize_database_url(url), pool_pre_ping=True)


def _tenant(conn, organization_id: int) -> None:
    conn.execute(
        text("SELECT set_config('app.current_organization_id', :organization_id, true)"),
        {"organization_id": str(organization_id)},
    )


@pytest.fixture(scope="module")
def fixture():
    suffix = uuid4().hex[:10]
    owner = _engine(OWNER_URL)
    runtime = _engine(RUNTIME_URL)
    values: dict[str, int] = {}
    with owner.begin() as conn:
        for label in ("a", "b"):
            org_id = conn.execute(
                text("""
                    INSERT INTO organizations (name, slug, tax_id, tier, description, is_active)
                    VALUES (:name, :slug, :tax_id, 'pro', 'Reconciliation RLS acceptance', true)
                    RETURNING id
                """),
                {
                    "name": f"Reconciliation RLS {label.upper()} {suffix}",
                    "slug": f"reconciliation-rls-{label}-{suffix}",
                    "tax_id": f"31-{7 if label == 'a' else 8}{suffix[:8]}",
                },
            ).scalar_one()
            values[f"org_{label}"] = org_id
            _tenant(conn, org_id)
            issue_id = conn.execute(
                text("""
                    INSERT INTO reconciliation_issues (
                        organization_id, operation_reference, fingerprint, rule_code,
                        severity, status, field_name, left_source, right_source, explanation
                    ) VALUES (
                        :org, :ref, :fingerprint, 'TEST_RLS', 'BLOCKING', 'OPEN',
                        'quantity', 'invoice.quantity', 'shipment.quantity', 'Fixture discrepancy'
                    ) RETURNING id
                """),
                {
                    "org": org_id,
                    "ref": f"OP-{label}-{suffix}",
                    "fingerprint": ("1" if label == "a" else "2") * 64,
                },
            ).scalar_one()
            values[f"issue_{label}"] = issue_id
    yield {**values, "owner": owner, "runtime": runtime}
    runtime.dispose()
    owner.dispose()


def test_no_tenant_context_hides_reconciliation_rows(fixture):
    with fixture["runtime"].begin() as conn:
        assert conn.execute(text("SELECT id FROM reconciliation_issues")).fetchall() == []


def test_reconciliation_rows_are_tenant_scoped(fixture):
    with fixture["runtime"].begin() as conn:
        _tenant(conn, fixture["org_a"])
        rows = conn.execute(text("SELECT id, organization_id FROM reconciliation_issues")).fetchall()
    assert rows
    assert {row[1] for row in rows} == {fixture["org_a"]}
    assert fixture["issue_b"] not in {row[0] for row in rows}


def test_cross_tenant_reconciliation_insert_is_denied(fixture):
    with pytest.raises(DBAPIError):
        with fixture["runtime"].begin() as conn:
            _tenant(conn, fixture["org_a"])
            conn.execute(
                text("""
                    INSERT INTO reconciliation_issues (
                        organization_id, operation_reference, fingerprint, rule_code,
                        severity, status, field_name, left_source, explanation
                    ) VALUES (
                        :org, 'CROSS-TENANT', :fingerprint, 'TEST_RLS', 'BLOCKING',
                        'OPEN', 'quantity', 'fixture', 'Must be rejected'
                    )
                """),
                {"org": fixture["org_b"], "fingerprint": "9" * 64},
            )
