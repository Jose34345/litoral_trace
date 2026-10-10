"""PostgreSQL RLS acceptance for frozen Assurance V2 package persistence.

Requires ENABLE_POSTGRES_TESTS and a dedicated migrated TEST_POSTGRES_DATABASE_URL
plus owner TEST_POSTGRES_MIGRATION_DATABASE_URL. Never use production credentials.
"""
from __future__ import annotations

from dataclasses import replace
import os
import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("ENABLE_POSTGRES_TESTS", "").lower() not in {"1", "true", "yes", "on"}
    or not os.getenv("TEST_POSTGRES_DATABASE_URL")
    or not (os.getenv("TEST_POSTGRES_MIGRATION_DATABASE_URL") or os.getenv("MIGRATION_DATABASE_URL")),
    reason="Dedicated migrated PostgreSQL/RLS fixture not configured.",
)

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from litoral_trace.us_lacey.exporters.assurance_v2_package import (
    PostgresOperationEventPackageStore, issue_assurance_package,
)
from test_assurance_v2_outputs_core import NOW, authorize, make_case
from test_us_lacey_operation_audit_postgres import fixture as audit_fixture


def test_package_store_is_immutable_and_tenant_isolated_by_rls(audit_fixture):
    engine = audit_fixture["runtime"]
    org_a = audit_fixture["org_a"]
    org_b = audit_fixture["org_b"]
    operation_a = audit_fixture["operation_a"]

    with audit_fixture["owner"].connect() as owner:
        operation_public_id = str(owner.execute(
            text("SELECT public_id FROM us_lacey_operations WHERE id=:id AND organization_id=:org"),
            {"id": operation_a, "org": org_a},
        ).scalar_one())

    case = make_case(organization_id=org_a, approved=False)
    case = authorize(replace(case, operation_id=operation_public_id))
    package = issue_assurance_package(case, generated_at=NOW)
    store = PostgresOperationEventPackageStore(session_factory=lambda: Session(engine))
    store.save(organization_id=org_a, operation_id=operation_public_id, package=package)
    store.save(organization_id=org_a, operation_id=operation_public_id, package=package)
    recovered = store.load(organization_id=org_a, operation_id=operation_public_id,
                           fingerprint=package.fingerprint)
    assert recovered is not None
    assert recovered.artifact("lawgs_xml") == package.artifact("lawgs_xml")
    assert recovered.artifact("lacey_excel") == package.artifact("lacey_excel")
    assert store.load(organization_id=org_b, operation_id=operation_public_id,
                      fingerprint=package.fingerprint) is None

    # Assert DB RLS too: even a query without the explicit app filter cannot
    # see A's persisted package while tenant B's GUC is active.
    with engine.begin() as conn:
        conn.execute(text("SELECT set_config('app.current_organization_id', :org, true)"),
                     {"org": str(org_b)})
        rows = conn.execute(
            text("SELECT id FROM us_lacey_operation_events WHERE event_key=:key"),
            {"key": "assurance_v2:" + package.fingerprint},
        ).fetchall()
        assert rows == []
