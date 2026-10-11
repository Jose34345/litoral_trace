"""PostgreSQL-only concurrent issuer locking acceptance.

Uses a synthetic already-authorized case *solely* to exercise locking and
idempotent package persistence. Not an HTTP/PDF source-completeness Golden.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import os

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from litoral_trace.us_lacey.assurance_v2_issuance import issue_locked_assurance_package
from test_assurance_v2_core_postgres import pg_case
from test_assurance_v2_outputs_core import make_case

pytestmark = pytest.mark.skipif(
    os.getenv("ENABLE_POSTGRES_TESTS") != "1"
    or not os.getenv("TEST_POSTGRES_DATABASE_URL")
    or not os.getenv("TEST_POSTGRES_MIGRATION_DATABASE_URL"),
    reason="Concurrent issuer acceptance requires isolated PostgreSQL owner/runtime roles",
)


def test_concurrent_issuance_commits_exactly_one_immutable_package(pg_case):
    values, _, runtime = pg_case
    org_id, operation_id, revision_id, user_id = (
        values["org_a"], values["op_a"], values["revision_a"], values["user_a"],
    )
    with runtime.begin() as connection:
        connection.execute(
            text("SELECT set_config('app.current_organization_id', :org, true)"),
            {"org": str(org_id)},
        )
        # This fixture does not include physical documents. Set its source
        # membership to the consistent empty state for an isolated lock test.
        connection.execute(text(
            "UPDATE us_lacey_source_set_revisions SET document_count=0 "
            "WHERE organization_id=:org AND id=:revision"
        ), {"org": org_id, "revision": revision_id})
        public_id = str(connection.execute(text(
            "SELECT public_id FROM us_lacey_operations "
            "WHERE organization_id=:org AND id=:operation"
        ), {"org": org_id, "operation": operation_id}).scalar_one())
        fingerprint = str(connection.execute(text(
            "SELECT source_set_fingerprint FROM us_lacey_source_set_revisions "
            "WHERE organization_id=:org AND id=:revision"
        ), {"org": org_id, "revision": revision_id}).scalar_one())

    base = replace(
        make_case(organization_id=org_id, approved=False),
        operation_id=public_id,
        source_set_fingerprint=fingerprint,
        current_source_set_fingerprint=fingerprint,
        source_set_revision=revision_id,
        current_source_set_revision=revision_id,
        source_set_generation=1,
    )
    case = replace(base, export_authorization={
        "id": "synthetic-issuer-lock-authorization",
        "actor_id": f"user:{user_id}",
        "authorized_at": "2026-10-11T12:00:00+00:00",
        "status": "AUTHORIZED",
        "source_set_fingerprint": fingerprint,
        "case_fingerprint": base.authority_fingerprint(),
    })

    def issue_once():
        return issue_locked_assurance_package(
            organization_id=org_id, operation_public_id=public_id,
            authenticated_user_id=user_id,
            expected_source_set_fingerprint=fingerprint,
            expected_case_fingerprint=case.authority_fingerprint(),
            session_factory=lambda: Session(runtime, expire_on_commit=False),
            case_loader=lambda *_: case,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        jobs = [executor.submit(issue_once) for _ in range(2)]
        receipts = [job.result(timeout=30) for job in jobs]
    assert all(item["status"] == "FILING_READY" for item in receipts)
    assert len({item["package_fingerprint"] for item in receipts}) == 1
    assert all(item["submitted_to_agency"] is False for item in receipts)
    with runtime.begin() as connection:
        connection.execute(
            text("SELECT set_config('app.current_organization_id', :org, true)"),
            {"org": str(org_id)},
        )
        count = connection.execute(text(
            "SELECT count(*) FROM us_lacey_operation_events "
            "WHERE organization_id=:org AND operation_id=:operation "
            "AND event_type='PACKAGE_GENERATED'"
        ), {"org": org_id, "operation": operation_id}).scalar_one()
    assert count == 1
