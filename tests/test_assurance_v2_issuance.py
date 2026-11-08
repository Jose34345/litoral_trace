"""Isolated issue-transaction acceptance, NOT a substitute for 13-PDF E2E."""
from __future__ import annotations

from dataclasses import replace

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

from litoral_trace.db.models import UsLaceyOperationEvent
from litoral_trace.us_lacey.assurance_v2_issuance import (
    StaleAssuranceIssue, issue_locked_assurance_package,
)
from litoral_trace.us_lacey.exporters.assurance_v2_package import PostgresOperationEventPackageStore
from test_assurance_v2_core_memory import session, org_and_actor, supplier_product, shipment
from test_assurance_v2_outputs_core import make_case


def _fixture_case(org, op, revision, actor):
    """Explicitly synthetic authorized case, only to test DB transaction behavior."""
    base = replace(
        make_case(organization_id=org.id, approved=False),
        organization_id=org.id,
        operation_id=str(op.public_id),
        source_set_fingerprint=revision.source_set_fingerprint,
        current_source_set_fingerprint=revision.source_set_fingerprint,
        source_set_revision=revision.id,
        current_source_set_revision=revision.id,
    )
    return replace(base, export_authorization={
        "id": "explicit-synthetic-authorization",
        "actor_id": f"user:{actor.id}",
        "authorized_at": "2026-10-09T15:00:00+00:00",
        "status": "AUTHORIZED",
        "source_set_fingerprint": revision.source_set_fingerprint,
        "case_fingerprint": base.authority_fingerprint(),
    })


def _prepare(session):
    org, actor = org_and_actor(session, "IssueAtomic")
    _, product = supplier_product(session, org.id, supplier_key="MID:ATOMIC-ISSUER")
    op, _, _, rev, _ = shipment(
        session, org.id, product.id,
        reference="ATOMIC-ISSUER-SHIP", source_value="Quercus alba",
    )
    session.commit()
    factory = sessionmaker(bind=session.get_bind(), expire_on_commit=False)
    case = _fixture_case(org, op, rev, actor)
    return org, actor, op, rev, factory, case


def _issue(org, actor, op, revision, factory, case, *, expected_fingerprint=None, store=None):
    return issue_locked_assurance_package(
        organization_id=org.id, operation_public_id=str(op.public_id),
        authenticated_user_id=actor.id,
        expected_source_set_fingerprint=(
            expected_fingerprint if expected_fingerprint is not None
            else revision.source_set_fingerprint
        ),
        expected_case_fingerprint=case.authority_fingerprint(),
        session_factory=factory,
        case_loader=lambda *_args: case,
        package_store=store,
    )


def test_current_source_and_authorized_actor_required(session):
    org, actor, op, rev, factory, case = _prepare(session)
    with pytest.raises(StaleAssuranceIssue, match="Source set changed"):
        _issue(org, actor, op, rev, factory, case, expected_fingerprint="stale" * 12 + "aaaa")
    with pytest.raises(PermissionError, match="explicit authorization"):
        _issue(org, actor, op, rev, factory, replace(case, export_authorization={}))
    assert session.scalar(select(func.count(UsLaceyOperationEvent.id))) == 0


def test_package_write_participates_in_atomic_commit_or_rollback(session):
    org, actor, op, rev, factory, case = _prepare(session)
    class FailingStore(PostgresOperationEventPackageStore):
        def save_in_transaction(self, db, **kwargs):
            super().save_in_transaction(db, **kwargs)
            raise RuntimeError("synthetic_failure_after_package_insert")

    with pytest.raises(RuntimeError, match="synthetic_failure"):
        _issue(org, actor, op, rev, factory, case, store=FailingStore(factory))
    assert session.scalar(select(func.count(UsLaceyOperationEvent.id))) == 0

    receipt = _issue(org, actor, op, rev, factory, case)
    assert receipt["status"] == "FILING_READY"
    assert receipt["submitted_to_agency"] is False
    assert len(str(receipt["package_fingerprint"])) == 64
    assert session.scalar(select(func.count(UsLaceyOperationEvent.id))) == 1
    loaded = PostgresOperationEventPackageStore(factory).load(
        organization_id=org.id, operation_id=str(op.public_id),
        fingerprint=receipt["package_fingerprint"],
    )
    assert loaded is not None and loaded.snapshot["case"]["export_authorization"]["status"] == "AUTHORIZED"
