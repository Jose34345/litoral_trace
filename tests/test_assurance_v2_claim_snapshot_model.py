"""The source claim sidecar is immutable and tied to one source revision."""
from __future__ import annotations

from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from litoral_trace.db.base import Base
from litoral_trace.db.models import UsLaceyAssuranceV2ClaimSnapshot


def test_claim_snapshot_has_unique_revision_binding_and_json_payload():
    table = UsLaceyAssuranceV2ClaimSnapshot.__table__
    unique_sets = {tuple(constraint.columns.keys()) for constraint in table.constraints
                   if constraint.__class__.__name__ == "UniqueConstraint"}
    assert ("organization_id", "operation_id", "source_set_revision_id",
            "source_set_fingerprint") in unique_sets
    assert {"snapshot_json", "status", "document_count", "claim_count"} <= set(table.columns.keys())
    assert {"operation_id", "source_set_revision_id", "organization_id"} <= set(table.columns.keys())


def test_claim_snapshot_cannot_be_inserted_twice_for_same_source_set():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    # SQLite is sufficient for the table's uniqueness; PostgreSQL RLS is tested separately.
    table = UsLaceyAssuranceV2ClaimSnapshot.__table__
    table.create(engine, checkfirst=True)
    with Session(engine) as session:
        values = dict(organization_id=11, operation_id=12, source_set_revision_id=13,
                      source_set_fingerprint="a" * 64, status="CANDIDATE_ONLY",
                      document_count=1, claim_count=1, snapshot_json={"schema_version": "assurance.claim.snapshot.v1"})
        session.add(UsLaceyAssuranceV2ClaimSnapshot(**values))
        session.commit()
        session.add(UsLaceyAssuranceV2ClaimSnapshot(**values))
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
        else:
            raise AssertionError("duplicate claim snapshot was accepted")
