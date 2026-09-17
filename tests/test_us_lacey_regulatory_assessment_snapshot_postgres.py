from __future__ import annotations

from datetime import datetime, timezone
import os

import pytest
from sqlalchemy import create_engine, inspect, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.models import (
    UsLaceyRegulatoryAssessmentSnapshot,
    UsLaceySourceSetRevision,
)
from litoral_trace.us_lacey.regulatory.rules import RULESET_VERSION
from litoral_trace.us_lacey.regulatory_assessment_snapshot import (
    mark_regulatory_assessment_snapshots_stale,
)
from litoral_trace.us_lacey.source_sets import seal_current_source_set
from tests.us_lacey_engine2_postgres import (
    add_test_document,
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


def _snapshot(
    *,
    organization_id: int,
    operation_id: int,
    revision: UsLaceySourceSetRevision,
    status: str = "CURRENT",
) -> UsLaceyRegulatoryAssessmentSnapshot:
    return UsLaceyRegulatoryAssessmentSnapshot(
        organization_id=organization_id,
        operation_id=operation_id,
        source_set_revision_id=revision.id,
        generation=revision.generation,
        source_set_fingerprint=revision.source_set_fingerprint,
        ruleset_version=RULESET_VERSION,
        input_fingerprint=("a" if status == "CURRENT" else "b") * 64,
        status=status,
        assessment_count=2,
        indeterminate_count=1,
        payload_json={
            "schema_version": "regulatory-assessment-snapshot-v1",
            "ruleset_version": RULESET_VERSION,
            "assessments": [
                {"rule_id": "DE_MINIMIS", "status": "INDETERMINATE"},
                {"rule_id": "SPECIAL_COMPOSITE", "status": "FAIL"},
            ],
        },
        finalized_at=datetime.now(timezone.utc),
    )


def _require_schema(engine) -> None:
    if "us_lacey_regulatory_assessment_snapshots" not in inspect(engine).get_table_names():
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_050")


def test_regulatory_assessment_model_exposes_tenant_ruleset_contract():
    table = UsLaceyRegulatoryAssessmentSnapshot.__table__
    assert table.name == "us_lacey_regulatory_assessment_snapshots"
    assert {
        "organization_id",
        "operation_id",
        "source_set_revision_id",
        "generation",
        "source_set_fingerprint",
        "ruleset_version",
        "input_fingerprint",
        "status",
        "payload_json",
    } <= set(table.c.keys())
    assert any(
        constraint.name == "fk_lacey_reg_assessment_revision_tenant"
        for constraint in table.foreign_key_constraints
    )
    assert any(
        constraint.name == "uq_lacey_reg_assessment_revision_ruleset"
        for constraint in table.constraints
    )


def test_regulatory_assessment_persists_once_per_revision_ruleset_and_rejects_cross_tenant_fk(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    _require_schema(engine2_postgres_engine)
    org_a, operation_a, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory, content=b"reg-a"
    )
    org_b, operation_b, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory, content=b"reg-b"
    )
    revision_a = seal_current_source_set(
        organization_id=org_a,
        operation_id=operation_a,
        session_factory=engine2_postgres_session_factory,
    )
    revision_b = seal_current_source_set(
        organization_id=org_b,
        operation_id=operation_b,
        session_factory=engine2_postgres_session_factory,
    )

    session = tenant_session(engine2_postgres_session_factory, org_a)
    session.add(_snapshot(organization_id=org_a, operation_id=operation_a, revision=revision_a))
    session.commit()

    session.add(_snapshot(organization_id=org_a, operation_id=operation_a, revision=revision_a))
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()

    cross_tenant = UsLaceyRegulatoryAssessmentSnapshot(
        organization_id=org_a,
        operation_id=operation_a,
        source_set_revision_id=revision_b.id,
        generation=revision_b.generation,
        source_set_fingerprint=revision_b.source_set_fingerprint,
        ruleset_version=RULESET_VERSION,
        input_fingerprint="c" * 64,
        status="CURRENT",
        assessment_count=1,
        indeterminate_count=1,
        payload_json={"schema_version": "regulatory-assessment-snapshot-v1", "assessments": []},
    )
    session.add(cross_tenant)
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()
    session.close()


def test_runtime_rls_hides_other_tenant_regulatory_assessment(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    _require_schema(engine2_postgres_engine)
    org_a, operation_a, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory, content=b"reg-rls-a"
    )
    org_b, operation_b, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory, content=b"reg-rls-b"
    )
    revision_a = seal_current_source_set(
        organization_id=org_a,
        operation_id=operation_a,
        session_factory=engine2_postgres_session_factory,
    )
    revision_b = seal_current_source_set(
        organization_id=org_b,
        operation_id=operation_b,
        session_factory=engine2_postgres_session_factory,
    )
    for org, operation, revision in (
        (org_a, operation_a, revision_a),
        (org_b, operation_b, revision_b),
    ):
        session = tenant_session(engine2_postgres_session_factory, org)
        session.add(_snapshot(organization_id=org, operation_id=operation, revision=revision))
        session.commit()
        session.close()

    runtime_url = os.environ.get("TEST_POSTGRES_DATABASE_URL")
    assert runtime_url, "TEST_POSTGRES_DATABASE_URL is required for regulatory-assessment RLS acceptance"
    runtime_engine = create_engine(normalize_database_url(runtime_url), pool_pre_ping=True)
    RuntimeFactory = sessionmaker(bind=runtime_engine, expire_on_commit=False)
    try:
        session = tenant_session(RuntimeFactory, org_a)
        visible = session.scalars(
            select(UsLaceyRegulatoryAssessmentSnapshot).order_by(
                UsLaceyRegulatoryAssessmentSnapshot.id
            )
        ).all()
        assert len(visible) == 1
        assert visible[0].organization_id == org_a
        cross = session.execute(
            update(UsLaceyRegulatoryAssessmentSnapshot)
            .where(UsLaceyRegulatoryAssessmentSnapshot.organization_id == org_b)
            .values(status="STALE")
        )
        assert cross.rowcount == 0
        session.commit()
        session.close()
    finally:
        runtime_engine.dispose()


def test_mark_regulatory_assessment_stale_preserves_payload(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    _require_schema(engine2_postgres_engine)
    org, operation, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory, content=b"reg-stale"
    )
    revision = seal_current_source_set(
        organization_id=org,
        operation_id=operation,
        session_factory=engine2_postgres_session_factory,
    )
    session = tenant_session(engine2_postgres_session_factory, org)
    snapshot = _snapshot(organization_id=org, operation_id=operation, revision=revision)
    session.add(snapshot)
    session.commit()
    snapshot_id = snapshot.id
    original_payload = dict(snapshot.payload_json)

    changed = mark_regulatory_assessment_snapshots_stale(
        session,
        organization_id=org,
        operation_id=operation,
    )
    session.commit()
    assert changed == 1
    persisted = session.get(UsLaceyRegulatoryAssessmentSnapshot, snapshot_id)
    assert persisted.status == "STALE"
    assert persisted.payload_json == original_payload
    session.close()


def test_new_source_set_generation_marks_prior_regulatory_assessment_stale(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    _require_schema(engine2_postgres_engine)
    org, operation, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory, content=b"reg-generation-one"
    )
    first_revision = seal_current_source_set(
        organization_id=org,
        operation_id=operation,
        session_factory=engine2_postgres_session_factory,
    )
    session = tenant_session(engine2_postgres_session_factory, org)
    snapshot = _snapshot(organization_id=org, operation_id=operation, revision=first_revision)
    session.add(snapshot)
    session.commit()
    snapshot_id = snapshot.id
    original_payload = dict(snapshot.payload_json)
    session.close()

    add_test_document(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation,
        role="UNKNOWN",
        filename="reg-generation-two.pdf",
        content=b"reg-generation-two",
        is_current=True,
    )
    second_revision = seal_current_source_set(
        organization_id=org,
        operation_id=operation,
        session_factory=engine2_postgres_session_factory,
    )

    assert second_revision.id != first_revision.id
    assert second_revision.generation == first_revision.generation + 1
    session = tenant_session(engine2_postgres_session_factory, org)
    prior = session.get(UsLaceyRegulatoryAssessmentSnapshot, snapshot_id)
    assert prior.status == "STALE"
    assert prior.payload_json == original_payload
    current_revisions = session.scalars(
        select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == org,
            UsLaceySourceSetRevision.operation_id == operation,
            UsLaceySourceSetRevision.is_current.is_(True),
        )
    ).all()
    assert [revision.id for revision in current_revisions] == [second_revision.id]
    session.close()
