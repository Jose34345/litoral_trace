from __future__ import annotations

from datetime import datetime, timezone
import os

import pytest
from sqlalchemy import create_engine, inspect, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.models import (
    UsLaceyProductIntelligenceSnapshot,
    UsLaceySourceSetRevision,
)
from litoral_trace.us_lacey.product_intelligence_snapshot import (
    mark_product_intelligence_snapshots_stale,
)
from litoral_trace.us_lacey.source_sets import seal_current_source_set
from tests.us_lacey_engine2_postgres import (
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


def _snapshot(*, organization_id: int, operation_id: int, revision: UsLaceySourceSetRevision, status: str = "READY"):
    return UsLaceyProductIntelligenceSnapshot(
        organization_id=organization_id,
        operation_id=operation_id,
        source_set_revision_id=revision.id,
        generation=revision.generation,
        source_set_fingerprint=revision.source_set_fingerprint,
        status=status,
        document_count=1,
        eligible_document_count=1,
        recognized_bom_table_count=1,
        unique_sku_count=1,
        component_count=1,
        material_count=1,
        issue_count=0,
        payload_json={"schema_version": "product-intelligence-snapshot-v1", "sources": []},
        finalized_at=datetime.now(timezone.utc),
    )


def test_product_intelligence_model_exposes_tenant_source_set_contract():
    table = UsLaceyProductIntelligenceSnapshot.__table__
    assert table.name == "us_lacey_product_intelligence_snapshots"
    assert {
        "organization_id",
        "operation_id",
        "source_set_revision_id",
        "generation",
        "source_set_fingerprint",
        "status",
        "payload_json",
    } <= set(table.c.keys())
    assert any(
        constraint.name == "fk_lacey_pi_snapshot_revision_tenant"
        for constraint in table.foreign_key_constraints
    )
    assert any(
        constraint.name == "uq_lacey_pi_snapshot_revision"
        for constraint in table.constraints
    )


def test_snapshot_persists_once_per_revision_and_cross_tenant_fk_fails(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    if "us_lacey_product_intelligence_snapshots" not in inspect(engine2_postgres_engine).get_table_names():
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_049")

    org_a, operation_a, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory, content=b"pi-a"
    )
    org_b, operation_b, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory, content=b"pi-b"
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
    first = _snapshot(
        organization_id=org_a,
        operation_id=operation_a,
        revision=revision_a,
    )
    session.add(first)
    session.commit()

    session.add(
        _snapshot(
            organization_id=org_a,
            operation_id=operation_a,
            revision=revision_a,
            status="PARTIAL",
        )
    )
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()

    cross_tenant = UsLaceyProductIntelligenceSnapshot(
        organization_id=org_a,
        operation_id=operation_a,
        source_set_revision_id=revision_b.id,
        generation=revision_b.generation,
        source_set_fingerprint=revision_b.source_set_fingerprint,
        status="NOT_APPLICABLE",
        document_count=0,
        eligible_document_count=0,
        recognized_bom_table_count=0,
        unique_sku_count=0,
        component_count=0,
        material_count=0,
        issue_count=0,
        payload_json={"schema_version": "product-intelligence-snapshot-v1", "sources": []},
    )
    session.add(cross_tenant)
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()
    session.close()


def test_runtime_rls_hides_other_tenant_product_intelligence_snapshot(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    if "us_lacey_product_intelligence_snapshots" not in inspect(engine2_postgres_engine).get_table_names():
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_049")

    org_a, operation_a, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory, content=b"pi-rls-a"
    )
    org_b, operation_b, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory, content=b"pi-rls-b"
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
    assert runtime_url, "TEST_POSTGRES_DATABASE_URL is required for Product Intelligence RLS acceptance"
    runtime_engine = create_engine(normalize_database_url(runtime_url), pool_pre_ping=True)
    RuntimeFactory = sessionmaker(bind=runtime_engine, expire_on_commit=False)
    try:
        session = tenant_session(RuntimeFactory, org_a)
        visible = session.scalars(
            select(UsLaceyProductIntelligenceSnapshot).order_by(UsLaceyProductIntelligenceSnapshot.id)
        ).all()
        assert len(visible) == 1
        assert visible[0].organization_id == org_a
        cross = session.execute(
            update(UsLaceyProductIntelligenceSnapshot)
            .where(UsLaceyProductIntelligenceSnapshot.organization_id == org_b)
            .values(status="STALE")
        )
        assert cross.rowcount == 0
        session.commit()
        session.close()
    finally:
        runtime_engine.dispose()


def test_mark_stale_changes_metadata_without_mutating_payload(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    if "us_lacey_product_intelligence_snapshots" not in inspect(engine2_postgres_engine).get_table_names():
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_049")

    org, operation, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory, content=b"pi-stale"
    )
    revision = seal_current_source_set(
        organization_id=org,
        operation_id=operation,
        session_factory=engine2_postgres_session_factory,
    )
    session = tenant_session(engine2_postgres_session_factory, org)
    snapshot = _snapshot(organization_id=org, operation_id=operation, revision=revision)
    snapshot.payload_json = {
        "schema_version": "product-intelligence-snapshot-v1",
        "sources": [{"document_id": "immutable"}],
    }
    session.add(snapshot)
    session.commit()
    snapshot_id = snapshot.id
    original_payload = snapshot.payload_json

    changed = mark_product_intelligence_snapshots_stale(
        session,
        organization_id=org,
        operation_id=operation,
    )
    session.commit()
    assert changed == 1
    persisted = session.get(UsLaceyProductIntelligenceSnapshot, snapshot_id)
    assert persisted.status == "STALE"
    assert persisted.payload_json == original_payload
    session.close()
