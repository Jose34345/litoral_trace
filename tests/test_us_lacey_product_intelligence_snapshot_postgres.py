from __future__ import annotations

from datetime import datetime, timezone
import os

import pytest
from sqlalchemy import create_engine, inspect, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.models import (
    UsLaceyProductIntelligenceSnapshot,
    UsLaceySourceSetRevision,
)
from litoral_trace.us_lacey.product_intelligence_snapshot import (
    build_product_intelligence_snapshot,
    mark_product_intelligence_snapshots_stale,
)
from litoral_trace.us_lacey.source_sets import SourceSetClaim, seal_current_source_set
from tests.us_lacey_engine2_postgres import (
    FakeVault,
    add_test_document,
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


def _claim_revision(factory, *, organization_id: int, revision: UsLaceySourceSetRevision) -> SourceSetClaim:
    session = tenant_session(factory, organization_id)
    try:
        claimed_at = session.execute(
            update(UsLaceySourceSetRevision)
            .where(
                UsLaceySourceSetRevision.organization_id == organization_id,
                UsLaceySourceSetRevision.id == revision.id,
                UsLaceySourceSetRevision.is_current.is_(True),
            )
            .values(status="FINALIZING", claimed_at=datetime.now(timezone.utc))
            .returning(UsLaceySourceSetRevision.claimed_at)
        ).scalar_one()
        session.commit()
    finally:
        session.close()
    return SourceSetClaim(
        revision_id=revision.id,
        generation=revision.generation,
        fingerprint=revision.source_set_fingerprint,
        claimed=True,
        reason="CLAIMED",
        claimed_at=claimed_at,
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


def test_builder_reinstalls_runtime_tenant_context_before_refresh(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    if "us_lacey_product_intelligence_snapshots" not in inspect(engine2_postgres_engine).get_table_names():
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_049")

    org, operation, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"pi-builder-runtime-rls",
    )
    revision = seal_current_source_set(
        organization_id=org,
        operation_id=operation,
        session_factory=engine2_postgres_session_factory,
    )
    claim = _claim_revision(
        engine2_postgres_session_factory,
        organization_id=org,
        revision=revision,
    )

    runtime_url = os.environ.get("TEST_POSTGRES_DATABASE_URL")
    assert runtime_url, "TEST_POSTGRES_DATABASE_URL is required for Product Intelligence RLS acceptance"
    runtime_engine = create_engine(normalize_database_url(runtime_url), pool_pre_ping=True)
    RuntimeFactory = sessionmaker(bind=runtime_engine, autoflush=False, expire_on_commit=False)
    try:
        snapshot = build_product_intelligence_snapshot(
            organization_id=org,
            operation_id=operation,
            claim=claim,
            session_factory=RuntimeFactory,
            vault_service=FakeVault(b""),
        )
        assert snapshot is not None
        assert snapshot.organization_id == org
        assert snapshot.source_set_revision_id == revision.id
        assert snapshot.status == "FAILED"
        assert snapshot.eligible_document_count == 1
        assert snapshot.issue_count == 1
    finally:
        runtime_engine.dispose()


def test_builder_reinstalls_runtime_tenant_context_after_integrity_error_rollback(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    if "us_lacey_product_intelligence_snapshots" not in inspect(engine2_postgres_engine).get_table_names():
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_049")

    org, operation, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"pi-builder-runtime-race",
    )
    revision = seal_current_source_set(
        organization_id=org,
        operation_id=operation,
        session_factory=engine2_postgres_session_factory,
    )
    claim = _claim_revision(
        engine2_postgres_session_factory,
        organization_id=org,
        revision=revision,
    )

    runtime_url = os.environ.get("TEST_POSTGRES_DATABASE_URL")
    assert runtime_url, "TEST_POSTGRES_DATABASE_URL is required for Product Intelligence RLS acceptance"
    runtime_engine = create_engine(normalize_database_url(runtime_url), pool_pre_ping=True)
    injected = [False]
    root_factory = engine2_postgres_session_factory

    class RacingSession(Session):
        def commit(self):
            pending = next(
                (item for item in self.new if isinstance(item, UsLaceyProductIntelligenceSnapshot)),
                None,
            )
            if pending is not None and not injected[0]:
                race = tenant_session(root_factory, int(pending.organization_id))
                try:
                    race.add(
                        UsLaceyProductIntelligenceSnapshot(
                            organization_id=pending.organization_id,
                            operation_id=pending.operation_id,
                            source_set_revision_id=pending.source_set_revision_id,
                            generation=pending.generation,
                            source_set_fingerprint=pending.source_set_fingerprint,
                            status=pending.status,
                            document_count=pending.document_count,
                            eligible_document_count=pending.eligible_document_count,
                            recognized_bom_table_count=pending.recognized_bom_table_count,
                            unique_sku_count=pending.unique_sku_count,
                            component_count=pending.component_count,
                            material_count=pending.material_count,
                            issue_count=pending.issue_count,
                            payload_json=dict(pending.payload_json or {}),
                            finalized_at=pending.finalized_at,
                        )
                    )
                    race.commit()
                    injected[0] = True
                finally:
                    race.close()
            return super().commit()

    RuntimeFactory = sessionmaker(
        bind=runtime_engine,
        class_=RacingSession,
        autoflush=False,
        expire_on_commit=False,
    )
    try:
        snapshot = build_product_intelligence_snapshot(
            organization_id=org,
            operation_id=operation,
            claim=claim,
            session_factory=RuntimeFactory,
            vault_service=FakeVault(b""),
        )
        assert injected[0] is True
        assert snapshot is not None
        assert snapshot.organization_id == org
        assert snapshot.source_set_revision_id == revision.id
        assert snapshot.status == "FAILED"
        assert snapshot.eligible_document_count == 1
        assert snapshot.issue_count == 1
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


def test_new_source_set_generation_marks_prior_product_intelligence_snapshot_stale(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    if "us_lacey_product_intelligence_snapshots" not in inspect(engine2_postgres_engine).get_table_names():
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_049")

    org, operation, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory, content=b"pi-generation-one"
    )
    first_revision = seal_current_source_set(
        organization_id=org,
        operation_id=operation,
        session_factory=engine2_postgres_session_factory,
    )
    session = tenant_session(engine2_postgres_session_factory, org)
    snapshot = _snapshot(organization_id=org, operation_id=operation, revision=first_revision)
    snapshot.payload_json = {
        "schema_version": "product-intelligence-snapshot-v1",
        "sources": [{"document_id": "immutable-generation-one"}],
    }
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
        filename="generation-two.pdf",
        content=b"generation-two",
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
    prior = session.get(UsLaceyProductIntelligenceSnapshot, snapshot_id)
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
