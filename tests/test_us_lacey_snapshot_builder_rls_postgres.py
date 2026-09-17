from __future__ import annotations

from datetime import datetime, timezone
import os

import pytest
from sqlalchemy import create_engine, inspect, update
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.models import (
    UsLaceyProductIntelligenceSnapshot,
    UsLaceySourceSetRevision,
)
from litoral_trace.us_lacey.product_intelligence_snapshot import (
    build_product_intelligence_snapshot,
)
from litoral_trace.us_lacey.regulatory_assessment_snapshot import (
    build_regulatory_assessment_snapshot,
)
from litoral_trace.us_lacey.source_sets import SourceSetClaim, seal_current_source_set
from tests.us_lacey_engine2_postgres import (
    FakeVault,
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


def _runtime_session_factory():
    runtime_url = os.environ.get("TEST_POSTGRES_DATABASE_URL")
    assert runtime_url, "TEST_POSTGRES_DATABASE_URL is required for snapshot builder RLS acceptance"
    engine = create_engine(normalize_database_url(runtime_url), pool_pre_ping=True)
    return engine, sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


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
            .values(
                status="FINALIZING",
                claimed_at=datetime.now(timezone.utc),
            )
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


def _persist_product_snapshot(
    factory,
    *,
    organization_id: int,
    operation_id: int,
    revision: UsLaceySourceSetRevision,
) -> None:
    session = tenant_session(factory, organization_id)
    try:
        session.add(
            UsLaceyProductIntelligenceSnapshot(
                organization_id=organization_id,
                operation_id=operation_id,
                source_set_revision_id=revision.id,
                generation=revision.generation,
                source_set_fingerprint=revision.source_set_fingerprint,
                status="NOT_APPLICABLE",
                document_count=1,
                eligible_document_count=0,
                recognized_bom_table_count=0,
                unique_sku_count=0,
                component_count=0,
                material_count=0,
                issue_count=0,
                payload_json={
                    "schema_version": "product-intelligence-snapshot-v1",
                    "summary": {
                        "document_count": 1,
                        "eligible_document_count": 0,
                        "recognized_bom_table_count": 0,
                        "unique_sku_count": 0,
                        "component_count": 0,
                        "material_count": 0,
                        "issue_count": 0,
                    },
                    "sources": [],
                    "issues": [],
                },
                finalized_at=datetime.now(timezone.utc),
            )
        )
        session.commit()
    finally:
        session.close()


def test_product_intelligence_builder_refreshes_under_force_rls_after_commit(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    if "us_lacey_product_intelligence_snapshots" not in inspect(engine2_postgres_engine).get_table_names():
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_049")

    org, operation, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"pi-builder-rls",
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

    runtime_engine, RuntimeFactory = _runtime_session_factory()
    try:
        snapshot = build_product_intelligence_snapshot(
            organization_id=org,
            operation_id=operation,
            claim=claim,
            session_factory=RuntimeFactory,
            vault_service=FakeVault(b""),
        )
    finally:
        runtime_engine.dispose()

    assert snapshot is not None
    assert snapshot.organization_id == org
    assert snapshot.source_set_revision_id == revision.id
    assert snapshot.status == "NOT_APPLICABLE"


def test_regulatory_assessment_builder_refreshes_under_force_rls_after_commit(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    if "us_lacey_regulatory_assessment_snapshots" not in inspect(engine2_postgres_engine).get_table_names():
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_050")

    org, operation, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"reg-builder-rls",
    )
    revision = seal_current_source_set(
        organization_id=org,
        operation_id=operation,
        session_factory=engine2_postgres_session_factory,
    )
    _persist_product_snapshot(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation,
        revision=revision,
    )
    claim = _claim_revision(
        engine2_postgres_session_factory,
        organization_id=org,
        revision=revision,
    )

    runtime_engine, RuntimeFactory = _runtime_session_factory()
    try:
        snapshot = build_regulatory_assessment_snapshot(
            organization_id=org,
            operation_id=operation,
            claim=claim,
            session_factory=RuntimeFactory,
        )
    finally:
        runtime_engine.dispose()

    assert snapshot is not None
    assert snapshot.organization_id == org
    assert snapshot.source_set_revision_id == revision.id
    assert snapshot.status == "CURRENT"
