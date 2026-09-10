from __future__ import annotations

from contextlib import nullcontext

import pytest
from sqlalchemy import func, inspect, select

from litoral_trace.db.models import (
    DocumentExtractionRun,
    DocumentTextSpan,
    ExtractedDocumentField,
    SemanticEvidenceNode,
    UsLaceyEvidenceSnapshot,
)
from litoral_trace.us_lacey import shadow_evidence_snapshot as shadow
from tests.us_lacey_engine2_postgres import (
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


def _no_lock(**_: object):
    return nullcontext()


def _require_phase_b_schema(engine) -> None:
    required = {
        "us_lacey_evidence_snapshots",
        "us_lacey_evidence_snapshot_documents",
        "document_text_spans",
        "semantic_evidence_nodes",
        "semantic_snapshot_nodes",
    }
    if not required.issubset(inspect(engine).get_table_names()):
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_047")


def _add_extraction_run(
    factory,
    *,
    organization_id: int,
    assurance_document_id: int,
    status: str,
    original_value: str | None = None,
    source_page: int = 1,
) -> int:
    session = tenant_session(factory, organization_id)
    run = DocumentExtractionRun(
        organization_id=organization_id,
        assurance_document_id=assurance_document_id,
        engine="phase-b-eligibility-test",
        engine_version="1",
        status=status,
    )
    session.add(run)
    session.flush()

    if original_value is not None:
        session.add(
            ExtractedDocumentField(
                organization_id=organization_id,
                assurance_document_id=assurance_document_id,
                extraction_run_id=run.id,
                field_name="description",
                original_value=original_value,
                normalized_value=original_value,
                value_type="text",
                confidence=0.95,
                confidence_level="HIGH",
                source_page=source_page,
                source_locator=f"legacy:page:{source_page}:description",
                auto_accepted=False,
                needs_review=status == "NEEDS_REVIEW",
            )
        )

    run_id = run.id
    session.commit()
    session.close()
    return run_id


def _build_snapshot(factory, *, organization_id: int, operation_id: int):
    return shadow.build_shadow_evidence_snapshot(
        organization_id=organization_id,
        operation_id=operation_id,
        use_configured_translation_provider=False,
        session_factory=factory,
        lock_factory=_no_lock,
    )


def test_shadow_snapshot_accepts_latest_needs_review_extraction(
    monkeypatch,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    _require_phase_b_schema(engine2_postgres_engine)
    monkeypatch.setenv(shadow.SHADOW_FLAG, "1")

    org, operation_id, _, assurance_id, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"phase-b-needs-review-evidence",
    )
    run_id = _add_extraction_run(
        engine2_postgres_session_factory,
        organization_id=org,
        assurance_document_id=assurance_id,
        status="NEEDS_REVIEW",
        original_value="Madera aserrada de eucalipto para exportacion",
    )

    result = _build_snapshot(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation_id,
    )

    assert result.created is True
    assert result.reason == "CREATED"
    assert result.metrics.spans_created > 0
    assert result.metrics.semantic_nodes_created > 0

    session = tenant_session(engine2_postgres_session_factory, org)
    snapshot = session.get(UsLaceyEvidenceSnapshot, result.snapshot_id)
    assert snapshot is not None
    assert snapshot.status == "CURRENT"
    assert snapshot.generation == 1
    assert snapshot.node_count > 0
    assert session.scalar(
        select(func.count()).select_from(DocumentTextSpan).where(
            DocumentTextSpan.organization_id == org,
            DocumentTextSpan.extraction_run_id == run_id,
        )
    ) > 0
    assert session.scalar(
        select(func.count()).select_from(SemanticEvidenceNode).where(
            SemanticEvidenceNode.organization_id == org,
            SemanticEvidenceNode.extraction_run_id == run_id,
        )
    ) > 0
    session.close()


def test_shadow_snapshot_rejects_running_extraction(
    monkeypatch,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    _require_phase_b_schema(engine2_postgres_engine)
    monkeypatch.setenv(shadow.SHADOW_FLAG, "1")

    org, operation_id, _, assurance_id, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"phase-b-running-evidence",
    )
    _add_extraction_run(
        engine2_postgres_session_factory,
        organization_id=org,
        assurance_document_id=assurance_id,
        status="SUCCEEDED",
        original_value="Previously completed extraction",
    )
    _add_extraction_run(
        engine2_postgres_session_factory,
        organization_id=org,
        assurance_document_id=assurance_id,
        status="RUNNING",
    )

    result = _build_snapshot(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation_id,
    )

    assert result.created is False
    assert result.reason == "SOURCE_SET_EXTRACTION_IN_PROGRESS"
    assert result.snapshot_id is None
    assert result.source_set_fingerprint is None

    session = tenant_session(engine2_postgres_session_factory, org)
    assert session.scalar(
        select(func.count()).select_from(UsLaceyEvidenceSnapshot).where(
            UsLaceyEvidenceSnapshot.organization_id == org,
            UsLaceyEvidenceSnapshot.operation_id == operation_id,
        )
    ) == 0
    session.close()


def test_shadow_fingerprint_mutates_on_new_extraction_run(
    monkeypatch,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    _require_phase_b_schema(engine2_postgres_engine)
    monkeypatch.setenv(shadow.SHADOW_FLAG, "1")

    org, operation_id, _, assurance_id, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"phase-b-fingerprint-evidence",
    )
    first_run_id = _add_extraction_run(
        engine2_postgres_session_factory,
        organization_id=org,
        assurance_document_id=assurance_id,
        status="SUCCEEDED",
        original_value="Same extracted evidence across parser runs",
    )

    first = _build_snapshot(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation_id,
    )
    assert first.created is True
    assert first.metrics.generation == 1

    second_run_id = _add_extraction_run(
        engine2_postgres_session_factory,
        organization_id=org,
        assurance_document_id=assurance_id,
        status="SUCCEEDED",
        original_value="Same extracted evidence across parser runs",
    )
    assert second_run_id != first_run_id

    second = _build_snapshot(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation_id,
    )

    assert second.created is True
    assert second.metrics.generation == 2
    assert second.snapshot_id != first.snapshot_id
    assert second.source_set_fingerprint != first.source_set_fingerprint

    session = tenant_session(engine2_postgres_session_factory, org)
    first_snapshot = session.get(UsLaceyEvidenceSnapshot, first.snapshot_id)
    second_snapshot = session.get(UsLaceyEvidenceSnapshot, second.snapshot_id)
    assert first_snapshot is not None
    assert second_snapshot is not None
    assert first_snapshot.status == "SUPERSEDED"
    assert second_snapshot.status == "CURRENT"
    assert first_snapshot.source_set_fingerprint != second_snapshot.source_set_fingerprint
    session.close()
