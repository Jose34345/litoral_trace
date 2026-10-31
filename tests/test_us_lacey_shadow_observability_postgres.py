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
    UsLaceyOperation,
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


def _add_page_less_extraction(
    factory,
    *,
    organization_id: int,
    assurance_document_id: int,
) -> None:
    session = tenant_session(factory, organization_id)
    run = DocumentExtractionRun(
        organization_id=organization_id,
        assurance_document_id=assurance_document_id,
        engine="phase-b-observability-test",
        engine_version="1",
        status="SUCCEEDED",
    )
    session.add(run)
    session.flush()

    values = [
        ("description", "Wooden furniture component"),
        ("entered_value", "12600"),
        ("genus", "Eucalyptus"),
        ("species", "grandis"),
    ]
    for index, (field_name, original_value) in enumerate(values, start=1):
        session.add(
            ExtractedDocumentField(
                organization_id=organization_id,
                assurance_document_id=assurance_document_id,
                extraction_run_id=run.id,
                field_name=field_name,
                original_value=original_value,
                normalized_value=original_value,
                value_type="text",
                confidence=0.95,
                confidence_level="HIGH",
                source_page=None,
                source_locator=f"legacy:unknown-page:{index}",
                auto_accepted=False,
                needs_review=True,
            )
        )
    session.commit()
    session.close()


def test_shadow_snapshot_persists_when_all_legacy_fields_lack_source_page(
    monkeypatch,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    _require_phase_b_schema(engine2_postgres_engine)
    monkeypatch.setenv(shadow.SHADOW_FLAG, "1")

    org, operation_id, _, assurance_id, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"phase-b-empty-evidence",
    )
    _add_page_less_extraction(
        engine2_postgres_session_factory,
        organization_id=org,
        assurance_document_id=assurance_id,
    )

    result = shadow.build_shadow_evidence_snapshot(
        organization_id=org,
        operation_id=operation_id,
        use_configured_translation_provider=False,
        session_factory=engine2_postgres_session_factory,
        lock_factory=_no_lock,
    )

    assert result.created is True
    assert result.reason == "CREATED_EMPTY_EVIDENCE"
    assert result.metrics.spans_created == 0
    assert result.metrics.spans_without_page == 4
    assert result.metrics.semantic_nodes_created == 0

    session = tenant_session(engine2_postgres_session_factory, org)
    snapshot = session.get(UsLaceyEvidenceSnapshot, result.snapshot_id)
    assert snapshot is not None
    assert snapshot.status == "CURRENT"
    assert snapshot.document_count == 1
    assert snapshot.node_count == 0

    operation = session.get(UsLaceyOperation, operation_id)
    assert operation is not None
    assert operation.current_evidence_snapshot_id == snapshot.id

    assert session.scalar(
        select(func.count()).select_from(DocumentTextSpan).where(
            DocumentTextSpan.organization_id == org,
            DocumentTextSpan.assurance_document_id == assurance_id,
        )
    ) == 0
    assert session.scalar(
        select(func.count()).select_from(SemanticEvidenceNode).where(
            SemanticEvidenceNode.organization_id == org,
            SemanticEvidenceNode.assurance_document_id == assurance_id,
        )
    ) == 0
    session.close()
