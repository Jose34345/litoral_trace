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
    UsLaceyEvidenceSnapshotDocument,
    UsLaceyOperation,
)
from litoral_trace.us_lacey import shadow_evidence_snapshot as shadow
from litoral_trace.us_lacey.shadow_evidence_snapshot import ShadowEvidenceSnapshotError
from tests.us_lacey_engine2_postgres import (
    add_test_document,
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


def _no_lock(**_: object):
    return nullcontext()


def _add_extraction(
    factory,
    *,
    organization_id: int,
    assurance_document_id: int,
    values: list[tuple[str, str, int | None]],
) -> int:
    session = tenant_session(factory, organization_id)
    run = DocumentExtractionRun(
        organization_id=organization_id,
        assurance_document_id=assurance_document_id,
        engine="phase-b-test",
        engine_version="1",
        status="SUCCEEDED",
    )
    session.add(run)
    session.flush()
    for index, (field_name, original_value, source_page) in enumerate(values, start=1):
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
                source_page=source_page,
                source_locator=f"table:1;data_row:{index};column:1",
                auto_accepted=False,
                needs_review=True,
            )
        )
    session.commit()
    run_id = run.id
    session.close()
    return run_id


def _require_phase_b_schema(engine) -> None:
    required = {
        "us_lacey_evidence_snapshots",
        "us_lacey_evidence_snapshot_documents",
        "document_text_spans",
        "document_text_translations",
        "semantic_evidence_nodes",
        "semantic_snapshot_nodes",
    }
    if not required.issubset(inspect(engine).get_table_names()):
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_047")


def test_shadow_snapshot_is_operation_wide_idempotent_and_supersedes_atomically(
    monkeypatch,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    _require_phase_b_schema(engine2_postgres_engine)
    monkeypatch.setenv(shadow.SHADOW_FLAG, "1")

    org, operation_id, first_link_id, first_assurance_id, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        role="COMMERCIAL_INVOICE",
        content=b"phase-b-first",
    )
    _add_extraction(
        engine2_postgres_session_factory,
        organization_id=org,
        assurance_document_id=first_assurance_id,
        values=[
            ("country_of_harvest", "País de cosecha declarado en Brasil", 1),
            ("raw.document_text", "texto sin pagina demostrable", None),
        ],
    )

    first = shadow.build_shadow_evidence_snapshot(
        organization_id=org,
        operation_id=operation_id,
        use_configured_translation_provider=False,
        session_factory=engine2_postgres_session_factory,
        lock_factory=_no_lock,
    )
    assert first.created is True
    assert first.metrics.generation == 1
    assert first.metrics.documents_included == 1
    assert first.metrics.spans_created == 1
    assert first.metrics.spans_without_page == 1

    session = tenant_session(engine2_postgres_session_factory, org)
    snapshot1 = session.get(UsLaceyEvidenceSnapshot, first.snapshot_id)
    assert snapshot1 is not None
    assert snapshot1.status == "CURRENT"
    assert snapshot1.document_count == 1
    assert {member.operation_document_id for member in snapshot1.documents} == {first_link_id}
    operation = session.get(UsLaceyOperation, operation_id)
    assert operation.current_evidence_snapshot_id == snapshot1.id
    session.close()

    again = shadow.build_shadow_evidence_snapshot(
        organization_id=org,
        operation_id=operation_id,
        use_configured_translation_provider=False,
        session_factory=engine2_postgres_session_factory,
        lock_factory=_no_lock,
    )
    assert again.created is False
    assert again.reason == "IDEMPOTENT_CURRENT"
    assert again.snapshot_id == first.snapshot_id

    second_link_id, second_assurance_id, _, _ = add_test_document(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation_id,
        role="SUPPLIER_DECLARATION",
        filename="supplier.pdf",
        content=b"phase-b-second",
    )
    _add_extraction(
        engine2_postgres_session_factory,
        organization_id=org,
        assurance_document_id=second_assurance_id,
        values=[("country_of_harvest", "País de colheita declarado no Brasil", 2)],
    )

    second = shadow.build_shadow_evidence_snapshot(
        organization_id=org,
        operation_id=operation_id,
        use_configured_translation_provider=False,
        session_factory=engine2_postgres_session_factory,
        lock_factory=_no_lock,
    )
    assert second.created is True
    assert second.metrics.generation == 2
    assert second.metrics.documents_included == 2

    session = tenant_session(engine2_postgres_session_factory, org)
    snapshots = session.scalars(
        select(UsLaceyEvidenceSnapshot)
        .where(
            UsLaceyEvidenceSnapshot.organization_id == org,
            UsLaceyEvidenceSnapshot.operation_id == operation_id,
        )
        .order_by(UsLaceyEvidenceSnapshot.generation)
    ).all()
    assert [(item.generation, item.status) for item in snapshots] == [
        (1, "SUPERSEDED"),
        (2, "CURRENT"),
    ]
    snapshot2 = snapshots[1]
    assert {member.operation_document_id for member in snapshot2.documents} == {
        first_link_id,
        second_link_id,
    }
    operation = session.get(UsLaceyOperation, operation_id)
    assert operation.current_evidence_snapshot_id == snapshot2.id
    assert session.scalar(
        select(func.count()).select_from(UsLaceyEvidenceSnapshot).where(
            UsLaceyEvidenceSnapshot.organization_id == org,
            UsLaceyEvidenceSnapshot.operation_id == operation_id,
            UsLaceyEvidenceSnapshot.status == "BUILDING",
        )
    ) == 0
    for node in session.scalars(
        select(SemanticEvidenceNode).where(SemanticEvidenceNode.organization_id == org)
    ).all():
        assert node.source_span_id is not None
        assert session.get(DocumentTextSpan, node.source_span_id) is not None
    session.close()


def test_shadow_snapshot_rolls_back_new_generation_if_build_fails(
    monkeypatch,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    _require_phase_b_schema(engine2_postgres_engine)
    monkeypatch.setenv(shadow.SHADOW_FLAG, "1")

    org, operation_id, _, assurance_id, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"phase-b-atomic-first",
    )
    _add_extraction(
        engine2_postgres_session_factory,
        organization_id=org,
        assurance_document_id=assurance_id,
        values=[("description", "Wooden furniture component", 1)],
    )
    first = shadow.build_shadow_evidence_snapshot(
        organization_id=org,
        operation_id=operation_id,
        use_configured_translation_provider=False,
        session_factory=engine2_postgres_session_factory,
        lock_factory=_no_lock,
    )
    assert first.created is True

    _, second_assurance_id, _, _ = add_test_document(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation_id,
        role="PACKING_LIST",
        filename="packing.pdf",
        content=b"phase-b-atomic-second",
    )
    _add_extraction(
        engine2_postgres_session_factory,
        organization_id=org,
        assurance_document_id=second_assurance_id,
        values=[("description", "Lista de empaque de madera", 1)],
    )

    monkeypatch.setattr(
        shadow,
        "_semantic_role",
        lambda _field_name: (_ for _ in ()).throw(RuntimeError("forced shadow build failure")),
    )
    with pytest.raises(RuntimeError, match="forced shadow build failure"):
        shadow.build_shadow_evidence_snapshot(
            organization_id=org,
            operation_id=operation_id,
            use_configured_translation_provider=False,
            session_factory=engine2_postgres_session_factory,
            lock_factory=_no_lock,
        )

    session = tenant_session(engine2_postgres_session_factory, org)
    snapshots = session.scalars(
        select(UsLaceyEvidenceSnapshot).where(
            UsLaceyEvidenceSnapshot.organization_id == org,
            UsLaceyEvidenceSnapshot.operation_id == operation_id,
        )
    ).all()
    assert len(snapshots) == 1
    assert snapshots[0].id == first.snapshot_id
    assert snapshots[0].status == "CURRENT"
    operation = session.get(UsLaceyOperation, operation_id)
    assert operation.current_evidence_snapshot_id == first.snapshot_id
    session.close()


def test_shadow_snapshot_rejects_cross_tenant_operation_scope(
    monkeypatch,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    _require_phase_b_schema(engine2_postgres_engine)
    monkeypatch.setenv(shadow.SHADOW_FLAG, "1")

    org_a, _, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"phase-b-tenant-a",
    )
    org_b, operation_b, _, assurance_b, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"phase-b-tenant-b",
    )
    _add_extraction(
        engine2_postgres_session_factory,
        organization_id=org_b,
        assurance_document_id=assurance_b,
        values=[("description", "Wooden shipment evidence", 1)],
    )

    with pytest.raises(ShadowEvidenceSnapshotError, match="tenant scope"):
        shadow.build_shadow_evidence_snapshot(
            organization_id=org_a,
            operation_id=operation_b,
            use_configured_translation_provider=False,
            session_factory=engine2_postgres_session_factory,
            lock_factory=_no_lock,
        )

    session_b = tenant_session(engine2_postgres_session_factory, org_b)
    assert session_b.scalar(
        select(func.count()).select_from(UsLaceyEvidenceSnapshot).where(
            UsLaceyEvidenceSnapshot.organization_id == org_b,
            UsLaceyEvidenceSnapshot.operation_id == operation_b,
        )
    ) == 0
    session_b.close()
