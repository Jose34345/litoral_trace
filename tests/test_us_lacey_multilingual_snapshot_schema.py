"""Schema-only acceptance for multilingual evidence foundations (045-047)."""
from __future__ import annotations

import hashlib
from uuid import uuid4

import pytest
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError

from litoral_trace.db.models import (
    DocumentExtractionRun,
    DocumentTextSpan,
    DocumentTextTranslation,
    SemanticEvidenceEdge,
    SemanticEvidenceNode,
    SemanticSnapshotNode,
    UsLaceyEvidenceSnapshot,
    UsLaceyEvidenceSnapshotDocument,
    UsLaceyOperation,
)
from litoral_trace.services.translation import (
    AwsTranslateProvider,
    NoOpEnglishProvider,
    TranslationProvider,
)
from tests.us_lacey_engine2_postgres import (
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


def test_schema_models_expose_snapshot_provenance_contract():
    assert "current_evidence_snapshot_id" in UsLaceyOperation.__table__.c
    assert {
        "us_lacey_evidence_snapshots",
        "us_lacey_evidence_snapshot_documents",
        "document_text_spans",
        "document_text_translations",
        "semantic_evidence_nodes",
        "semantic_snapshot_nodes",
        "semantic_evidence_edges",
    } <= {
        UsLaceyEvidenceSnapshot.__tablename__,
        UsLaceyEvidenceSnapshotDocument.__tablename__,
        DocumentTextSpan.__tablename__,
        DocumentTextTranslation.__tablename__,
        SemanticEvidenceNode.__tablename__,
        SemanticSnapshotNode.__tablename__,
        SemanticEvidenceEdge.__tablename__,
    }
    current_fk = next(
        item
        for item in UsLaceyOperation.__table__.foreign_key_constraints
        if item.name == "fk_us_lacey_operations_current_snapshot_tenant"
    )
    assert {column.name for column in current_fk.columns} == {
        "current_evidence_snapshot_id",
        "organization_id",
    }
    assert "documents" in UsLaceyEvidenceSnapshot.__mapper__.relationships
    assert "translations" in DocumentTextSpan.__mapper__.relationships
    assert "evidence_nodes" in DocumentTextSpan.__mapper__.relationships
    assert "snapshot_links" in SemanticEvidenceNode.__mapper__.relationships


def test_noop_english_translation_is_identity_and_protocol_compatible():
    provider = NoOpEnglishProvider()
    assert isinstance(provider, TranslationProvider)
    result = provider.translate("Country of harvest", "en", "en")
    assert result.translated_text == "Country of harvest"
    assert result.provider == "NOOP_ENGLISH"
    assert result.metadata["identity_translation"] is True
    with pytest.raises(ValueError):
        provider.translate("País de cosecha", "es", "en")


class _FakeAwsTranslateClient:
    def translate_text(self, **kwargs):
        assert kwargs == {
            "Text": "País de cosecha",
            "SourceLanguageCode": "es",
            "TargetLanguageCode": "en",
        }
        return {
            "TranslatedText": "Country of harvest",
            "SourceLanguageCode": "es",
            "TargetLanguageCode": "en",
            "AppliedTerminologies": [],
        }


def test_aws_translation_provider_is_lazy_and_injectable():
    provider = AwsTranslateProvider(client=_FakeAwsTranslateClient())
    result = provider.translate("País de cosecha", "es", "en")
    assert (result.translated_text, result.provider) == (
        "Country of harvest",
        "AWS_TRANSLATE",
    )


def test_new_tables_persist_relationships_and_enforce_tenant_fks_and_uniqueness(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    required = {
        "us_lacey_evidence_snapshots",
        "us_lacey_evidence_snapshot_documents",
        "document_text_spans",
        "document_text_translations",
        "semantic_evidence_nodes",
        "semantic_snapshot_nodes",
        "semantic_evidence_edges",
    }
    if not required.issubset(inspect(engine2_postgres_engine).get_table_names()):
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_047")

    org, operation_id, operation_document_id, assurance_id, _, source_sha = create_test_graph(
        engine2_postgres_session_factory,
        content=b"multilingual-schema-a",
    )
    other_org, _, other_operation_document_id, _, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"multilingual-schema-b",
    )
    assert other_org != org

    session = tenant_session(engine2_postgres_session_factory, org)
    extraction = DocumentExtractionRun(
        organization_id=org,
        assurance_document_id=assurance_id,
        engine="schema-test",
        engine_version="1",
        status="SUCCEEDED",
    )
    session.add(extraction)
    session.flush()

    fingerprint = hashlib.sha256(uuid4().bytes).hexdigest()
    snapshot = UsLaceyEvidenceSnapshot(
        organization_id=org,
        operation_id=operation_id,
        generation=1,
        status="CURRENT",
        source_set_fingerprint=fingerprint,
        graph_version="semantic-graph-v2",
        ontology_version="ontology-v1",
        translation_pipeline_version="translation-v1",
        document_count=1,
    )
    session.add(snapshot)
    session.flush()

    membership = UsLaceyEvidenceSnapshotDocument(
        organization_id=org,
        snapshot_id=snapshot.id,
        operation_document_id=operation_document_id,
        assurance_document_id=assurance_id,
        extraction_run_id=extraction.id,
        source_sha256=source_sha,
        document_role="SUPPLIER_DECLARATION",
        processing_result="SUCCEEDED",
    )
    session.add(membership)

    source_text = "País de cosecha"
    span = DocumentTextSpan(
        organization_id=org,
        assurance_document_id=assurance_id,
        extraction_run_id=extraction.id,
        page=1,
        block_id="table-1-row-1-col-4",
        table_id="table-1",
        row_index=1,
        column_index=4,
        original_text=source_text,
        original_language="es",
        language_confidence=0.99,
        extraction_method="NATIVE_TEXT",
        content_hash=hashlib.sha256(source_text.encode()).hexdigest(),
    )
    session.add(span)
    session.flush()

    translation = DocumentTextTranslation(
        organization_id=org,
        source_span_id=span.id,
        source_language="es",
        target_language="en",
        translated_text="Country of harvest",
        provider="TEST",
        model_name="deterministic",
        model_version="1",
        input_hash=hashlib.sha256(source_text.encode()).hexdigest(),
        output_hash=hashlib.sha256(b"Country of harvest").hexdigest(),
    )
    node = SemanticEvidenceNode(
        organization_id=org,
        assurance_document_id=assurance_id,
        extraction_run_id=extraction.id,
        source_span_id=span.id,
        target_field="country_of_harvest",
        semantic_role="COUNTRY_OF_HARVEST",
        scope="PLANT_COMPONENT",
        local_entity_key=f"{assurance_id}:table-1:row:1",
        original_value="Argentina",
        normalized_value="ARGENTINA",
        evidence_class="EXPLICIT",
        document_type="SUPPLIER_DECLARATION",
        extraction_confidence=0.99,
        authority_score=45.0,
        candidate_score=92.0,
        fingerprint=hashlib.sha256(b"node-a" + uuid4().bytes).hexdigest(),
    )
    session.add_all((translation, node))
    session.flush()

    link = SemanticSnapshotNode(
        organization_id=org,
        snapshot_id=snapshot.id,
        evidence_node_id=node.id,
        canonical_entity_id="PLANT_COMPONENT:1",
    )
    node2 = SemanticEvidenceNode(
        organization_id=org,
        assurance_document_id=assurance_id,
        extraction_run_id=extraction.id,
        source_span_id=span.id,
        target_field="country_of_harvest",
        semantic_role="COUNTRY_OF_HARVEST",
        scope="PLANT_COMPONENT",
        local_entity_key=f"{assurance_id}:table-1:row:1",
        original_value="Argentina",
        normalized_value="ARGENTINA",
        evidence_class="EXPLICIT",
        document_type="SUPPLIER_DECLARATION",
        extraction_confidence=0.95,
        authority_score=45.0,
        candidate_score=90.0,
        fingerprint=hashlib.sha256(b"node-b" + uuid4().bytes).hexdigest(),
    )
    session.add_all((link, node2))
    session.flush()
    edge = SemanticEvidenceEdge(
        organization_id=org,
        snapshot_id=snapshot.id,
        from_node_id=node.id,
        to_node_id=node2.id,
        relation_type="CORROBORATES",
        confidence=0.98,
        reason_code="TEST_CORROBORATION",
    )
    session.add(edge)
    session.query(UsLaceyOperation).filter_by(id=operation_id).update(
        {"current_evidence_snapshot_id": snapshot.id}
    )
    session.commit()

    persisted = session.query(UsLaceyEvidenceSnapshot).filter_by(id=snapshot.id).one()
    assert persisted.documents[0].extraction_run_id == extraction.id
    assert persisted.snapshot_nodes[0].canonical_entity_id == "PLANT_COMPONENT:1"
    assert persisted.edges[0].relation_type == "CORROBORATES"
    assert session.query(DocumentTextSpan).filter_by(id=span.id).one().translations[0].translated_text == "Country of harvest"
    assert session.query(UsLaceyOperation).filter_by(id=operation_id).one().current_evidence_snapshot_id == snapshot.id

    session.add(
        UsLaceyEvidenceSnapshot(
            organization_id=org,
            operation_id=operation_id,
            generation=1,
            status="BUILDING",
            source_set_fingerprint=hashlib.sha256(uuid4().bytes).hexdigest(),
            graph_version="x",
            ontology_version="x",
            translation_pipeline_version="x",
        )
    )
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()

    session.add(
        UsLaceyEvidenceSnapshotDocument(
            organization_id=org,
            snapshot_id=snapshot.id,
            operation_document_id=other_operation_document_id,
            assurance_document_id=assurance_id,
            extraction_run_id=extraction.id,
            source_sha256=source_sha,
            document_role="UNKNOWN",
            processing_result="SUCCEEDED",
        )
    )
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()
    session.close()


def test_phase_b_shadow_lifecycle_runs_inside_postgres_gate(
    monkeypatch,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    """The gate already executes this file explicitly; keep Phase B acceptance non-skippable."""
    from tests import test_us_lacey_multilingual_shadow_postgres as phase_b
    from tests import test_us_lacey_shadow_eligibility_postgres as eligibility

    acceptance_tests = (
        phase_b.test_shadow_snapshot_is_operation_wide_idempotent_and_supersedes_atomically,
        phase_b.test_shadow_snapshot_refuses_partial_snapshot_when_any_current_source_is_unavailable,
        phase_b.test_shadow_snapshot_rejects_cross_tenant_operation_scope,
        phase_b.test_shadow_snapshot_rolls_back_new_generation_if_build_fails,
        eligibility.test_shadow_snapshot_accepts_latest_needs_review_extraction,
        eligibility.test_shadow_snapshot_rejects_running_extraction,
        eligibility.test_shadow_fingerprint_mutates_on_new_extraction_run,
    )
    for acceptance in acceptance_tests:
        with monkeypatch.context() as scoped:
            acceptance(
                scoped,
                engine2_postgres_engine,
                engine2_postgres_session_factory,
            )
