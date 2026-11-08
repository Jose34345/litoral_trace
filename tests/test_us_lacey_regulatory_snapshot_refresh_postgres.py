from tests.test_us_lacey_regulatory_assessment_snapshot_postgres import (
    _require_schema, _claim_revision,
)
from litoral_trace.db.models import (
    UsLaceyOperationField, UsLaceyPpqPlantLine,
)
from litoral_trace.us_lacey.regulatory_assessment_snapshot import build_regulatory_assessment_snapshot
from litoral_trace.us_lacey.source_sets import seal_current_source_set
from tests.us_lacey_engine2_postgres import create_test_graph, tenant_session, engine2_postgres_engine, engine2_postgres_session_factory

def test_builder_refreshes_same_revision_after_canonical_evidence_arrives(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    """Golden race: first snapshot pre-canonical, later same-revision PPQ facts."""
    _require_schema(engine2_postgres_engine)
    org, operation, _, assurance_id, _, _ = create_test_graph(
        engine2_postgres_session_factory, content=b"reg-late-canonical-inputs"
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

    initial = build_regulatory_assessment_snapshot(
        organization_id=org, operation_id=operation, claim=claim,
        session_factory=engine2_postgres_session_factory,
    )
    assert initial is not None
    assert initial.payload_json["summary"]["subject_count"] == 0
    first_id = initial.id
    first_fingerprint = initial.input_fingerprint

    session = tenant_session(engine2_postgres_session_factory, org)
    line = UsLaceyPpqPlantLine(
        organization_id=org, operation_id=operation, line_reference="1", ordinal=1
    )
    session.add(line)
    session.flush()
    for field_name, value in (
        ("hts_code", "4419199010"),
        ("article_component", "Solid bamboo coasters"),
        ("genus", "Phyllostachys"),
        ("species", "edulis"),
    ):
        session.add(UsLaceyOperationField(
            organization_id=org, operation_id=operation,
            merchandise_line_reference="1", plant_line_id=line.id,
            field_name=field_name, field_scope="PLANT_LINE",
            normalized_value=value, field_status="FOUND", validation_status="VALID",
            confidence=0.98, source_assurance_document_id=assurance_id,
            extractor="canonical-shipment-truth",
        ))
    session.commit()
    session.close()

    updated = build_regulatory_assessment_snapshot(
        organization_id=org, operation_id=operation, claim=claim,
        session_factory=engine2_postgres_session_factory,
    )
    assert updated is not None
    assert updated.id == first_id
    assert updated.input_fingerprint != first_fingerprint
    assessments = {
        item["rule_id"]: item
        for item in updated.payload_json["assessments"]
    }
    assert assessments["HTS_APPLICABILITY"]["status"] == "PASS"
    assert assessments["SPECIAL_COMPOSITE"]["status"] == "NOT_APPLICABLE"
    assert not assessments["HTS_APPLICABILITY"]["review_required"]
    assert not assessments["SPECIAL_COMPOSITE"]["review_required"]

    unchanged = build_regulatory_assessment_snapshot(
        organization_id=org, operation_id=operation, claim=claim,
        session_factory=engine2_postgres_session_factory,
    )
    assert unchanged.id == first_id
    assert unchanged.input_fingerprint == updated.input_fingerprint
    assert unchanged.finalized_at == updated.finalized_at
