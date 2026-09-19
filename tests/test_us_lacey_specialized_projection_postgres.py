from __future__ import annotations

from uuid import uuid4

from litoral_trace.db.models import (
    UsLaceyEngineDocumentRun,
    UsLaceyFieldCandidate,
    UsLaceyOperation,
    UsLaceyOperationField,
    UsLaceyPpqPlantLine,
)
from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    MultiAgentExtractionResult,
    OperationStatus,
    SpecialistRole,
)
from litoral_trace.lacey_engine.multi_agent.fusion import FusionConflict, FusionKey
from litoral_trace.lacey_engine.multi_agent.router import RoutingPlan
from litoral_trace.us_lacey import lacey_engine_service as service_module
from litoral_trace.us_lacey import specialized_shadow as specialized_module
from litoral_trace.us_lacey.lacey_engine_service import UsLaceyEngine2Service
from litoral_trace.us_lacey.specialized_shadow import (
    SPECIALIZED_SHADOW_SCHEMA_VERSION,
    SpecializedShadowRun,
)
from tests.test_us_lacey_shadow_dispatcher_postgres import (
    BOL,
    _add_missing_field,
    _configure_shadow,
    _specialized_success,
)
from tests.us_lacey_engine2_postgres import (
    FakeVault,
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


def _configure_projection(monkeypatch, factory, *, mode: str) -> None:
    _configure_shadow(monkeypatch, factory)
    monkeypatch.setenv("LT_AI_ARCHITECTURE", "specialized")
    monkeypatch.setenv("LT_AI_SPECIALIZED_PROJECTION_MODE", mode)


def _line_success(*, documents, **_) -> SpecializedShadowRun:
    source = documents[0]
    candidate = AICandidate(
        field_key="genus",
        value="Pinus",
        normalized_value="Pinus",
        evidence_class=EvidenceClass.EXPLICIT,
        page=1,
        source_text="SKU:PINE-001 Genus: Pinus",
        confidence=0.98,
        provider="specialized-fixture",
        model="specialized-fixture-model",
        evidence_verified=True,
    )
    envelope = CandidateEnvelope(
        candidate=candidate,
        document_id=source.document_id,
        document_type=DocumentType.BOTANICAL_DECLARATION,
        specialist=SpecialistRole.BOTANICAL,
        agent_run_id=uuid4(),
        line_item_key="SKU:PINE-001",
        source_span_id=None,
    )
    return SpecializedShadowRun(
        result=MultiAgentExtractionResult(
            routing_plan=RoutingPlan(()),
            specialist_results=(),
            fused_candidates=(envelope,),
            partial_failures=(),
            operation=OperationStatus.COMPLETED,
            specialist_statuses=(),
        ),
        provider="specialized-fixture",
        model="specialized-fixture-model",
        latency_ms=12,
        input_tokens=90,
        output_tokens=8,
        total_tokens=98,
    )


def _conflicting_bol_success(*, documents, **_) -> SpecializedShadowRun:
    source = documents[0]
    first = CandidateEnvelope(
        candidate=AICandidate(
            field_key="bill_of_lading",
            value=BOL,
            normalized_value=BOL,
            evidence_class=EvidenceClass.EXPLICIT,
            page=1,
            source_text=f"Master B/L: {BOL}",
            confidence=0.99,
            provider="specialized-fixture",
            model="specialized-fixture-model",
            evidence_verified=True,
        ),
        document_id=source.document_id,
        document_type=DocumentType.BILL_OF_LADING,
        specialist=SpecialistRole.LOGISTICS,
        agent_run_id=uuid4(),
        line_item_key=None,
        source_span_id=None,
    )
    competing = "OOLU1234567890"
    second = CandidateEnvelope(
        candidate=AICandidate(
            field_key="bill_of_lading",
            value=competing,
            normalized_value=competing,
            evidence_class=EvidenceClass.EXPLICIT,
            page=1,
            source_text=f"Master B/L: {competing}",
            confidence=0.99,
            provider="specialized-fixture",
            model="specialized-fixture-model",
            evidence_verified=True,
        ),
        document_id=uuid4(),
        document_type=DocumentType.BILL_OF_LADING,
        specialist=SpecialistRole.LOGISTICS,
        agent_run_id=uuid4(),
        line_item_key=None,
        source_span_id=None,
    )
    conflict = FusionConflict(
        key=FusionKey("bill_of_lading", None, None),
        candidates=(first, second),
        normalized_values=(BOL, competing),
        requires_ai_resolution=True,
    )
    return SpecializedShadowRun(
        result=MultiAgentExtractionResult(
            routing_plan=RoutingPlan(()),
            specialist_results=(),
            fused_candidates=(first,),
            partial_failures=(),
            operation=OperationStatus.COMPLETED,
            specialist_statuses=(),
            fusion_conflicts=(conflict,),
        ),
        provider="specialized-fixture",
        model="specialized-fixture-model",
        latency_ms=14,
        input_tokens=110,
        output_tokens=10,
        total_tokens=120,
    )


def test_runtime_shadow_projection_records_telemetry_without_mutating_field(
    monkeypatch,
    engine2_postgres_session_factory,
) -> None:
    org, operation, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"specialized-projection-shadow",
    )
    _add_missing_field(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation,
        field_name="bill_of_lading",
    )
    _configure_projection(monkeypatch, engine2_postgres_session_factory, mode="shadow")
    monkeypatch.setattr(specialized_module, "run_specialized_shadow_operation", _specialized_success)

    result = UsLaceyEngine2Service(
        session_factory=engine2_postgres_session_factory,
        vault_service=FakeVault(b"specialized-projection-shadow"),
    ).resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    assert result.status == "SUCCEEDED"

    session = tenant_session(engine2_postgres_session_factory, org)
    field = session.query(UsLaceyOperationField).filter_by(
        operation_id=operation,
        field_name="bill_of_lading",
    ).one()
    run = session.query(UsLaceyEngineDocumentRun).filter_by(
        operation_id=operation,
        schema_version=SPECIALIZED_SHADOW_SCHEMA_VERSION,
        status="SUCCEEDED",
    ).one()
    assert field.field_status == "MISSING"
    assert field.normalized_value is None
    assert run.resolution_json["projection"] == {
        "version": "lacey_specialized_projection_v2",
        "mode": "shadow",
        "eligible_count": 1,
        "projected_count": 0,
        "review_count": 0,
        "skipped_count": 0,
        "planned_line_count": 0,
        "materialized_line_count": 0,
    }
    session.close()


def test_runtime_enforce_projection_persists_unconfirmed_pending_candidate(
    monkeypatch,
    engine2_postgres_session_factory,
) -> None:
    org, operation, _, assurance_id, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"specialized-projection-enforce",
    )
    _add_missing_field(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation,
        field_name="bill_of_lading",
    )
    _configure_projection(monkeypatch, engine2_postgres_session_factory, mode="enforce")
    monkeypatch.setattr(specialized_module, "run_specialized_shadow_operation", _specialized_success)

    service = UsLaceyEngine2Service(
        session_factory=engine2_postgres_session_factory,
        vault_service=FakeVault(b"specialized-projection-enforce"),
    )
    result = service.resolve_operation_with_engine2(
        organization_id=org,
        operation_id=operation,
    )
    assert result.status == "SUCCEEDED"
    repeat = service.resolve_operation_with_engine2(
        organization_id=org,
        operation_id=operation,
    )
    assert repeat.status == "SUCCEEDED"

    session = tenant_session(engine2_postgres_session_factory, org)
    field = session.query(UsLaceyOperationField).filter_by(
        operation_id=operation,
        field_name="bill_of_lading",
    ).one()
    candidates = session.query(UsLaceyFieldCandidate).filter_by(
        operation_id=operation,
        operation_field_id=field.id,
    ).all()
    assert len(candidates) == 1
    candidate = candidates[0]
    run = session.query(UsLaceyEngineDocumentRun).filter_by(
        operation_id=operation,
        schema_version=SPECIALIZED_SHADOW_SCHEMA_VERSION,
        status="SUCCEEDED",
    ).one()

    assert field.field_status == "FOUND"
    assert field.normalized_value == BOL
    assert field.human_value is None
    assert field.reviewed_at is None
    assert field.reviewed_by_user_id is None
    assert candidate.normalized_value == BOL
    assert candidate.source_assurance_document_id == assurance_id
    assert candidate.decision == "PENDING"
    assert candidate.extractor == "specialized-field-judge"
    assert len(candidate.fingerprint) == 64
    assert run.resolution_json["projection"]["mode"] == "enforce"
    assert run.resolution_json["projection"]["projected_count"] == 1
    session.close()


def test_runtime_enforce_materializes_stable_line_before_projecting_value(
    monkeypatch,
    engine2_postgres_session_factory,
) -> None:
    org, operation, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        role="BOTANICAL_DECLARATION",
        content=b"specialized-line-materialization",
    )
    _configure_projection(monkeypatch, engine2_postgres_session_factory, mode="enforce")
    monkeypatch.setattr(specialized_module, "run_specialized_shadow_operation", _line_success)

    result = UsLaceyEngine2Service(
        session_factory=engine2_postgres_session_factory,
        vault_service=FakeVault(b"specialized-line-materialization"),
    ).resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    assert result.status == "SUCCEEDED"

    session = tenant_session(engine2_postgres_session_factory, org)
    lines = session.query(UsLaceyPpqPlantLine).filter_by(operation_id=operation).all()
    assert len(lines) == 1
    assert lines[0].line_reference.startswith("LT-")
    genus = session.query(UsLaceyOperationField).filter_by(
        operation_id=operation,
        merchandise_line_reference=lines[0].line_reference,
        field_name="genus",
    ).one()
    persisted_operation = session.get(UsLaceyOperation, operation)
    assert genus.field_status == "FOUND"
    assert genus.normalized_value == "Pinus"
    assert genus.human_value is None
    assert persisted_operation.merchandise_line_count == 1
    assert session.query(UsLaceyFieldCandidate).filter_by(
        operation_id=operation,
        operation_field_id=genus.id,
        decision="PENDING",
    ).count() == 1
    session.close()


def test_runtime_enforce_uses_fusion_conflict_gate_and_never_silently_picks_winner(
    monkeypatch,
    engine2_postgres_session_factory,
) -> None:
    org, operation, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"specialized-projection-conflict",
    )
    _add_missing_field(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation,
        field_name="bill_of_lading",
    )
    _configure_projection(monkeypatch, engine2_postgres_session_factory, mode="enforce")
    monkeypatch.setattr(
        specialized_module,
        "run_specialized_shadow_operation",
        _conflicting_bol_success,
    )

    result = UsLaceyEngine2Service(
        session_factory=engine2_postgres_session_factory,
        vault_service=FakeVault(b"specialized-projection-conflict"),
    ).resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    assert result.status == "SUCCEEDED"

    session = tenant_session(engine2_postgres_session_factory, org)
    field = session.query(UsLaceyOperationField).filter_by(
        operation_id=operation,
        field_name="bill_of_lading",
    ).one()
    run = session.query(UsLaceyEngineDocumentRun).filter_by(
        operation_id=operation,
        schema_version=SPECIALIZED_SHADOW_SCHEMA_VERSION,
        status="SUCCEEDED",
    ).one()
    assert field.field_status == "MISSING"
    assert field.normalized_value is None
    assert session.query(UsLaceyFieldCandidate).filter_by(operation_id=operation).count() == 0
    assert run.resolution_json["projection"]["review_count"] == 1
    assert run.resolution_json["projection"]["projected_count"] == 0
    session.close()
