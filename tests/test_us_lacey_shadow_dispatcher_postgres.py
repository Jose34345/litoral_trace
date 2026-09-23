from __future__ import annotations

from uuid import uuid4

from litoral_trace.db.models import UsLaceyEngineDocumentRun, UsLaceyOperationField
from litoral_trace.lacey_engine.ai_shadow import (
    AICandidate,
    AIExtractionResult,
    AI_SHADOW_SCHEMA_VERSION,
)
from litoral_trace.lacey_engine.domain import (
    AdmittedCandidate,
    DocumentResolution,
    DocumentType as EngineDocumentType,
    EvidenceClass,
    FieldStatus,
    LayoutBlock,
    ParsedLayout,
    Provenance,
    RawCandidate,
    ResolvedField,
)
from tests.test_us_lacey_engine2_persistence import _bundle

from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    MultiAgentExtractionResult,
    OperationStatus,
    SpecialistRole,
)
from litoral_trace.lacey_engine.multi_agent.router import RoutingPlan
from litoral_trace.us_lacey import ai_suggestions as ai_suggestions_module
from litoral_trace.us_lacey import lacey_engine_service as service_module
from litoral_trace.us_lacey import specialized_shadow as specialized_module
from litoral_trace.us_lacey.lacey_engine_service import UsLaceyEngine2Service
from litoral_trace.us_lacey.ppq505 import PPQ505_SHIPMENT_REFERENCE
from litoral_trace.us_lacey.specialized_shadow import (
    SPECIALIZED_SHADOW_SCHEMA_VERSION,
    SpecializedShadowRun,
)
from tests.us_lacey_engine2_postgres import (
    FakeVault,
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


BOL = "MAEU274342495"
CONTAINER = "MSKU9228574"


class LegacySuccessProvider:
    name = "legacy-fixture"
    model = "legacy-fixture-model"

    def extract(self, *, filename: str, content: bytes) -> AIExtractionResult:
        return AIExtractionResult(
            provider=self.name,
            model=self.model,
            schema_version=AI_SHADOW_SCHEMA_VERSION,
            candidates=(
                AICandidate(
                    field_key="bill_of_lading",
                    value=BOL,
                    normalized_value=BOL,
                    evidence_class=EvidenceClass.EXPLICIT,
                    page=1,
                    source_text=f"Master B/L: {BOL}",
                    confidence=0.99,
                    provider=self.name,
                    model=self.model,
                ),
            ),
            page_count=1,
            latency_ms=17,
            input_tokens=101,
            output_tokens=9,
            total_tokens=110,
        )


def _engine2_resolution() -> DocumentResolution:
    text = f"Master B/L: {BOL}"
    block = LayoutBlock(
        block_id="bill-line",
        page=1,
        bbox=None,
        text=text,
        block_type="text",
        key_text="Master B/L",
        value_text=BOL,
    )
    raw = RawCandidate(
        field_key="bill_of_lading",
        raw_text=BOL,
        normalized_value=BOL,
        source_block=block,
        evidence_class=EvidenceClass.EXPLICIT,
        extractor_name="fixture",
        extractor_version="1",
        label="Master B/L",
    )
    admitted = AdmittedCandidate(
        raw=raw,
        provenance=Provenance(
            filename="bill.pdf",
            page=1,
            bbox=None,
            block_id=block.block_id,
            source_text=text,
            extractor_name="fixture",
            extractor_version="1",
            evidence_class=EvidenceClass.EXPLICIT,
        ),
        score=1.0,
        document_type=EngineDocumentType.BILL_OF_LADING,
    )
    return DocumentResolution(
        filename="bill.pdf",
        engine_version="fixture-engine2",
        document_type=EngineDocumentType.BILL_OF_LADING,
        type_confidence=1.0,
        layout=ParsedLayout(blocks=(block,), page_count=1),
        sections=(),
        fields={
            "bill_of_lading": ResolvedField(
                field_key="bill_of_lading",
                status=FieldStatus.MATCHED,
                effective_value=BOL,
                winning_candidate=admitted,
                candidates=(admitted,),
            )
        },
    )


def _add_missing_field(factory, *, organization_id: int, operation_id: int, field_name: str) -> None:
    session = tenant_session(factory, organization_id)
    session.add(
        UsLaceyOperationField(
            organization_id=organization_id,
            operation_id=operation_id,
            merchandise_line_reference=PPQ505_SHIPMENT_REFERENCE,
            field_name=field_name,
            field_scope="SHIPMENT",
            plant_line_id=None,
            field_status="MISSING",
            confidence=0.0,
            validation_status="MISSING",
        )
    )
    session.commit()
    session.close()


def _configure_shadow(monkeypatch, factory) -> None:
    monkeypatch.setenv("LT_AI_ARCHITECTURE", "shadow")
    monkeypatch.setenv("US_LACEY_AI_SHADOW_MODE", "shadow")
    monkeypatch.setenv("US_LACEY_AI_PROVIDER", "gemini")
    monkeypatch.setenv("US_LACEY_AI_ALLOW_EXTERNAL", "1")
    monkeypatch.setenv("US_LACEY_GEMINI_API_KEY", "fixture-key")
    monkeypatch.setattr(service_module, "process_bundle", lambda **_: _bundle(_engine2_resolution()))
    monkeypatch.setattr(service_module, "build_ai_provider", lambda _: LegacySuccessProvider())
    monkeypatch.setattr(ai_suggestions_module, "get_us_lacey_db_session", factory)


def _specialized_success(*, documents, **_) -> SpecializedShadowRun:
    source = documents[0]
    candidate = AICandidate(
        field_key="bill_of_lading",
        value=BOL,
        normalized_value=BOL,
        evidence_class=EvidenceClass.EXPLICIT,
        page=1,
        source_text=f"Master B/L: {BOL}",
        confidence=0.98,
        provider="specialized-fixture",
        model="specialized-fixture-model",
        evidence_verified=True,
    )
    envelope = CandidateEnvelope(
        candidate=candidate,
        document_id=source.document_id,
        document_type=DocumentType.BILL_OF_LADING,
        specialist=SpecialistRole.LOGISTICS,
        agent_run_id=uuid4(),
        line_item_key=None,
        source_span_id=None,
    )
    result = MultiAgentExtractionResult(
        routing_plan=RoutingPlan(()),
        specialist_results=(),
        fused_candidates=(envelope,),
        partial_failures=(),
        operation=OperationStatus.COMPLETED,
        specialist_statuses=(),
    )
    return SpecializedShadowRun(
        result=result,
        provider="specialized-fixture",
        model="specialized-fixture-model",
        latency_ms=31,
        input_tokens=203,
        output_tokens=19,
        total_tokens=222,
    )


def test_shadow_specialized_failure_keeps_legacy_success_and_ui_projection(
    monkeypatch,
    engine2_postgres_session_factory,
) -> None:
    org, operation, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"shadow-failure-isolation",
    )
    _add_missing_field(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation,
        field_name="bill_of_lading",
    )
    _configure_shadow(monkeypatch, engine2_postgres_session_factory)
    specialized_calls: list[int] = []

    def fail_specialized(**_) -> SpecializedShadowRun:
        specialized_calls.append(1)
        raise RuntimeError("specialized catastrophic failure")

    monkeypatch.setattr(
        specialized_module,
        "run_specialized_shadow_operation",
        fail_specialized,
    )

    result = UsLaceyEngine2Service(
        session_factory=engine2_postgres_session_factory,
        vault_service=FakeVault(b"shadow-failure-isolation"),
    ).resolve_operation_with_engine2(
        organization_id=org,
        operation_id=operation,
    )

    assert result.status == "SUCCEEDED"
    assert specialized_calls == [1]

    session = tenant_session(engine2_postgres_session_factory, org)
    legacy = session.query(UsLaceyEngineDocumentRun).filter_by(
        operation_id=operation,
        schema_version=AI_SHADOW_SCHEMA_VERSION,
        status="SUCCEEDED",
    ).one()
    assert legacy.resolution_json["architecture"] == "legacy"
    assert legacy.resolution_json["total_tokens"] == 110
    session.close()

    assert ai_suggestions_module.project_verified_ai_suggestions(
        organization_id=org,
        operation_id=operation,
    ) == 1
    session = tenant_session(engine2_postgres_session_factory, org)
    field = session.query(UsLaceyOperationField).filter_by(
        operation_id=operation,
        field_name="bill_of_lading",
    ).one()
    assert field.field_status == "FOUND"
    assert field.normalized_value == BOL
    assert field.extractor == "engine2+legacy-fixture-agreement"
    session.close()


def test_shadow_dual_persistence_coexists_and_ui_ignores_specialized_schema(
    monkeypatch,
    engine2_postgres_session_factory,
) -> None:
    org, operation, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"shadow-dual-persistence",
    )
    _add_missing_field(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation,
        field_name="bill_of_lading",
    )
    _add_missing_field(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation,
        field_name="container_number",
    )
    _configure_shadow(monkeypatch, engine2_postgres_session_factory)
    monkeypatch.setattr(
        specialized_module,
        "run_specialized_shadow_operation",
        _specialized_success,
    )

    result = UsLaceyEngine2Service(
        session_factory=engine2_postgres_session_factory,
        vault_service=FakeVault(b"shadow-dual-persistence"),
    ).resolve_operation_with_engine2(
        organization_id=org,
        operation_id=operation,
    )
    assert result.status == "SUCCEEDED"

    session = tenant_session(engine2_postgres_session_factory, org)
    legacy = session.query(UsLaceyEngineDocumentRun).filter_by(
        operation_id=operation,
        schema_version=AI_SHADOW_SCHEMA_VERSION,
        status="SUCCEEDED",
    ).one()
    specialized = session.query(UsLaceyEngineDocumentRun).filter_by(
        operation_id=operation,
        schema_version=SPECIALIZED_SHADOW_SCHEMA_VERSION,
        status="SUCCEEDED",
    ).one()
    assert legacy.resolution_json["architecture"] == "legacy"
    assert specialized.resolution_json["architecture"] == "specialized"
    assert specialized.resolution_json["total_tokens"] == 222
    assert specialized.resolution_json["candidate_count"] == 1

    # Make the specialized payload deliberately compatible with the legacy bridge.
    # The UI query must still ignore it solely because its schema is specialized.
    specialized.resolution_json = {
        "schema_version": SPECIALIZED_SHADOW_SCHEMA_VERSION,
        "architecture": "specialized",
        "provider": "specialized-fixture",
        "model": "specialized-fixture-model",
        "candidates": [
            {
                "field_key": "container_number",
                "value": CONTAINER,
                "normalized_value": CONTAINER,
                "evidence_class": "EXPLICIT",
                "page": 1,
                "source_text": f"Container Number: {CONTAINER}",
                "confidence": 0.99,
                "evidence_verified": True,
            }
        ],
        "reconciliation": [
            {
                "field_key": "container_number",
                "status": "AGREEMENT",
                "ai_value": CONTAINER,
            }
        ],
    }
    session.commit()
    session.close()

    assert ai_suggestions_module.project_verified_ai_suggestions(
        organization_id=org,
        operation_id=operation,
    ) == 1

    session = tenant_session(engine2_postgres_session_factory, org)
    bill = session.query(UsLaceyOperationField).filter_by(
        operation_id=operation,
        field_name="bill_of_lading",
    ).one()
    container = session.query(UsLaceyOperationField).filter_by(
        operation_id=operation,
        field_name="container_number",
    ).one()
    assert bill.field_status == "FOUND"
    assert bill.normalized_value == BOL
    assert container.field_status == "MISSING"
    assert container.normalized_value is None
    session.close()
