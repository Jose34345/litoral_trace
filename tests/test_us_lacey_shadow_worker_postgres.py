from __future__ import annotations

from types import SimpleNamespace
import time

from litoral_trace.db.models import UsLaceyEngineDocumentRun, UsLaceyOperationField
from litoral_trace.lacey_engine.ai_shadow import (
    AICandidate,
    AIExtractionResult,
    AI_SHADOW_SCHEMA_VERSION,
)
from litoral_trace.lacey_engine.domain import (
    AdmittedCandidate,
    DocumentResolution,
    DocumentType,
    EvidenceClass,
    FieldStatus,
    LayoutBlock,
    ParsedLayout,
    Provenance,
    RawCandidate,
    ResolvedField,
)
from litoral_trace.us_lacey import ai_suggestions as ai_suggestions_module
from litoral_trace.us_lacey import lacey_engine_service as service_module
from litoral_trace.us_lacey import specialized_shadow as specialized_module
from litoral_trace.us_lacey import worker
from litoral_trace.us_lacey.ppq505 import PPQ505_SHIPMENT_REFERENCE
from tests.us_lacey_engine2_postgres import (
    FakeVault,
    bundle_from_resolution,
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


BOL = "MAEU274342495"


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
            latency_ms=13,
            input_tokens=89,
            output_tokens=8,
            total_tokens=97,
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
        document_type=DocumentType.BILL_OF_LADING,
    )
    return DocumentResolution(
        filename="bill.pdf",
        engine_version="fixture-engine2",
        document_type=DocumentType.BILL_OF_LADING,
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


def test_worker_completes_and_ui_projects_only_legacy_when_specialized_crashes(
    monkeypatch,
    engine2_postgres_session_factory,
) -> None:
    content = b"worker-shadow-failure-isolation"
    org, operation, _, assurance_id, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=content,
    )
    session = tenant_session(engine2_postgres_session_factory, org)
    session.add(
        UsLaceyOperationField(
            organization_id=org,
            operation_id=operation,
            merchandise_line_reference=PPQ505_SHIPMENT_REFERENCE,
            field_name="bill_of_lading",
            field_scope="SHIPMENT",
            plant_line_id=None,
            field_status="MISSING",
            confidence=0.0,
            validation_status="MISSING",
        )
    )
    session.commit()
    session.close()

    monkeypatch.setenv("LT_AI_ARCHITECTURE", "shadow")
    monkeypatch.setenv("US_LACEY_AI_SHADOW_MODE", "shadow")
    monkeypatch.setenv("US_LACEY_AI_PROVIDER", "gemini")
    monkeypatch.setenv("US_LACEY_AI_ALLOW_EXTERNAL", "1")
    monkeypatch.setenv("US_LACEY_GEMINI_API_KEY", "fixture-key")
    monkeypatch.setattr(service_module, "process_bundle", lambda **_: bundle_from_resolution(_engine2_resolution()))
    monkeypatch.setattr(service_module, "build_ai_provider", lambda _: LegacySuccessProvider())
    monkeypatch.setattr(ai_suggestions_module, "get_us_lacey_db_session", engine2_postgres_session_factory)

    specialized_calls: list[int] = []

    def fail_specialized(**_: object):
        specialized_calls.append(1)
        raise RuntimeError("specialized catastrophic failure")

    monkeypatch.setattr(
        specialized_module,
        "run_specialized_shadow_operation",
        fail_specialized,
    )

    service = worker.UsLaceyEngine2Service(
        session_factory=engine2_postgres_session_factory,
        vault_service=FakeVault(content),
    )
    job = SimpleNamespace(
        id=7001,
        organization_id=org,
        operation_id=operation,
        assurance_document_id=assurance_id,
    )
    monkeypatch.setattr(worker, "claim_next_us_lacey_job", lambda worker_id: job)
    monkeypatch.setattr(worker, "_assurance_public_id", lambda **_: "fixture-document")
    monkeypatch.setattr(
        worker,
        "_processing_service",
        lambda: SimpleNamespace(process=lambda **_: "COMPLETED"),
    )
    monkeypatch.setattr(
        worker,
        "project_assurance_document_to_us_lacey",
        lambda **_: SimpleNamespace(projected_count=0, conflict_count=0),
    )
    monkeypatch.setattr(worker, "engine2_mode", lambda: "SHADOW")
    monkeypatch.setattr(
        worker,
        "_shadow_engine2",
        lambda **_: service.resolve_operation_with_engine2(
            organization_id=org,
            operation_id=operation,
        ),
    )
    monkeypatch.setattr(worker, "_project_engine2_suggestions", lambda **_: 0)
    monkeypatch.setattr(
        worker,
        "_project_verified_ai_suggestions",
        lambda **_: ai_suggestions_module.project_verified_ai_suggestions(
            organization_id=org,
            operation_id=operation,
        ),
    )
    monkeypatch.setattr(worker, "_run_ai_review_recommendations", lambda **_: None)
    monkeypatch.setattr(worker, "complete_us_lacey_job", lambda **_: True)
    monkeypatch.setattr(worker, "_refresh_operation", lambda **_: "READY_FOR_REVIEW")

    result = worker.process_one_us_lacey_job(worker_id="unit")

    assert result.job_status == "COMPLETED"

    deadline = time.monotonic() + 3.0
    field = None
    legacy_runs = []
    while time.monotonic() < deadline:
        session = tenant_session(engine2_postgres_session_factory, org)
        try:
            legacy_runs = session.query(UsLaceyEngineDocumentRun).filter_by(
                operation_id=operation,
                schema_version=AI_SHADOW_SCHEMA_VERSION,
                status="SUCCEEDED",
            ).all()
            field = session.query(UsLaceyOperationField).filter_by(
                operation_id=operation,
                field_name="bill_of_lading",
            ).one()
            if specialized_calls == [1] and legacy_runs and field.field_status == "FOUND":
                break
        finally:
            session.close()
        time.sleep(0.01)

    assert specialized_calls == [1]
    assert len(legacy_runs) == 1
    assert legacy_runs[0].resolution_json["architecture"] == "legacy"
    assert field is not None
    assert field.field_status == "FOUND"
    assert field.normalized_value == BOL
    assert field.extractor == "engine2+legacy-fixture-agreement"
