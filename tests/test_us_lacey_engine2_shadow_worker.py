from __future__ import annotations
from types import SimpleNamespace

from litoral_trace.lacey_engine.ai_providers import AIProviderConfig
from litoral_trace.us_lacey import worker
from litoral_trace.us_lacey.lacey_engine_service import UsLaceyEngine2Service
from tests.us_lacey_engine2_postgres import (
    engine2_postgres_engine,
    engine2_postgres_session_factory,
)
from tests.test_us_lacey_engine2_partial_failure_postgres import (
    test_partial_failure_persists_successful_siblings_and_never_snapshots_incomplete_source_set,
    test_repeated_partial_failure_reuses_one_failed_run_without_unique_violation,
)
from tests.test_us_lacey_field_judge_regression_corpus import (
    test_seven_unknown_uploads_remain_seven_current_documents,
)
from tests.test_us_lacey_projection_mode_identity_postgres import (
    test_projection_mode_transition_creates_new_auditable_specialized_run,
)
from tests.test_us_lacey_shadow_dispatcher_postgres import (
    test_shadow_dual_persistence_coexists_and_ui_ignores_specialized_schema,
    test_shadow_specialized_failure_keeps_legacy_success_and_ui_projection,
)
from tests.test_us_lacey_shadow_worker_postgres import (
    test_worker_completes_and_ui_projects_only_legacy_when_specialized_crashes,
)
from tests.test_us_lacey_specialized_projection_postgres import (
    test_runtime_enforce_materializes_stable_line_before_projecting_value,
    test_runtime_enforce_projection_persists_unconfirmed_pending_candidate,
    test_runtime_enforce_uses_fusion_conflict_gate_and_never_silently_picks_winner,
    test_runtime_shadow_projection_records_telemetry_without_mutating_field,
)


def _wire_authoritative_success(monkeypatch):
    job = SimpleNamespace(id=7, organization_id=11, operation_id=13, assurance_document_id=17)
    monkeypatch.setattr(worker, "claim_next_us_lacey_job", lambda worker_id: job)
    monkeypatch.setattr(worker, "_assurance_public_id", lambda **_: "doc")
    monkeypatch.setattr(worker, "_processing_service", lambda: SimpleNamespace(process=lambda **_: "COMPLETED"))
    projection = SimpleNamespace(projected_count=4, conflict_count=2)
    monkeypatch.setattr(worker, "project_assurance_document_to_us_lacey", lambda **_: projection)
    monkeypatch.setattr(worker, "complete_us_lacey_job", lambda **_: True)
    monkeypatch.setattr(worker, "_refresh_operation", lambda **_: "READY_FOR_REVIEW")
    # Keep these worker-ordering tests isolated from database/environment-backed
    # suggestion bridges. Dedicated tests cover each bridge independently.
    monkeypatch.setattr(worker, "_project_engine2_suggestions", lambda **_: 0)
    monkeypatch.setattr(worker, "_project_verified_ai_suggestions", lambda **_: 0)
    monkeypatch.setattr(worker, "_run_ai_review_recommendations", lambda **_: None)
    return job


def test_worker_engine2_off_does_not_invoke_shadow_service(monkeypatch):
    _wire_authoritative_success(monkeypatch)
    monkeypatch.setattr(worker, "engine2_mode", lambda: "OFF")
    monkeypatch.setattr(worker, "UsLaceyEngine2Service", lambda **_: (_ for _ in ()).throw(AssertionError("shadow invoked")))
    result = worker.process_one_us_lacey_job(worker_id="unit")
    assert (result.job_status, result.projected_count, result.conflict_count) == ("COMPLETED", 4, 2)


def test_worker_shadow_success_preserves_authoritative_result(monkeypatch):
    _wire_authoritative_success(monkeypatch)
    calls = []
    monkeypatch.setattr(worker, "engine2_mode", lambda: "SHADOW")
    monkeypatch.setattr(worker, "_shadow_engine2", lambda **kwargs: calls.append(kwargs))
    result = worker.process_one_us_lacey_job(worker_id="unit")
    assert calls == [{"organization_id": 11, "operation_id": 13}]
    assert (result.job_status, result.projected_count, result.conflict_count, result.operation_status) == ("COMPLETED", 4, 2, "READY_FOR_REVIEW")


def test_worker_shadow_failure_does_not_fail_authoritative_job(monkeypatch, caplog):
    _wire_authoritative_success(monkeypatch)
    monkeypatch.setattr(worker, "engine2_mode", lambda: "SHADOW")
    monkeypatch.setattr(worker, "build_us_lacey_storage_settings", lambda: object())
    monkeypatch.setattr(worker, "get_us_lacey_storage_client", lambda: object())
    monkeypatch.setattr(worker, "VaultService", lambda **_: object())
    monkeypatch.setattr(worker, "UsLaceyEngine2Service", lambda **_: SimpleNamespace(resolve_operation_with_engine2=lambda **_: (_ for _ in ()).throw(RuntimeError("shadow boom"))))
    result = worker.process_one_us_lacey_job(worker_id="unit")
    assert result.job_status == "COMPLETED" and result.projected_count == 4
    assert "Lacey Engine 2 shadow resolution failed" in caplog.text


def test_worker_shadow_specialized_failure_after_legacy_success_still_completes(monkeypatch):
    _wire_authoritative_success(monkeypatch)
    monkeypatch.setattr(worker, "engine2_mode", lambda: "SHADOW")
    monkeypatch.setenv("LT_AI_ARCHITECTURE", "shadow")

    service = UsLaceyEngine2Service(session_factory=lambda: None, vault_service=object())
    calls: list[str] = []
    monkeypatch.setattr(
        service,
        "_run_legacy_ai_operation",
        lambda **_: calls.append("legacy"),
        raising=False,
    )

    def fail_specialized(**_: object) -> None:
        calls.append("specialized")
        raise RuntimeError("specialized catastrophic failure")

    monkeypatch.setattr(
        service,
        "_run_specialized_ai_operation",
        fail_specialized,
        raising=False,
    )
    config = AIProviderConfig(
        mode="SHADOW",
        provider="gemini",
        model="fixture-model",
        base_url="https://example.invalid",
        api_key="fixture-key",
        timeout_seconds=30.0,
        max_pages=8,
        allow_external=True,
    )

    def run_shadow_dispatch(**_: object) -> None:
        service._dispatch_ai_extractors(
            config=config,
            organization_id=11,
            operation_id=13,
            documents=(),
            source_set_fingerprint="f" * 64,
        )

    monkeypatch.setattr(worker, "_shadow_engine2", run_shadow_dispatch)
    result = worker.process_one_us_lacey_job(worker_id="unit")

    assert result.job_status == "COMPLETED"
    assert result.projected_count == 4
    assert calls == ["legacy", "specialized"]


def test_worker_shadow_mode_still_runs_current_projection(monkeypatch):
    _wire_authoritative_success(monkeypatch)
    projected = []
    monkeypatch.setattr(worker, "engine2_mode", lambda: "SHADOW")
    monkeypatch.setattr(worker, "project_assurance_document_to_us_lacey", lambda **kwargs: (projected.append(kwargs), SimpleNamespace(projected_count=4, conflict_count=2))[1])
    monkeypatch.setattr(worker, "_shadow_engine2", lambda **_: None)
    worker.process_one_us_lacey_job(worker_id="unit")
    assert len(projected) == 1


def test_worker_runs_shadow_before_terminal_completion_and_refresh(monkeypatch):
    _wire_authoritative_success(monkeypatch); events = []
    monkeypatch.setattr(worker, "engine2_mode", lambda: "SHADOW")
    monkeypatch.setattr(worker, "project_assurance_document_to_us_lacey", lambda **_: (events.append("projection"), SimpleNamespace(projected_count=4, conflict_count=2))[1])
    monkeypatch.setattr(worker, "complete_us_lacey_job", lambda **_: (events.append("complete"), True)[1])
    monkeypatch.setattr(worker, "_refresh_operation", lambda **_: (events.append("refresh"), "READY_FOR_REVIEW")[1])
    monkeypatch.setattr(worker, "_shadow_engine2", lambda **_: events.append("shadow"))
    result = worker.process_one_us_lacey_job(worker_id="unit")
    assert events == ["projection", "shadow", "complete", "refresh"]
    assert (result.job_status, result.projected_count, result.conflict_count) == ("COMPLETED", 4, 2)


def test_worker_shadow_may_persist_before_atomic_completion_failure(monkeypatch):
    _wire_authoritative_success(monkeypatch); calls = []
    monkeypatch.setattr(worker, "engine2_mode", lambda: "SHADOW")
    monkeypatch.setattr(worker, "complete_us_lacey_job", lambda **_: False)
    monkeypatch.setattr(worker, "_shadow_engine2", lambda **_: calls.append("shadow"))
    monkeypatch.setattr(worker, "fail_us_lacey_job", lambda **_: "FAILED")
    result = worker.process_one_us_lacey_job(worker_id="unit")
    # Shadow evidence is non-authoritative and may already exist. The queue job must
    # still fail rather than report a terminal false success.
    assert result.job_status == "FAILED" and calls == ["shadow"]
