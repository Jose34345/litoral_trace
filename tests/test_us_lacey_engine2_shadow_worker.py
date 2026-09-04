from __future__ import annotations
from types import SimpleNamespace
from litoral_trace.us_lacey import worker


def _wire_authoritative_success(monkeypatch):
    job = SimpleNamespace(id=7, organization_id=11, operation_id=13, assurance_document_id=17)
    monkeypatch.setattr(worker, "claim_next_us_lacey_job", lambda worker_id: job)
    monkeypatch.setattr(worker, "_assurance_public_id", lambda **_: "doc")
    monkeypatch.setattr(worker, "_processing_service", lambda: SimpleNamespace(process=lambda **_: "COMPLETED"))
    projection = SimpleNamespace(projected_count=4, conflict_count=2)
    monkeypatch.setattr(worker, "project_assurance_document_to_us_lacey", lambda **_: projection)
    monkeypatch.setattr(worker, "complete_us_lacey_job", lambda **_: True)
    monkeypatch.setattr(worker, "_refresh_operation", lambda **_: "READY_FOR_REVIEW")
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


def test_worker_shadow_mode_still_runs_current_projection(monkeypatch):
    _wire_authoritative_success(monkeypatch)
    projected = []
    monkeypatch.setattr(worker, "engine2_mode", lambda: "SHADOW")
    monkeypatch.setattr(worker, "project_assurance_document_to_us_lacey", lambda **kwargs: (projected.append(kwargs), SimpleNamespace(projected_count=4, conflict_count=2))[1])
    monkeypatch.setattr(worker, "_shadow_engine2", lambda **_: None)
    worker.process_one_us_lacey_job(worker_id="unit")
    assert len(projected) == 1


def test_worker_completes_and_refreshes_before_shadow(monkeypatch):
    job = _wire_authoritative_success(monkeypatch); events = []
    monkeypatch.setattr(worker, "engine2_mode", lambda: "SHADOW")
    monkeypatch.setattr(worker, "project_assurance_document_to_us_lacey", lambda **_: (events.append("projection"), SimpleNamespace(projected_count=4, conflict_count=2))[1])
    monkeypatch.setattr(worker, "complete_us_lacey_job", lambda **_: (events.append("complete"), True)[1])
    monkeypatch.setattr(worker, "_refresh_operation", lambda **_: (events.append("refresh"), "READY_FOR_REVIEW")[1])
    monkeypatch.setattr(worker, "_shadow_engine2", lambda **_: events.append("shadow"))
    result = worker.process_one_us_lacey_job(worker_id="unit")
    assert events == ["projection", "complete", "refresh", "shadow"]
    assert (result.job_status, result.projected_count, result.conflict_count) == ("COMPLETED", 4, 2)


def test_worker_does_not_shadow_when_authoritative_completion_fails(monkeypatch):
    _wire_authoritative_success(monkeypatch); calls = []
    monkeypatch.setattr(worker, "engine2_mode", lambda: "SHADOW")
    monkeypatch.setattr(worker, "complete_us_lacey_job", lambda **_: False)
    monkeypatch.setattr(worker, "_shadow_engine2", lambda **_: calls.append("shadow"))
    monkeypatch.setattr(worker, "fail_us_lacey_job", lambda **_: "FAILED")
    result = worker.process_one_us_lacey_job(worker_id="unit")
    assert result.job_status == "FAILED" and calls == []
