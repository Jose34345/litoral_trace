from __future__ import annotations

from contextlib import nullcontext
import logging
from types import SimpleNamespace
from uuid import uuid4

from litoral_trace.us_lacey import worker


class _ProcessingService:
    def process(self, **_: object) -> str:
        return "PROCESSED"


class _NoopHeartbeat:
    def __init__(self, **_: object) -> None:
        pass

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass


def test_worker_emits_structured_stage_durations_for_finalizing_job(monkeypatch, caplog) -> None:
    job = SimpleNamespace(
        id=11,
        organization_id=7,
        operation_id=22,
        assurance_document_id=33,
    )
    fingerprint = "f" * 64

    monkeypatch.setattr(worker, "claim_next_us_lacey_job", lambda **_: job)
    monkeypatch.setattr(worker, "_UsLaceyJobHeartbeat", _NoopHeartbeat)
    monkeypatch.setattr(worker, "_assurance_public_id", lambda **_: uuid4())
    monkeypatch.setattr(
        worker,
        "_document_descriptor",
        lambda **_: SimpleNamespace(filename="document.pdf", size_bytes=1, vault_public_id=uuid4()),
    )
    monkeypatch.setattr(worker, "_preflight_existing_document", lambda **_: None)
    monkeypatch.setattr(worker, "_processing_service", lambda: _ProcessingService())
    monkeypatch.setattr(worker, "us_lacey_operation_projection_lock", lambda **_: nullcontext())
    monkeypatch.setattr(
        worker,
        "project_assurance_document_to_us_lacey",
        lambda **_: SimpleNamespace(projected_count=2, conflict_count=0),
    )
    monkeypatch.setattr(
        worker,
        "_claim_source_set_finalization",
        lambda **_: SimpleNamespace(claimed=True, fingerprint=fingerprint),
    )
    monkeypatch.setattr(worker, "_shadow_engine2", lambda **_: None)
    monkeypatch.setattr(worker, "_project_engine2_suggestions", lambda **_: 0)
    monkeypatch.setattr(worker, "_project_verified_ai_suggestions", lambda **_: 0)
    monkeypatch.setattr(worker, "_run_ai_review_recommendations", lambda **_: None)
    monkeypatch.setattr(worker, "_shadow_multilingual_evidence_snapshot", lambda **_: None)
    monkeypatch.setattr(worker, "finalize_claim", lambda **_: True)
    monkeypatch.setattr(worker, "complete_us_lacey_job", lambda **_: True)
    monkeypatch.setattr(worker, "_refresh_operation", lambda **_: "READY_FOR_REVIEW")
    monkeypatch.setattr(
        worker,
        "fail_us_lacey_job",
        lambda **_: (_ for _ in ()).throw(AssertionError("success path must not fail")),
    )

    caplog.set_level(logging.INFO, logger=worker.LOGGER.name)
    result = worker.process_one_us_lacey_job(worker_id="timing-test")
    assert result.job_status == "COMPLETED"

    timing_records = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "us_lacey_stage_timing"
    ]
    by_stage = {getattr(record, "stage", None): record for record in timing_records}
    expected_stages = {
        "preflight",
        "document_processing",
        "authoritative_projection",
        "engine2_shadow",
        "canonical_publication",
        "product_intelligence",
        "verified_ai_suggestions",
        "ai_review_recommendations",
        "multilingual_snapshot",
        "source_set_finalize",
        "queue_complete",
        "operation_refresh",
        "total",
    }
    assert set(by_stage) == expected_stages

    for stage, record in by_stage.items():
        assert record.organization_id == job.organization_id, stage
        assert record.operation_id == job.operation_id, stage
        assert record.job_id == job.id, stage
        assert isinstance(record.duration_ms, float), stage
        assert record.duration_ms >= 0.0, stage

    assert by_stage["engine2_shadow"].source_set_fingerprint == fingerprint
    assert by_stage["source_set_finalize"].source_set_fingerprint == fingerprint
    assert by_stage["total"].job_status == "COMPLETED"
