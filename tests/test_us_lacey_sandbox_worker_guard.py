from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from litoral_trace.us_lacey import worker
from litoral_trace.us_lacey.jobs import UsLaceyJob


class _Heartbeat:
    def __init__(self, **_kwargs):
        self.started = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.started = False


def test_expired_sandbox_job_fails_before_document_or_ai_processing(monkeypatch):
    job = UsLaceyJob(
        id=17,
        public_id=uuid4(),
        organization_id=41,
        operation_id=9,
        assurance_document_id=5,
        status="RUNNING",
        attempt_count=1,
        max_attempts=3,
        available_at=datetime.now(timezone.utc),
        locked_by="sandbox-worker",
    )
    monkeypatch.setattr(worker, "claim_next_us_lacey_job", lambda **_kwargs: job)
    monkeypatch.setattr(worker, "_UsLaceyJobHeartbeat", _Heartbeat)
    monkeypatch.setattr(
        worker,
        "_sandbox_tenant_expired",
        lambda **_kwargs: True,
    )

    failures: list[dict[str, object]] = []

    def fail(**kwargs):
        failures.append(kwargs)
        return "FAILED"

    monkeypatch.setattr(worker, "fail_us_lacey_job", fail)
    monkeypatch.setattr(worker, "_refresh_operation", lambda **_kwargs: "FAILED")

    def processing_must_not_run():
        raise AssertionError("expired sandbox must stop before document processing")

    monkeypatch.setattr(worker, "_processing_service", processing_must_not_run)

    result = worker.process_one_us_lacey_job(worker_id="sandbox-worker")

    assert result.claimed is True
    assert result.job_id == 17
    assert result.job_status == "FAILED"
    assert result.projected_count == 0
    assert failures == [
        {
            "job_id": 17,
            "worker_id": "sandbox-worker",
            "error_code": "SANDBOX_EXPIRED",
            "safe_error_message": "This sandbox expired before processing started.",
            "retryable": False,
        }
    ]
