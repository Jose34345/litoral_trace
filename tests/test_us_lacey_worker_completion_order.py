from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace
from uuid import uuid4

from litoral_trace.us_lacey import worker


class _ProcessingService:
    def __init__(self, calls: list[str], *, status: str = "PROCESSED") -> None:
        self._calls = calls
        self._status = status

    def process(self, **_: object) -> str:
        self._calls.append("process")
        return self._status


def _job() -> SimpleNamespace:
    return SimpleNamespace(
        id=11,
        organization_id=7,
        operation_id=22,
        assurance_document_id=33,
    )


def _stub_success_path(monkeypatch, calls: list[str]) -> None:
    monkeypatch.setattr(worker, "claim_next_us_lacey_job", lambda **_: _job())
    # A non-UUID test id deliberately bypasses the Vault/preflight branch while
    # preserving the public-id lookup seam used by the worker.
    monkeypatch.setattr(worker, "_assurance_public_id", lambda **_: "doc-public-id")
    monkeypatch.setattr(worker, "_processing_service", lambda: _ProcessingService(calls))

    def project(**_: object) -> SimpleNamespace:
        calls.append("project")
        return SimpleNamespace(projected_count=3, conflict_count=1)

    monkeypatch.setattr(worker, "project_assurance_document_to_us_lacey", project)
    monkeypatch.setattr(
        worker,
        "_reconcile_candidate_equivalence",
        lambda **_: (calls.append("candidate_equivalence"), 0)[1],
    )
    monkeypatch.setattr(worker, "_shadow_engine2", lambda **_: calls.append("engine2"))

    def engine2_suggestions(**_: object) -> int:
        calls.append("engine2_suggestions")
        return 0

    def ai_suggestions(**_: object) -> int:
        calls.append("ai_suggestions")
        return 0

    monkeypatch.setattr(worker, "_project_engine2_suggestions", engine2_suggestions)
    monkeypatch.setattr(worker, "_project_verified_ai_suggestions", ai_suggestions)
    monkeypatch.setattr(worker, "_run_ai_review_recommendations", lambda **_: calls.append("ai_review"))


def test_worker_marks_customer_visible_completion_before_non_authoritative_ai_review(monkeypatch) -> None:
    calls: list[str] = []
    _stub_success_path(monkeypatch, calls)

    def complete(**_: object) -> bool:
        calls.append("complete")
        return True

    def refresh(**_: object) -> str:
        calls.append("refresh")
        return "READY_FOR_REVIEW"

    monkeypatch.setattr(worker, "complete_us_lacey_job", complete)
    monkeypatch.setattr(worker, "_refresh_operation", refresh)
    monkeypatch.setattr(
        worker,
        "fail_us_lacey_job",
        lambda **_: (_ for _ in ()).throw(AssertionError("success path must not fail the job")),
    )

    result = worker.process_one_us_lacey_job(worker_id="worker-test")

    assert calls == [
        "process",
        "project",
        "candidate_equivalence",
        "engine2",
        "ai_suggestions",
        "engine2_suggestions",
        "complete",
        "refresh",
        "ai_review",
    ]
    assert result.claimed is True
    assert result.job_status == "COMPLETED"
    assert result.document_status == "PROCESSED"
    assert result.operation_status == "READY_FOR_REVIEW"
    assert result.projected_count == 3
    assert result.conflict_count == 1


def test_post_completion_ai_review_failure_cannot_requeue_completed_job(monkeypatch) -> None:
    calls: list[str] = []
    _stub_success_path(monkeypatch, calls)

    def failing_review(**_: object) -> None:
        calls.append("ai_review")
        raise RuntimeError("unexpected review wrapper failure")

    def complete(**_: object) -> bool:
        calls.append("complete")
        return True

    def fail(**_: object) -> str:
        calls.append("fail")
        return "QUEUED"

    def refresh(**_: object) -> str:
        calls.append("refresh")
        return "READY_FOR_REVIEW"

    monkeypatch.setattr(worker, "_run_ai_review_recommendations", failing_review)
    monkeypatch.setattr(worker, "complete_us_lacey_job", complete)
    monkeypatch.setattr(worker, "fail_us_lacey_job", fail)
    monkeypatch.setattr(worker, "_refresh_operation", refresh)

    result = worker.process_one_us_lacey_job(worker_id="worker-test")

    assert calls == [
        "process",
        "project",
        "candidate_equivalence",
        "engine2",
        "ai_suggestions",
        "engine2_suggestions",
        "complete",
        "refresh",
        "ai_review",
    ]
    assert "fail" not in calls
    assert result.claimed is True
    assert result.job_status == "COMPLETED"
    assert result.operation_status == "READY_FOR_REVIEW"
    assert result.document_status == "PROCESSED"
    assert result.projected_count == 3
    assert result.conflict_count == 1


def test_worker_defers_operation_ai_until_every_current_source_is_terminal(monkeypatch) -> None:
    calls: list[str] = []
    _stub_success_path(monkeypatch, calls)
    monkeypatch.setattr(worker, "_assurance_public_id", lambda **_: uuid4())
    monkeypatch.setattr(
        worker,
        "_document_descriptor",
        lambda **_: SimpleNamespace(filename="document.pdf", size_bytes=1, vault_public_id=uuid4()),
    )
    monkeypatch.setattr(worker, "_preflight_existing_document", lambda **_: None)
    monkeypatch.setattr(worker, "us_lacey_operation_projection_lock", lambda **_: nullcontext())
    monkeypatch.setattr(worker, "_claim_source_set_finalization", lambda **_: SimpleNamespace(claimed=False, fingerprint=None))
    monkeypatch.setattr(worker, "_shadow_multilingual_evidence_snapshot", lambda **_: calls.append("snapshot"))
    monkeypatch.setattr(worker, "complete_us_lacey_job", lambda **_: calls.append("complete") or True)
    monkeypatch.setattr(worker, "_refresh_operation", lambda **_: calls.append("refresh") or "PROCESSING")
    monkeypatch.setattr(worker, "fail_us_lacey_job", lambda **_: (_ for _ in ()).throw(AssertionError()))

    result = worker.process_one_us_lacey_job(worker_id="worker-test")

    assert calls == ["process", "project", "complete", "refresh"]
    assert result.operation_status == "PROCESSING"
