from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace
from uuid import uuid4

from litoral_trace.lacey_engine.errors import UnsupportedDocumentDomainError
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
    def shadow_engine2(**_: object) -> SimpleNamespace:
        calls.append("engine2")
        return SimpleNamespace(
            status="SUCCEEDED",
            shipment_run_id=91,
            succeeded_document_count=3,
            failed_document_count=0,
        )

    monkeypatch.setattr(worker, "_shadow_engine2", shadow_engine2)

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
    monkeypatch.setattr(worker, "_preflight_engine2_domain", lambda **_: None)
    monkeypatch.setattr(worker, "us_lacey_operation_projection_lock", lambda **_: nullcontext())
    monkeypatch.setattr(worker, "_claim_source_set_finalization", lambda **_: SimpleNamespace(claimed=False, fingerprint=None))
    monkeypatch.setattr(worker, "_shadow_multilingual_evidence_snapshot", lambda **_: calls.append("snapshot"))
    monkeypatch.setattr(worker, "complete_us_lacey_job", lambda **_: calls.append("complete") or True)
    monkeypatch.setattr(worker, "_refresh_operation", lambda **_: calls.append("refresh") or "PROCESSING")
    monkeypatch.setattr(worker, "fail_us_lacey_job", lambda **_: (_ for _ in ()).throw(AssertionError()))

    result = worker.process_one_us_lacey_job(worker_id="worker-test")

    assert calls == ["process", "project", "complete", "refresh"]
    assert result.operation_status == "PROCESSING"


def test_partial_engine2_shadow_does_not_fail_authoritative_worker_job(monkeypatch) -> None:
    calls: list[str] = []
    _stub_success_path(monkeypatch, calls)

    def partial_shadow(**_: object) -> SimpleNamespace:
        calls.append("engine2_partial")
        return SimpleNamespace(
            status="BLOCKED_PARTIAL",
            shipment_run_id=None,
            succeeded_document_count=2,
            failed_document_count=1,
        )

    def forbidden_canonical(**_: object) -> int:
        raise AssertionError("canonical publication must be skipped without a shipment run")

    monkeypatch.setattr(worker, "_shadow_engine2", partial_shadow)
    monkeypatch.setattr(worker, "_project_engine2_suggestions", forbidden_canonical)
    monkeypatch.setattr(worker, "complete_us_lacey_job", lambda **_: calls.append("complete") or True)
    monkeypatch.setattr(
        worker,
        "_refresh_operation",
        lambda **_: calls.append("refresh") or "REVIEW_REQUIRED",
    )
    monkeypatch.setattr(
        worker,
        "fail_us_lacey_job",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("partial Engine 2 shadow must not fail the owned job")
        ),
    )

    result = worker.process_one_us_lacey_job(worker_id="worker-test")

    assert calls == [
        "process",
        "project",
        "candidate_equivalence",
        "engine2_partial",
        "ai_suggestions",
        "complete",
        "refresh",
        "ai_review",
    ]
    assert result.claimed is True
    assert result.job_status == "COMPLETED"
    assert result.operation_status == "REVIEW_REQUIRED"
    assert result.document_status == "PROCESSED"



def test_worker_rejects_unsupported_domain_before_processing_or_projection(monkeypatch):
    calls: list[str] = []
    job = _job()
    monkeypatch.setattr(worker, "claim_next_us_lacey_job", lambda **_: job)
    monkeypatch.setattr(worker, "_assurance_public_id", lambda **_: uuid4())
    monkeypatch.setattr(
        worker,
        "_document_descriptor",
        lambda **_: SimpleNamespace(
            filename="initial-decision.pdf",
            size_bytes=1024,
            vault_public_id=uuid4(),
        ),
    )
    monkeypatch.setattr(worker, "_preflight_existing_document", lambda **_: calls.append("budget"))

    def reject(**_kwargs):
        calls.append("domain")
        raise UnsupportedDocumentDomainError(domain="LEGAL_DECISION")

    monkeypatch.setattr(worker, "_preflight_engine2_domain", reject)
    monkeypatch.setattr(
        worker,
        "_processing_service",
        lambda: (_ for _ in ()).throw(
            AssertionError("Assurance processing must not run for rejected domain")
        ),
    )
    monkeypatch.setattr(
        worker,
        "project_assurance_document_to_us_lacey",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("projection must not run for rejected domain")
        ),
    )

    observed: dict[str, object] = {}

    def fail(**kwargs):
        observed.update(kwargs)
        return "FAILED"

    monkeypatch.setattr(worker, "fail_us_lacey_job", fail)

    result = worker.process_one_us_lacey_job(worker_id="worker-domain-test")

    assert calls == ["budget", "domain"]
    assert observed["error_code"] == "UNSUPPORTED_DOMAIN"
    assert observed["retryable"] is False
    assert result.job_status == "FAILED"
    assert result.document_status == "FAILED"
    assert result.operation_status == "FAILED"
    assert result.projected_count == 0
    assert result.conflict_count == 0
