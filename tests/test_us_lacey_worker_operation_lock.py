from contextlib import contextmanager
from types import SimpleNamespace
from uuid import uuid4

from litoral_trace.us_lacey import worker


def test_worker_holds_operation_lock_across_projection_postprocessors(monkeypatch):
    events: list[str] = []
    job = SimpleNamespace(
        id=41,
        organization_id=7,
        operation_id=9,
        assurance_document_id=13,
    )
    descriptor = SimpleNamespace(
        assurance_public_id=uuid4(),
        vault_public_id=uuid4(),
        filename="shipment.pdf",
        size_bytes=1200,
    )

    monkeypatch.setattr(worker, "claim_next_us_lacey_job", lambda **_kwargs: job)
    monkeypatch.setattr(worker, "_assurance_public_id", lambda **_kwargs: descriptor.assurance_public_id)
    monkeypatch.setattr(worker, "_document_descriptor", lambda **_kwargs: descriptor)
    monkeypatch.setattr(worker, "_preflight_existing_document", lambda **_kwargs: None)
    monkeypatch.setattr(
        worker,
        "_processing_service",
        lambda: SimpleNamespace(process=lambda **_kwargs: "EXTRACTED"),
    )

    @contextmanager
    def projection_lock(**kwargs):
        assert kwargs == {"organization_id": 7, "operation_id": 9}
        events.append("lock-enter")
        try:
            yield
        finally:
            events.append("lock-exit")

    monkeypatch.setattr(worker, "us_lacey_operation_projection_lock", projection_lock)

    def project(**_kwargs):
        events.append("project")
        return SimpleNamespace(projected_count=5, conflict_count=0)

    monkeypatch.setattr(worker, "project_assurance_document_to_us_lacey", project)
    monkeypatch.setattr(worker, "_shadow_engine2", lambda **_kwargs: events.append("engine2-shadow"))
    monkeypatch.setattr(
        worker,
        "_project_engine2_suggestions",
        lambda **_kwargs: events.append("engine2-suggestions") or 0,
    )
    monkeypatch.setattr(
        worker,
        "_project_verified_ai_suggestions",
        lambda **_kwargs: events.append("ai-suggestions") or 0,
    )
    monkeypatch.setattr(
        worker,
        "_run_ai_review_recommendations",
        lambda **_kwargs: events.append("ai-review"),
    )
    monkeypatch.setattr(
        worker,
        "complete_us_lacey_job",
        lambda **_kwargs: events.append("complete") or True,
    )
    monkeypatch.setattr(
        worker,
        "_refresh_operation",
        lambda **_kwargs: events.append("refresh") or "READY_FOR_REVIEW",
    )

    result = worker.process_one_us_lacey_job(worker_id="worker-lock-test")

    assert result.job_status == "COMPLETED"
    assert result.projected_count == 5
    assert events == [
        "lock-enter",
        "project",
        "engine2-shadow",
        "engine2-suggestions",
        "ai-suggestions",
        "ai-review",
        "lock-exit",
        "complete",
        "refresh",
    ]
