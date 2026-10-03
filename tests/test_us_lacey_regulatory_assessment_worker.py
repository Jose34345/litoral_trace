from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

from litoral_trace.us_lacey import worker
from litoral_trace.us_lacey.source_sets import SourceSetClaim


class _ProcessingService:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def process(self, **_: object) -> str:
        self.calls.append("process")
        return "PROCESSED"


def _configure(monkeypatch, calls: list[str]) -> SourceSetClaim:
    job = SimpleNamespace(id=11, organization_id=7, operation_id=22, assurance_document_id=33)
    claim = SourceSetClaim(
        revision_id=9,
        generation=2,
        fingerprint="f" * 64,
        claimed=True,
        reason="CLAIMED",
        claimed_at=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(worker, "claim_next_us_lacey_job", lambda **_: job)
    monkeypatch.setattr(worker, "_assurance_public_id", lambda **_: uuid4())
    monkeypatch.setattr(worker, "_document_descriptor", lambda **_: SimpleNamespace(filename="bom.csv", size_bytes=128, vault_public_id=uuid4()))
    monkeypatch.setattr(worker, "_preflight_existing_document", lambda **_: None)
    monkeypatch.setattr(worker, "_processing_service", lambda: _ProcessingService(calls))
    monkeypatch.setattr(worker, "us_lacey_operation_projection_lock", lambda **_: nullcontext())
    monkeypatch.setattr(worker, "project_assurance_document_to_us_lacey", lambda **_: calls.append("project") or SimpleNamespace(projected_count=2, conflict_count=0))
    monkeypatch.setattr(worker, "_claim_source_set_finalization", lambda **_: claim)
    monkeypatch.setattr(worker, "_shadow_engine2", lambda **_: calls.append("engine2"))
    monkeypatch.setattr(worker, "_project_verified_ai_suggestions", lambda **_: calls.append("ai") or 0)
    monkeypatch.setattr(worker, "_project_engine2_suggestions", lambda **_: calls.append("canonical") or 0)
    monkeypatch.setattr(worker, "_build_product_intelligence_snapshot", lambda **_: calls.append("pi"))
    monkeypatch.setattr(worker, "_shadow_multilingual_evidence_snapshot", lambda **_: calls.append("multilingual"))
    monkeypatch.setattr(worker, "finalize_claim", lambda **_: calls.append("finalize") or True)
    monkeypatch.setattr(worker, "complete_us_lacey_job", lambda **_: calls.append("complete") or True)
    monkeypatch.setattr(worker, "_refresh_operation", lambda **_: calls.append("refresh") or "READY_FOR_REVIEW")
    monkeypatch.setattr(worker, "_run_ai_review_recommendations", lambda **_: calls.append("review"))
    monkeypatch.setattr(worker, "fail_us_lacey_job", lambda **_: (_ for _ in ()).throw(AssertionError("success path must not fail")))
    return claim


def test_worker_runs_regulatory_assessment_after_product_intelligence_before_multilingual(monkeypatch):
    calls: list[str] = []
    claim = _configure(monkeypatch, calls)
    monkeypatch.setattr(
        worker,
        "_build_regulatory_assessment_snapshot",
        lambda **kwargs: calls.append(f"rules:{kwargs['claim'].revision_id}:{kwargs['claim'].generation}"),
        raising=False,
    )

    result = worker.process_one_us_lacey_job(worker_id="rules-worker")

    token = f"rules:{claim.revision_id}:{claim.generation}"
    assert result.job_status == "COMPLETED"
    assert calls.count(token) == 1
    assert calls.index("pi") < calls.index(token) < calls.index("multilingual")
    assert calls.index("finalize") < calls.index("complete")


def test_regulatory_builder_failure_is_fail_closed_but_does_not_corrupt_canonical_job(monkeypatch):
    calls: list[str] = []
    _configure(monkeypatch, calls)

    def explode(**_: object):
        calls.append("rules-attempt")
        raise RuntimeError("synthetic regulatory assessment failure")

    monkeypatch.setattr(worker, "build_regulatory_assessment_snapshot", explode, raising=False)

    result = worker.process_one_us_lacey_job(worker_id="rules-worker")

    assert result.job_status == "COMPLETED"
    assert "rules-attempt" in calls
    assert "finalize" in calls
    assert "complete" in calls
