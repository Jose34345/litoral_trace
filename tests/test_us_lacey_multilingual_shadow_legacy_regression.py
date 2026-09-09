from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace
from uuid import UUID

from litoral_trace.us_lacey import worker


class _ProcessingService:
    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    def process(self, **_: object) -> str:
        self._calls.append("process")
        return "NEEDS_REVIEW"


def _run_authoritative_path(monkeypatch, *, shadow_enabled: bool):
    legacy_calls: list[str] = []
    shadow_calls: list[str] = []
    assurance_public_id = UUID("11111111-1111-1111-1111-111111111111")
    job = SimpleNamespace(
        id=91,
        organization_id=17,
        operation_id=23,
        assurance_document_id=29,
    )

    if shadow_enabled:
        monkeypatch.setenv("LT_LACEY_MULTILINGUAL_SHADOW", "1")
    else:
        monkeypatch.setenv("LT_LACEY_MULTILINGUAL_SHADOW", "0")

    monkeypatch.setattr(worker, "claim_next_us_lacey_job", lambda **_: job)
    monkeypatch.setattr(worker, "_assurance_public_id", lambda **_: assurance_public_id)
    monkeypatch.setattr(
        worker,
        "_document_descriptor",
        lambda **_: SimpleNamespace(
            assurance_public_id=assurance_public_id,
            vault_public_id=UUID("22222222-2222-2222-2222-222222222222"),
            filename="invoice.pdf",
            size_bytes=1024,
        ),
    )
    monkeypatch.setattr(worker, "_preflight_existing_document", lambda **_: legacy_calls.append("preflight"))
    monkeypatch.setattr(worker, "_processing_service", lambda: _ProcessingService(legacy_calls))
    monkeypatch.setattr(worker, "us_lacey_operation_projection_lock", lambda **_: nullcontext())

    def project(**_: object):
        legacy_calls.append("project")
        return SimpleNamespace(projected_count=4, conflict_count=2)

    monkeypatch.setattr(worker, "project_assurance_document_to_us_lacey", project)
    monkeypatch.setattr(worker, "_shadow_engine2", lambda **_: legacy_calls.append("engine2"))
    monkeypatch.setattr(worker, "_project_engine2_suggestions", lambda **_: legacy_calls.append("engine2_suggestions") or 0)
    monkeypatch.setattr(worker, "_project_verified_ai_suggestions", lambda **_: legacy_calls.append("ai_suggestions") or 0)
    monkeypatch.setattr(worker, "_run_ai_review_recommendations", lambda **_: legacy_calls.append("ai_review"))

    def failing_shadow(**_: object):
        shadow_calls.append("shadow_attempt")
        raise RuntimeError("shadow storage unavailable")

    monkeypatch.setattr(worker, "build_shadow_evidence_snapshot", failing_shadow)
    monkeypatch.setattr(worker, "complete_us_lacey_job", lambda **_: legacy_calls.append("complete") or True)
    monkeypatch.setattr(worker, "_refresh_operation", lambda **_: legacy_calls.append("refresh") or "REVIEW_REQUIRED")
    monkeypatch.setattr(
        worker,
        "fail_us_lacey_job",
        lambda **_: (_ for _ in ()).throw(AssertionError("shadow must never fail the legacy job")),
    )

    result = worker.process_one_us_lacey_job(worker_id="phase-b-regression")
    return result, legacy_calls, shadow_calls


def test_legacy_projection_and_operation_result_are_identical_with_shadow_off_and_on(monkeypatch):
    off_result, off_legacy_calls, off_shadow_calls = _run_authoritative_path(
        monkeypatch,
        shadow_enabled=False,
    )
    on_result, on_legacy_calls, on_shadow_calls = _run_authoritative_path(
        monkeypatch,
        shadow_enabled=True,
    )

    assert off_result == on_result
    assert off_result.job_status == "COMPLETED"
    assert off_result.document_status == "NEEDS_REVIEW"
    assert off_result.operation_status == "REVIEW_REQUIRED"
    assert off_result.projected_count == 4
    assert off_result.conflict_count == 2

    assert off_legacy_calls == on_legacy_calls == [
        "preflight",
        "process",
        "project",
        "engine2",
        "engine2_suggestions",
        "ai_suggestions",
        "ai_review",
        "complete",
        "refresh",
    ]
    assert off_shadow_calls == []
    assert on_shadow_calls == ["shadow_attempt"]
