"""Workflow contract for source-set mutation/finalization serialization."""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from uuid import uuid4

from litoral_trace.us_lacey import workflow


class _Operations:
    def get_internal_id(self, **_: object) -> int:
        return 91


class _Ingestion:
    def __init__(self, events: list[tuple[str, object]]) -> None:
        self.events = events
        self.sequence = 0

    def ingest_document(self, *, filename: str, **_: object):
        self.sequence += 1
        self.events.append(("ingest", filename))
        return SimpleNamespace(assurance_document_id=self.sequence)


def _install_workflow_seams(monkeypatch, events: list[tuple[str, object]]) -> None:
    @contextmanager
    def operation_lock(**kwargs):
        events.append(("lock_enter", kwargs["operation_id"]))
        try:
            yield
        finally:
            events.append(("lock_exit", kwargs["operation_id"]))

    monkeypatch.setattr(
        workflow,
        "us_lacey_operation_projection_lock",
        operation_lock,
        raising=False,
    )
    monkeypatch.setattr(workflow, "require_us_lacey_operational_access", lambda **_: None)
    monkeypatch.setattr(workflow, "enforce_shipment_document_budget", lambda **_: None)
    monkeypatch.setattr(
        workflow,
        "seal_current_source_set",
        lambda **_: events.append(("seal", 91)) or SimpleNamespace(id=1),
    )
    monkeypatch.setattr(
        workflow,
        "enqueue_us_lacey_document_job",
        lambda *, assurance_document_id, **_: (
            events.append(("enqueue", assurance_document_id))
            or SimpleNamespace(id=assurance_document_id, status="QUEUED")
        ),
    )
    monkeypatch.setattr(
        workflow,
        "_mark_operation_processing",
        lambda **_: events.append(("processing", 91)),
    )


def _sandbox_capacity_guard(events: list[tuple[str, object]]):
    def guard(**kwargs):
        events.append(("sandbox_capacity", kwargs["operation_id"]))

    return guard


def test_single_upload_mutation_is_one_operation_locked_critical_section(monkeypatch) -> None:
    events: list[tuple[str, object]] = []
    _install_workflow_seams(monkeypatch, events)

    result = workflow.upload_and_enqueue_us_lacey_document(
        organization_id=3,
        user_id=4,
        operation_public_id=uuid4(),
        filename="single.pdf",
        content_type="application/pdf",
        content=b"single",
        document_role="BILL_OF_LADING",
        ingestion=_Ingestion(events),
        operations=_Operations(),
        sandbox_capacity_guard=_sandbox_capacity_guard(events),
    )

    assert result.job.status == "QUEUED"
    assert events == [
        ("lock_enter", 91),
        ("sandbox_capacity", 91),
        ("ingest", "single.pdf"),
        ("seal", 91),
        ("enqueue", 1),
        ("processing", 91),
        ("lock_exit", 91),
    ]


def test_batch_upload_mutation_is_one_operation_locked_critical_section(monkeypatch) -> None:
    events: list[tuple[str, object]] = []
    _install_workflow_seams(monkeypatch, events)

    queued = workflow.upload_and_enqueue_us_lacey_document_batch(
        organization_id=3,
        user_id=4,
        operation_public_id=uuid4(),
        documents=(
            ("first.pdf", "application/pdf", b"first", "UNKNOWN"),
            ("second.pdf", "application/pdf", b"second", "UNKNOWN"),
        ),
        ingestion=_Ingestion(events),
        operations=_Operations(),
        sandbox_capacity_guard=_sandbox_capacity_guard(events),
    )

    assert len(queued) == 2
    assert events == [
        ("lock_enter", 91),
        ("sandbox_capacity", 91),
        ("ingest", "first.pdf"),
        ("ingest", "second.pdf"),
        ("seal", 91),
        ("enqueue", 1),
        ("enqueue", 2),
        ("processing", 91),
        ("lock_exit", 91),
    ]
