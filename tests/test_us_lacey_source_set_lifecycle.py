from types import SimpleNamespace
from uuid import uuid4

from litoral_trace.us_lacey import workflow


def test_seven_document_batch_seals_all_members_before_any_job_is_eligible(monkeypatch):
    """A worker cannot observe a 1..6-document prefix of one HTTP intake batch."""
    events: list[tuple[str, int | None]] = []

    class Operations:
        def get_internal_id(self, **_):
            return 91

    class Ingestion:
        def ingest_document(self, *, filename, **_):
            sequence = int(filename.removeprefix("doc-").removesuffix(".pdf"))
            events.append(("ingest", sequence))
            return SimpleNamespace(assurance_document_id=sequence)

    monkeypatch.setattr(
        workflow,
        "seal_current_source_set",
        lambda **_: events.append(("seal", None)) or SimpleNamespace(id=7, generation=1),
    )
    monkeypatch.setattr(
        workflow,
        "enqueue_us_lacey_document_job",
        lambda *, assurance_document_id, **_: events.append(("enqueue", assurance_document_id)) or SimpleNamespace(id=assurance_document_id),
    )
    monkeypatch.setattr(workflow, "_mark_operation_processing", lambda **_: events.append(("processing", None)))

    queued = workflow.upload_and_enqueue_us_lacey_document_batch(
        organization_id=3,
        user_id=4,
        operation_public_id=uuid4(),
        documents=tuple((f"doc-{number}.pdf", "application/pdf", b"x", "UNKNOWN") for number in range(1, 8)),
        ingestion=Ingestion(),
        operations=Operations(),
    )

    assert [item.ingestion.assurance_document_id for item in queued] == list(range(1, 8))
    assert events == [
        *(("ingest", number) for number in range(1, 8)),
        ("seal", None),
        *(("enqueue", number) for number in range(1, 8)),
        ("processing", None),
    ]
