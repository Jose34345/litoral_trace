"""PostgreSQL acceptance for mutation/finalization serialization."""
from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import select

from litoral_trace.db.models import (
    AssuranceDocument,
    UsLaceyOperation,
    UsLaceySourceSetRevision,
    VaultDocument,
)
from litoral_trace.us_lacey import operation_lock, workflow
from litoral_trace.us_lacey.operations import UsLaceyOperationService
from litoral_trace.us_lacey.source_sets import seal_current_source_set
from tests.us_lacey_engine2_postgres import (
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


def _add_unattached_document(factory, *, organization_id: int) -> int:
    session = tenant_session(factory, organization_id)
    content = b"source-set-mutation-n-plus-one"
    vault = VaultDocument(
        organization_id=organization_id,
        original_filename="source-set-mutation-n-plus-one.pdf",
        content_type="application/pdf",
        size_bytes=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        object_key=f"tests/{uuid4().hex}",
        storage_backend="s3",
        storage_bucket="tests",
        document_type="OTHER_EVIDENCE",
        status="available",
    )
    session.add(vault)
    session.flush()
    assurance = AssuranceDocument(
        organization_id=organization_id,
        vault_document_id=vault.id,
        semantic_document_type="UNKNOWN",
        processing_status="EXTRACTED",
    )
    session.add(assurance)
    session.commit()
    assurance_id = int(assurance.id)
    session.close()
    return assurance_id


def test_batch_mutation_waits_for_worker_operation_lock_before_attach_and_seal(
    monkeypatch,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
) -> None:
    """A source mutation cannot interleave with operation-level finalization."""
    org, operation_id, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        role="BILL_OF_LADING",
        content=b"source-set-mutation-n",
    )
    revision_n = seal_current_source_set(
        organization_id=org,
        operation_id=operation_id,
        session_factory=engine2_postgres_session_factory,
    )
    replacement_assurance_id = _add_unattached_document(
        engine2_postgres_session_factory,
        organization_id=org,
    )
    operation_service = UsLaceyOperationService(
        session_factory=engine2_postgres_session_factory,
    )
    session = tenant_session(engine2_postgres_session_factory, org)
    operation_public_id = session.get(UsLaceyOperation, operation_id).public_id
    session.close()

    # Both the simulated worker guard and the production workflow guard must use
    # the same isolated PostgreSQL database/session factory in this acceptance test.
    monkeypatch.setattr(
        operation_lock,
        "get_us_lacey_db_session",
        engine2_postgres_session_factory,
    )
    monkeypatch.setattr(
        workflow,
        "seal_current_source_set",
        lambda **kwargs: seal_current_source_set(
            **kwargs,
            session_factory=engine2_postgres_session_factory,
        ),
    )
    monkeypatch.setattr(
        workflow,
        "enqueue_us_lacey_document_job",
        lambda **_: SimpleNamespace(id=991, status="QUEUED"),
    )
    monkeypatch.setattr(workflow, "_mark_operation_processing", lambda **_: None)

    workflow_started = Event()
    ingestion_entered = Event()
    lock_acquired = Event()
    release_lock = Event()

    class RealAttachmentIngestion:
        def ingest_document(self, *, document_role: str, **_: object):
            ingestion_entered.set()
            link_id = operation_service.attach_document(
                organization_id=org,
                operation_public_id=operation_public_id,
                assurance_document_id=replacement_assurance_id,
                document_role=document_role,
            )
            return SimpleNamespace(
                assurance_document_id=replacement_assurance_id,
                operation_document_link_id=link_id,
            )

    def hold_worker_finalization_lock() -> None:
        with operation_lock.us_lacey_operation_projection_lock(
            organization_id=org,
            operation_id=operation_id,
        ):
            lock_acquired.set()
            assert release_lock.wait(timeout=15), "test did not release worker operation lock"

    def mutate_source_set():
        workflow_started.set()
        return workflow.upload_and_enqueue_us_lacey_document_batch(
            organization_id=org,
            user_id=1,
            operation_public_id=operation_public_id,
            documents=((
                "source-set-mutation-n-plus-one.pdf",
                "application/pdf",
                b"source-set-mutation-n-plus-one",
                "BILL_OF_LADING",
            ),),
            ingestion=RealAttachmentIngestion(),
            operations=operation_service,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        holder = pool.submit(hold_worker_finalization_lock)
        assert lock_acquired.wait(timeout=10), "worker operation lock was not acquired"
        mutation = pool.submit(mutate_source_set)
        assert workflow_started.wait(timeout=10), "mutation workflow did not start"
        try:
            mutation_was_blocked = not ingestion_entered.wait(timeout=3)
        finally:
            release_lock.set()
            holder.result(timeout=20)
            queued = mutation.result(timeout=20)

    assert mutation_was_blocked, (
        "source-set batch entered attach_document while worker finalization held the operation lock"
    )
    assert ingestion_entered.is_set()
    assert queued[0].ingestion.assurance_document_id == replacement_assurance_id

    session = tenant_session(engine2_postgres_session_factory, org)
    current = session.scalar(
        select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == org,
            UsLaceySourceSetRevision.operation_id == operation_id,
            UsLaceySourceSetRevision.is_current.is_(True),
        )
    )
    historical_n = session.get(UsLaceySourceSetRevision, revision_n.id)
    assert current is not None
    assert current.id != revision_n.id
    assert current.generation == historical_n.generation + 1
    assert historical_n.is_current is False
    session.close()
