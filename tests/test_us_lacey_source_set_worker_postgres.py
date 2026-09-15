"""PostgreSQL acceptance for the source-set worker finalization barrier."""
from __future__ import annotations

import hashlib
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, inspect, select

from litoral_trace.db.models import (
    AssuranceDocument,
    UsLaceyOperation,
    UsLaceyOperationDocument,
    UsLaceyProcessingJob,
    UsLaceySourceSetMember,
    UsLaceySourceSetRevision,
    VaultDocument,
)
from litoral_trace.us_lacey import worker
from litoral_trace.us_lacey.jobs import enqueue_us_lacey_document_job
from litoral_trace.us_lacey.operations import UsLaceyOperationService
from litoral_trace.us_lacey.source_sets import (
    claim_ready_source_set,
    seal_current_source_set,
)
from tests.us_lacey_engine2_postgres import (
    add_test_document,
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


def _require_source_set_schema(engine) -> None:
    required = {
        "us_lacey_processing_jobs",
        "us_lacey_source_set_members",
        "us_lacey_source_set_revisions",
    }
    if not required.issubset(inspect(engine).get_table_names()):
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_048")


def _add_unattached_assurance_document(
    factory,
    *,
    organization_id: int,
    filename: str,
    content: bytes,
) -> int:
    """Create an extracted document that only the real operation service attaches."""
    session = tenant_session(factory, organization_id)
    suffix = uuid4().hex
    vault = VaultDocument(
        organization_id=organization_id,
        original_filename=filename,
        content_type="application/pdf",
        size_bytes=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        object_key=f"tests/{suffix}",
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
    assurance_document_id = assurance.id
    session.close()
    return assurance_document_id


def _install_non_target_worker_seams(monkeypatch, factory, ai_calls) -> None:
    """Keep queue/CAS/finalization real while removing parser and external work."""
    monkeypatch.setattr(
        worker,
        "_processing_service",
        lambda: SimpleNamespace(process=lambda **_: "COMPLETED"),
    )
    monkeypatch.setattr(worker, "_preflight_existing_document", lambda **_: None)
    monkeypatch.setattr(
        worker,
        "project_assurance_document_to_us_lacey",
        lambda **_: SimpleNamespace(projected_count=0, conflict_count=0),
    )
    monkeypatch.setattr(worker, "_project_engine2_suggestions", lambda **_: 0)
    monkeypatch.setattr(worker, "_project_verified_ai_suggestions", lambda **_: 0)
    monkeypatch.setattr(worker, "_run_ai_review_recommendations", lambda **_: None)
    monkeypatch.setattr(worker, "_shadow_multilingual_evidence_snapshot", lambda **_: None)

    def record_operation_ai(*, organization_id: int, operation_id: int) -> None:
        session = tenant_session(factory, organization_id)
        revision = session.scalar(
            select(UsLaceySourceSetRevision).where(
                UsLaceySourceSetRevision.organization_id == organization_id,
                UsLaceySourceSetRevision.operation_id == operation_id,
                UsLaceySourceSetRevision.is_current.is_(True),
            )
        )
        assert revision is not None
        ai_calls.append((revision.generation, revision.source_set_fingerprint))
        session.close()

    monkeypatch.setattr(worker, "_shadow_engine2", record_operation_ai)


def _current_revisions(session, *, organization_id: int, operation_id: int):
    return session.scalars(
        select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == organization_id,
            UsLaceySourceSetRevision.operation_id == operation_id,
            UsLaceySourceSetRevision.is_current.is_(True),
        )
    ).all()


def test_seven_document_source_set_runs_operation_ai_exactly_once_after_last_member(
    monkeypatch,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
) -> None:
    """Only each sealed generation's final member can run operation-level AI."""
    _require_source_set_schema(engine2_postgres_engine)
    org, operation_id, _, first_assurance_id, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        role="UNKNOWN",
        content=b"source-set-member-1",
    )
    assurance_ids = [first_assurance_id]
    for index in range(2, 8):
        _, assurance_document_id, _, _ = add_test_document(
            engine2_postgres_session_factory,
            organization_id=org,
            operation_id=operation_id,
            role="UNKNOWN",
            filename=f"source-set-member-{index}.pdf",
            content=f"source-set-member-{index}".encode(),
            is_current=True,
        )
        assurance_ids.append(assurance_document_id)

    revision_n = seal_current_source_set(
        organization_id=org,
        operation_id=operation_id,
        session_factory=engine2_postgres_session_factory,
    )
    session = tenant_session(engine2_postgres_session_factory, org)
    persisted_n = session.get(UsLaceySourceSetRevision, revision_n.id)
    members_n = session.scalars(
        select(UsLaceySourceSetMember).where(
            UsLaceySourceSetMember.organization_id == org,
            UsLaceySourceSetMember.source_set_revision_id == revision_n.id,
        )
    ).all()
    documents_n = session.scalars(
        select(AssuranceDocument).where(
            AssuranceDocument.organization_id == org,
            AssuranceDocument.id.in_(assurance_ids),
        )
    ).all()
    assert persisted_n.generation == 1
    assert persisted_n.document_count == 7
    assert persisted_n.status == "SEALED"
    assert persisted_n.is_current is True
    assert len(members_n) == 7
    assert {document.id for document in documents_n} == set(assurance_ids)
    assert all(isinstance(document.public_id, UUID) for document in documents_n)
    operation_public_id = session.get(UsLaceyOperation, operation_id).public_id
    session.close()

    for assurance_document_id in assurance_ids:
        enqueue_us_lacey_document_job(
            organization_id=org,
            operation_id=operation_id,
            assurance_document_id=assurance_document_id,
        )

    ai_calls: list[tuple[int, str]] = []
    _install_non_target_worker_seams(
        monkeypatch,
        engine2_postgres_session_factory,
        ai_calls,
    )
    for index in range(1, 7):
        result = worker.process_one_us_lacey_job(worker_id=f"barrier-{index}")
        assert result.claimed is True
        assert result.job_status == "COMPLETED"
        assert ai_calls == []
        session = tenant_session(engine2_postgres_session_factory, org)
        assert session.get(UsLaceySourceSetRevision, revision_n.id).status == "SEALED"
        session.close()

    final_result = worker.process_one_us_lacey_job(worker_id="barrier-7")
    assert final_result.claimed is True
    assert final_result.job_status == "COMPLETED"
    assert ai_calls == [(persisted_n.generation, persisted_n.source_set_fingerprint)]

    session = tenant_session(engine2_postgres_session_factory, org)
    persisted_n = session.get(UsLaceySourceSetRevision, revision_n.id)
    jobs_n = session.scalars(
        select(UsLaceyProcessingJob).where(
            UsLaceyProcessingJob.organization_id == org,
            UsLaceyProcessingJob.operation_id == operation_id,
        )
    ).all()
    assert persisted_n.status == "FINALIZED"
    assert persisted_n.is_current is True
    assert persisted_n.finalized_at is not None
    assert len(jobs_n) == 7
    assert all(job.status == "COMPLETED" for job in jobs_n)
    session.close()

    identical_retry = seal_current_source_set(
        organization_id=org,
        operation_id=operation_id,
        session_factory=engine2_postgres_session_factory,
    )
    retry_claim = claim_ready_source_set(
        organization_id=org,
        operation_id=operation_id,
        completing_job_id=final_result.job_id,
        session_factory=engine2_postgres_session_factory,
    )
    assert identical_retry.id == revision_n.id
    assert identical_retry.generation == revision_n.generation
    assert identical_retry.source_set_fingerprint == revision_n.source_set_fingerprint
    assert retry_claim.claimed is False
    assert retry_claim.reason == "NOT_SEALED"
    assert ai_calls == [(persisted_n.generation, persisted_n.source_set_fingerprint)]

    replacement_assurance_id = _add_unattached_assurance_document(
        engine2_postgres_session_factory,
        organization_id=org,
        filename="source-set-member-8.pdf",
        content=b"source-set-member-8",
    )
    session = tenant_session(engine2_postgres_session_factory, org)
    assert session.scalar(
        select(func.count(UsLaceyOperationDocument.id)).where(
            UsLaceyOperationDocument.organization_id == org,
            UsLaceyOperationDocument.assurance_document_id == replacement_assurance_id,
        )
    ) == 0
    session.close()
    replacement_link_id = UsLaceyOperationService(
        session_factory=engine2_postgres_session_factory,
    ).attach_document(
        organization_id=org,
        operation_public_id=operation_public_id,
        assurance_document_id=replacement_assurance_id,
        document_role="UNKNOWN",
    )
    revision_n_plus_one = seal_current_source_set(
        organization_id=org,
        operation_id=operation_id,
        session_factory=engine2_postgres_session_factory,
    )

    session = tenant_session(engine2_postgres_session_factory, org)
    persisted_n = session.get(UsLaceySourceSetRevision, revision_n.id)
    persisted_n_plus_one = session.get(
        UsLaceySourceSetRevision,
        revision_n_plus_one.id,
    )
    replacement_link = session.get(UsLaceyOperationDocument, replacement_link_id)
    members_n_plus_one = session.scalars(
        select(UsLaceySourceSetMember).where(
            UsLaceySourceSetMember.organization_id == org,
            UsLaceySourceSetMember.source_set_revision_id == revision_n_plus_one.id,
        )
    ).all()
    assert replacement_link.assurance_document_id == replacement_assurance_id
    assert replacement_link.is_current is True
    assert revision_n_plus_one.id != revision_n.id
    assert persisted_n_plus_one.generation == persisted_n.generation + 1
    assert persisted_n_plus_one.source_set_fingerprint != persisted_n.source_set_fingerprint
    assert persisted_n_plus_one.document_count == 8
    assert persisted_n_plus_one.status == "SEALED"
    assert persisted_n_plus_one.is_current is True
    assert persisted_n.is_current is False
    assert len(members_n_plus_one) == 8
    session.close()

    enqueue_us_lacey_document_job(
        organization_id=org,
        operation_id=operation_id,
        assurance_document_id=replacement_assurance_id,
    )
    next_result = worker.process_one_us_lacey_job(worker_id="barrier-8")
    assert next_result.claimed is True
    assert next_result.job_status == "COMPLETED"
    assert ai_calls == [
        (persisted_n.generation, persisted_n.source_set_fingerprint),
        (
            persisted_n_plus_one.generation,
            persisted_n_plus_one.source_set_fingerprint,
        ),
    ]

    session = tenant_session(engine2_postgres_session_factory, org)
    persisted_n = session.get(UsLaceySourceSetRevision, revision_n.id)
    persisted_n_plus_one = session.get(
        UsLaceySourceSetRevision,
        revision_n_plus_one.id,
    )
    assert persisted_n.is_current is False
    assert persisted_n_plus_one.status == "FINALIZED"
    assert persisted_n_plus_one.is_current is True
    assert persisted_n_plus_one.finalized_at is not None
    assert seal_current_source_set(
        organization_id=org,
        operation_id=operation_id,
        session_factory=engine2_postgres_session_factory,
    ).id == revision_n_plus_one.id
    current_revisions = _current_revisions(
        session,
        organization_id=org,
        operation_id=operation_id,
    )
    assert [revision.id for revision in current_revisions] == [persisted_n_plus_one.id]
    session.close()


def test_recurring_source_set_creates_a_new_generation_instead_of_reviving_history(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
) -> None:
    """A -> B -> A is a new revision even when A's canonical fingerprint repeats."""
    _require_source_set_schema(engine2_postgres_engine)
    org, operation_id, original_link_id, _, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        role="BILL_OF_LADING",
        content=b"recurring-source-set-a",
    )
    revision_a1 = seal_current_source_set(
        organization_id=org,
        operation_id=operation_id,
        session_factory=engine2_postgres_session_factory,
    )
    identical_a1 = seal_current_source_set(
        organization_id=org,
        operation_id=operation_id,
        session_factory=engine2_postgres_session_factory,
    )
    assert identical_a1.id == revision_a1.id

    replacement_link_id, _, _, _ = add_test_document(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation_id,
        role="BILL_OF_LADING",
        filename="recurring-source-set-b.pdf",
        content=b"recurring-source-set-b",
        version_number=2,
        is_current=False,
    )
    session = tenant_session(engine2_postgres_session_factory, org)
    session.get(UsLaceyOperationDocument, original_link_id).is_current = False
    session.get(UsLaceyOperationDocument, replacement_link_id).is_current = True
    session.commit()
    session.close()

    revision_b = seal_current_source_set(
        organization_id=org,
        operation_id=operation_id,
        session_factory=engine2_postgres_session_factory,
    )
    assert revision_b.generation == revision_a1.generation + 1
    assert revision_b.source_set_fingerprint != revision_a1.source_set_fingerprint

    session = tenant_session(engine2_postgres_session_factory, org)
    session.get(UsLaceyOperationDocument, replacement_link_id).is_current = False
    session.get(UsLaceyOperationDocument, original_link_id).is_current = True
    session.commit()
    session.close()

    revision_a2 = seal_current_source_set(
        organization_id=org,
        operation_id=operation_id,
        session_factory=engine2_postgres_session_factory,
    )

    assert revision_a2.id != revision_a1.id
    assert revision_a2.generation == revision_b.generation + 1
    assert revision_a2.source_set_fingerprint == revision_a1.source_set_fingerprint
    assert revision_a2.is_current is True

    session = tenant_session(engine2_postgres_session_factory, org)
    revisions = session.scalars(
        select(UsLaceySourceSetRevision)
        .where(
            UsLaceySourceSetRevision.organization_id == org,
            UsLaceySourceSetRevision.operation_id == operation_id,
        )
        .order_by(UsLaceySourceSetRevision.generation)
    ).all()
    assert [item.generation for item in revisions] == [1, 2, 3]
    assert [item.id for item in revisions if item.is_current] == [revision_a2.id]
    assert revisions[0].source_set_fingerprint == revisions[2].source_set_fingerprint
    session.close()
