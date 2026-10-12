"""Real PostgreSQL acceptance: V2 lab same-document reprocessing is fenced and safe."""
from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone

import pytest
from sqlalchemy import func, select

from litoral_trace.db.models import (
    AssuranceDocument, UsLaceyOperation, UsLaceyOperationDocument,
    UsLaceyOperationEvent, UsLaceyProcessingJob, UsLaceySourceSetRevision,
    UsLaceySourceSetMember, VaultDocument,
)
from litoral_trace.us_lacey.jobs import UsLaceyJobError
from litoral_trace.us_lacey.reprocessing import reprocess_existing_v2lab_documents
from litoral_trace.us_lacey.source_sets import seal_current_source_set
from tests.us_lacey_engine2_postgres import (
    add_test_document, create_test_graph, engine2_postgres_session_factory, tenant_session,
)


def _prepare_completed_v2lab(factory):
    org, op_id, link_id, assurance_id, vault_id, digest = create_test_graph(factory)
    db = tenant_session(factory, org)
    operation = db.get(UsLaceyOperation, op_id)
    operation.client_reference = "V2LAB-PACK2"
    operation.status = "REVIEW_REQUIRED"
    db.add(UsLaceyProcessingJob(
        organization_id=org, operation_id=op_id,
        assurance_document_id=assurance_id,
        status="COMPLETED", attempt_count=1, max_attempts=3,
        available_at=datetime.now(timezone.utc),
    ))
    db.commit()
    public_id = operation.public_id
    db.close()
    revision = seal_current_source_set(
        organization_id=org, operation_id=op_id, session_factory=factory,
    )
    db = tenant_session(factory, org)
    current = db.get(UsLaceySourceSetRevision, revision.id)
    current.status = "FINALIZED"
    db.commit()
    db.close()
    return org, op_id, public_id, assurance_id, link_id, vault_id, digest, revision


def _replay(factory, org, public_id):
    return reprocess_existing_v2lab_documents(
        organization_id=org, operation_public_id=public_id, user_id=1,
        session_factory=factory, lock_factory=lambda **_kw: nullcontext(),
    )


def test_same_source_replay_requeues_once_without_new_documents_or_authority(
    engine2_postgres_session_factory,
):
    factory = engine2_postgres_session_factory
    org, op_id, public_id, doc_id, link_id, vault_id, digest, old = (
        _prepare_completed_v2lab(factory)
    )
    assert _replay(factory, org, public_id) == 1

    db = tenant_session(factory, org)
    operation = db.get(UsLaceyOperation, op_id)
    source = db.scalar(select(UsLaceySourceSetRevision).where(
        UsLaceySourceSetRevision.organization_id == org,
        UsLaceySourceSetRevision.operation_id == op_id,
        UsLaceySourceSetRevision.is_current.is_(True),
    ))
    prior = db.get(UsLaceySourceSetRevision, old.id)
    jobs = db.scalars(select(UsLaceyProcessingJob).where(
        UsLaceyProcessingJob.organization_id == org,
        UsLaceyProcessingJob.operation_id == op_id,
    )).all()
    events = db.scalars(select(UsLaceyOperationEvent).where(
        UsLaceyOperationEvent.organization_id == org,
        UsLaceyOperationEvent.operation_id == op_id,
    )).all()
    assert operation.status == "PROCESSING"
    assert operation.review_result is None
    assert prior.status == "SUPERSEDED" and not prior.is_current
    assert source.status == "SEALED"
    assert source.generation == old.generation + 1
    assert source.source_set_fingerprint == old.source_set_fingerprint
    members = db.scalars(select(UsLaceySourceSetMember).where(
        UsLaceySourceSetMember.organization_id == org,
        UsLaceySourceSetMember.source_set_revision_id == source.id,
    )).all()
    assert [m.assurance_document_id for m in members] == [doc_id]
    assert len(jobs) == 1 and jobs[0].status == "QUEUED"
    assert jobs[0].attempt_count == 0
    assert db.scalar(select(func.count(UsLaceyOperationDocument.id)).where(
        UsLaceyOperationDocument.organization_id == org,
        UsLaceyOperationDocument.operation_id == op_id,
    )) == 1
    assert db.get(AssuranceDocument, doc_id).processing_status == "EXTRACTED"
    assert db.get(VaultDocument, vault_id).sha256 == digest
    assert any(e.details.get("action") == "reprocess_requested" for e in events)
    db.close()

    # Concurrent/double-click replay must not create a third generation.
    with pytest.raises(UsLaceyJobError, match="finish"):
        _replay(factory, org, public_id)


def test_replay_rejects_wrong_tenant_and_unavailable_original_atomically(
    engine2_postgres_session_factory,
):
    factory = engine2_postgres_session_factory
    org, op_id, public_id, _doc, _link, vault_id, _digest, old = (
        _prepare_completed_v2lab(factory)
    )
    with pytest.raises(UsLaceyJobError, match="not found"):
        _replay(factory, org + 1000000, public_id)
    db = tenant_session(factory, org)
    db.get(VaultDocument, vault_id).status = "delete_pending"
    db.commit()
    db.close()
    with pytest.raises(UsLaceyJobError, match="unavailable"):
        _replay(factory, org, public_id)
    db = tenant_session(factory, org)
    rev = db.get(UsLaceySourceSetRevision, old.id)
    op = db.get(UsLaceyOperation, op_id)
    assert rev.is_current and rev.status == "FINALIZED"
    assert op.status == "REVIEW_REQUIRED"
    db.close()


def test_replay_fails_closed_for_non_lab_reference(engine2_postgres_session_factory):
    factory = engine2_postgres_session_factory
    org, _op, public_id, *_ = _prepare_completed_v2lab(factory)
    db = tenant_session(factory, org)
    op = db.scalar(select(UsLaceyOperation).where(
        UsLaceyOperation.organization_id == org,
        UsLaceyOperation.public_id == public_id,
    ))
    op.client_reference = "CUSTOMER-REAL-SHIPMENT"
    db.commit()
    db.close()
    with pytest.raises(UsLaceyJobError, match="restricted"):
        _replay(factory, org, public_id)


def test_pack2_six_existing_documents_requeue_as_one_atomic_batch(
    engine2_postgres_session_factory,
):
    factory = engine2_postgres_session_factory
    org, op_id, public_id, *_ = _prepare_completed_v2lab(factory)
    for idx in range(5):
        _link, doc_id, _vault, _sha = add_test_document(
            factory, organization_id=org, operation_id=op_id,
            role="UNKNOWN", filename=f"pack2-{idx + 2}.pdf",
            content=f"separate-original-{idx + 2}".encode(),
        )
        db = tenant_session(factory, org)
        db.add(UsLaceyProcessingJob(
            organization_id=org, operation_id=op_id,
            assurance_document_id=doc_id, status="COMPLETED",
            attempt_count=1, max_attempts=3,
            available_at=datetime.now(timezone.utc),
        ))
        db.commit()
        db.close()
    latest = seal_current_source_set(
        organization_id=org, operation_id=op_id, session_factory=factory,
    )
    db = tenant_session(factory, org)
    db.get(UsLaceySourceSetRevision, latest.id).status = "FINALIZED"
    db.commit()
    db.close()

    assert _replay(factory, org, public_id) == 6
    db = tenant_session(factory, org)
    queued = db.scalars(select(UsLaceyProcessingJob).where(
        UsLaceyProcessingJob.organization_id == org,
        UsLaceyProcessingJob.operation_id == op_id,
        UsLaceyProcessingJob.status == "QUEUED",
    )).all()
    current = db.scalar(select(UsLaceySourceSetRevision).where(
        UsLaceySourceSetRevision.organization_id == org,
        UsLaceySourceSetRevision.operation_id == op_id,
        UsLaceySourceSetRevision.is_current.is_(True),
    ))
    assert len(queued) == 6
    assert current.generation == latest.generation + 1
    assert current.document_count == 6
    assert db.scalar(select(func.count(UsLaceyOperationDocument.id)).where(
        UsLaceyOperationDocument.organization_id == org,
        UsLaceyOperationDocument.operation_id == op_id,
        UsLaceyOperationDocument.is_current.is_(True),
    )) == 6
    db.close()
