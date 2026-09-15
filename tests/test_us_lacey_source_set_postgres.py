"""PostgreSQL acceptance for source-set CAS and stale-publication guards."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, text

from litoral_trace.db.models import (
    UsLaceyProcessingJob,
    UsLaceySourceSetMember,
    UsLaceySourceSetRevision,
)
from litoral_trace.us_lacey.source_sets import SourceSetClaim, claim_ready_source_set, finalize_claim
from tests.us_lacey_engine2_postgres import create_test_graph, engine2_postgres_engine, engine2_postgres_session_factory, tenant_session


def _sealed(factory):
    org, operation, link, assurance, _, _ = create_test_graph(factory)
    session = tenant_session(factory, org)
    revision = UsLaceySourceSetRevision(
        organization_id=org, operation_id=operation, generation=1,
        source_set_fingerprint="a" * 64, document_count=1, status="SEALED", is_current=True,
    )
    session.add(revision); session.flush()
    session.add(UsLaceySourceSetMember(organization_id=org, source_set_revision_id=revision.id, operation_document_id=link, assurance_document_id=assurance))
    job = UsLaceyProcessingJob(organization_id=org, operation_id=operation, assurance_document_id=assurance, status="RUNNING", max_attempts=3, available_at=datetime.now(timezone.utc))
    session.add(job); session.commit()
    result = org, operation, revision.id, job.id
    session.close()
    return result


def test_two_finalizers_have_one_persistent_cas_winner(engine2_postgres_session_factory):
    org, operation, revision_id, job_id = _sealed(engine2_postgres_session_factory)
    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(lambda _: claim_ready_source_set(organization_id=org, operation_id=operation, completing_job_id=job_id, session_factory=engine2_postgres_session_factory), range(2)))
    assert sum(claim.claimed for claim in claims) == 1
    session = tenant_session(engine2_postgres_session_factory, org)
    assert session.get(UsLaceySourceSetRevision, revision_id).status == "FINALIZING"
    session.close()


def test_stale_claim_cannot_publish_after_new_generation_becomes_current(engine2_postgres_session_factory):
    org, operation, revision_id, job_id = _sealed(engine2_postgres_session_factory)
    claim = claim_ready_source_set(organization_id=org, operation_id=operation, completing_job_id=job_id, session_factory=engine2_postgres_session_factory)
    assert claim.claimed
    session = tenant_session(engine2_postgres_session_factory, org)
    old = session.get(UsLaceySourceSetRevision, revision_id); old.is_current = False
    session.add(UsLaceySourceSetRevision(organization_id=org, operation_id=operation, generation=2, source_set_fingerprint="b" * 64, document_count=1, status="SEALED", is_current=True))
    session.commit(); session.close()
    assert not finalize_claim(organization_id=org, claim=claim, session_factory=engine2_postgres_session_factory)
    session = tenant_session(engine2_postgres_session_factory, org)
    stale = session.get(UsLaceySourceSetRevision, revision_id)
    assert stale.status == "FINALIZING"
    assert stale.is_current is False
    current = session.scalars(select(UsLaceySourceSetRevision).where(
        UsLaceySourceSetRevision.organization_id == org,
        UsLaceySourceSetRevision.operation_id == operation,
        UsLaceySourceSetRevision.is_current.is_(True),
    )).all()
    assert len(current) == 1
    assert current[0].generation == 2
    assert current[0].source_set_fingerprint == "b" * 64
    session.close()


def test_finalized_generation_is_not_claimed_again(engine2_postgres_session_factory):
    org, operation, _, job_id = _sealed(engine2_postgres_session_factory)
    claim = claim_ready_source_set(organization_id=org, operation_id=operation, completing_job_id=job_id, session_factory=engine2_postgres_session_factory)
    assert finalize_claim(organization_id=org, claim=claim, session_factory=engine2_postgres_session_factory)
    retry = claim_ready_source_set(organization_id=org, operation_id=operation, completing_job_id=job_id, session_factory=engine2_postgres_session_factory)
    assert not retry.claimed and retry.reason == "NOT_SEALED"


def test_retry_attempt_reclaims_abandoned_finalizing_revision_and_fences_stale_claim(engine2_postgres_session_factory):
    org, operation, revision_id, job_id = _sealed(engine2_postgres_session_factory)
    first = claim_ready_source_set(
        organization_id=org,
        operation_id=operation,
        completing_job_id=job_id,
        session_factory=engine2_postgres_session_factory,
    )
    assert first.claimed

    session = tenant_session(engine2_postgres_session_factory, org)
    revision = session.get(UsLaceySourceSetRevision, revision_id)
    assert revision.status == "FINALIZING"
    assert revision.claimed_at is not None
    retry_locked_at = revision.claimed_at + timedelta(seconds=1)
    session.execute(
        text(
            """
            UPDATE public.us_lacey_processing_jobs
            SET attempt_count = attempt_count + 1,
                status = 'RUNNING',
                locked_by = 'worker-b',
                locked_at = :locked_at,
                heartbeat_at = :locked_at,
                updated_at = :locked_at
            WHERE id = :job_id
            """
        ),
        {"job_id": job_id, "locked_at": retry_locked_at},
    )
    session.commit()
    session.close()

    retry = claim_ready_source_set(
        organization_id=org,
        operation_id=operation,
        completing_job_id=job_id,
        session_factory=engine2_postgres_session_factory,
    )
    assert retry.claimed
    assert retry.reason == "RECLAIMED"

    assert not finalize_claim(
        organization_id=org,
        claim=first,
        session_factory=engine2_postgres_session_factory,
    )
    assert finalize_claim(
        organization_id=org,
        claim=retry,
        session_factory=engine2_postgres_session_factory,
    )

    session = tenant_session(engine2_postgres_session_factory, org)
    finalized = session.get(UsLaceySourceSetRevision, revision_id)
    assert finalized.status == "FINALIZED"
    assert finalized.is_current is True
    session.close()
