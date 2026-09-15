"""PostgreSQL acceptance for source-set CAS, recovery and tenant isolation."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import os

from sqlalchemy import create_engine, select, text, update
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.models import (
    UsLaceyProcessingJob,
    UsLaceySourceSetMember,
    UsLaceySourceSetRevision,
)
from litoral_trace.us_lacey.jobs import claim_next_us_lacey_job, recover_stale_us_lacey_jobs
from litoral_trace.us_lacey.source_sets import SourceSetClaim, claim_ready_source_set, finalize_claim, seal_current_source_set
from tests.us_lacey_engine2_postgres import add_test_document, create_test_graph, engine2_postgres_engine, engine2_postgres_session_factory, tenant_session


def _sealed(factory):
    """Create a real sealed revision so its fingerprint matches CURRENT documents."""
    org, operation, _, assurance, _, _ = create_test_graph(factory)
    revision = seal_current_source_set(
        organization_id=org,
        operation_id=operation,
        session_factory=factory,
    )
    session = tenant_session(factory, org)
    job = UsLaceyProcessingJob(
        organization_id=org,
        operation_id=operation,
        assurance_document_id=assurance,
        status="RUNNING",
        max_attempts=3,
        available_at=datetime.now(timezone.utc),
    )
    session.add(job)
    session.commit()
    result = org, operation, revision.id, job.id
    session.close()
    return result


def _add_running_job(factory, *, org, operation, assurance):
    session = tenant_session(factory, org)
    job = UsLaceyProcessingJob(
        organization_id=org,
        operation_id=operation,
        assurance_document_id=assurance,
        status="RUNNING",
        max_attempts=3,
        available_at=datetime.now(timezone.utc),
    )
    session.add(job)
    session.commit()
    job_id = int(job.id)
    session.close()
    return job_id


def _mark_newer_running_attempt(factory, *, org, job_id, worker_id="worker-b"):
    """Start a later attempt using the database clock, matching queue-claim ordering."""
    session = tenant_session(factory, org)
    locked_at = session.execute(
        text(
            """
            UPDATE public.us_lacey_processing_jobs
            SET attempt_count = attempt_count + 1,
                status = 'RUNNING',
                locked_by = :worker_id,
                locked_at = clock_timestamp(),
                heartbeat_at = clock_timestamp(),
                updated_at = clock_timestamp()
            WHERE id = :job_id
            RETURNING locked_at
            """
        ),
        {"job_id": job_id, "worker_id": worker_id},
    ).scalar_one()
    session.commit()
    session.close()
    return locked_at


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


def test_finalize_claim_fails_closed_when_current_documents_changed_after_claim(engine2_postgres_session_factory):
    """A post-claim membership change cannot publish the older revision FINALIZED."""
    org, operation, revision_id, job_id = _sealed(engine2_postgres_session_factory)
    claim = claim_ready_source_set(
        organization_id=org,
        operation_id=operation,
        completing_job_id=job_id,
        session_factory=engine2_postgres_session_factory,
    )
    assert claim.claimed

    add_test_document(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation,
        role="UNKNOWN",
        filename="post-claim-source-set-change.pdf",
        content=b"post-claim-source-set-change",
        is_current=True,
    )

    assert not finalize_claim(
        organization_id=org,
        claim=claim,
        session_factory=engine2_postgres_session_factory,
    )
    session = tenant_session(engine2_postgres_session_factory, org)
    revision = session.get(UsLaceySourceSetRevision, revision_id)
    assert revision.status == "FINALIZING"
    assert revision.is_current is True
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
    prior_claimed_at = revision.claimed_at
    session.close()
    retry_locked_at = _mark_newer_running_attempt(
        engine2_postgres_session_factory,
        org=org,
        job_id=job_id,
    )
    assert retry_locked_at > prior_claimed_at

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


def test_concurrent_retry_reclaim_has_one_cas_winner(engine2_postgres_session_factory):
    org, operation, revision_id, job_id = _sealed(engine2_postgres_session_factory)
    first = claim_ready_source_set(
        organization_id=org,
        operation_id=operation,
        completing_job_id=job_id,
        session_factory=engine2_postgres_session_factory,
    )
    assert first.claimed and first.claimed_at is not None

    retry_locked_at = _mark_newer_running_attempt(
        engine2_postgres_session_factory,
        org=org,
        job_id=job_id,
    )
    assert retry_locked_at > first.claimed_at

    def reclaim(_):
        return claim_ready_source_set(
            organization_id=org,
            operation_id=operation,
            completing_job_id=job_id,
            session_factory=engine2_postgres_session_factory,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        retries = list(pool.map(reclaim, range(2)))

    winners = [claim for claim in retries if claim.claimed]
    losers = [claim for claim in retries if not claim.claimed]
    assert len(winners) == 1
    assert winners[0].reason == "RECLAIMED"
    assert len(losers) == 1
    assert losers[0].reason == "ALREADY_CLAIMED"
    assert not finalize_claim(
        organization_id=org,
        claim=first,
        session_factory=engine2_postgres_session_factory,
    )
    assert finalize_claim(
        organization_id=org,
        claim=winners[0],
        session_factory=engine2_postgres_session_factory,
    )

    session = tenant_session(engine2_postgres_session_factory, org)
    revision = session.get(UsLaceySourceSetRevision, revision_id)
    assert revision.status == "FINALIZED"
    session.close()


def test_real_queue_stale_recovery_reclaims_finalizing_source_set(engine2_postgres_session_factory):
    org, operation, revision_id, job_id = _sealed(engine2_postgres_session_factory)
    stale_at = datetime.now(timezone.utc) - timedelta(minutes=2)
    session = tenant_session(engine2_postgres_session_factory, org)
    session.execute(
        text(
            """
            UPDATE public.us_lacey_processing_jobs
            SET attempt_count = 1,
                status = 'RUNNING',
                locked_by = 'worker-a',
                locked_at = :stale_at,
                heartbeat_at = :stale_at,
                started_at = :stale_at,
                updated_at = :stale_at
            WHERE id = :job_id
            """
        ),
        {"job_id": job_id, "stale_at": stale_at},
    )
    session.commit()
    session.close()

    first = claim_ready_source_set(
        organization_id=org,
        operation_id=operation,
        completing_job_id=job_id,
        session_factory=engine2_postgres_session_factory,
    )
    assert first.claimed and first.claimed_at is not None

    retried, failed = recover_stale_us_lacey_jobs(
        stale_after_seconds=60,
        retry_delay_seconds=0,
    )
    assert retried == 1
    assert failed == 0

    recovered_job = claim_next_us_lacey_job(worker_id="worker-b")
    assert recovered_job is not None
    assert recovered_job.id == job_id
    assert recovered_job.organization_id == org
    assert recovered_job.operation_id == operation
    assert recovered_job.status == "RUNNING"
    assert recovered_job.attempt_count == 2

    retry = claim_ready_source_set(
        organization_id=org,
        operation_id=operation,
        completing_job_id=job_id,
        session_factory=engine2_postgres_session_factory,
    )
    assert retry.claimed
    assert retry.reason == "RECLAIMED"
    assert retry.claimed_at is not None
    assert retry.claimed_at > first.claimed_at
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
    revision = session.get(UsLaceySourceSetRevision, revision_id)
    assert revision.status == "FINALIZED"
    session.close()


def test_source_set_revisions_members_and_claim_are_runtime_tenant_isolated(engine2_postgres_session_factory):
    org_a, operation_a, _, assurance_a, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"source-set-tenant-a",
    )
    org_b, operation_b, _, assurance_b, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"source-set-tenant-b",
    )
    revision_a = seal_current_source_set(
        organization_id=org_a,
        operation_id=operation_a,
        session_factory=engine2_postgres_session_factory,
    )
    revision_b = seal_current_source_set(
        organization_id=org_b,
        operation_id=operation_b,
        session_factory=engine2_postgres_session_factory,
    )
    job_a = _add_running_job(
        engine2_postgres_session_factory,
        org=org_a,
        operation=operation_a,
        assurance=assurance_a,
    )
    _add_running_job(
        engine2_postgres_session_factory,
        org=org_b,
        operation=operation_b,
        assurance=assurance_b,
    )

    runtime_url = os.environ.get("TEST_POSTGRES_DATABASE_URL")
    assert runtime_url, "TEST_POSTGRES_DATABASE_URL is required for source-set RLS acceptance"
    runtime_engine = create_engine(normalize_database_url(runtime_url), pool_pre_ping=True)
    RuntimeFactory = sessionmaker(bind=runtime_engine, expire_on_commit=False)
    try:
        session = tenant_session(RuntimeFactory, org_a)
        visible_revisions = session.scalars(
            select(UsLaceySourceSetRevision).order_by(UsLaceySourceSetRevision.id)
        ).all()
        visible_members = session.scalars(
            select(UsLaceySourceSetMember).order_by(UsLaceySourceSetMember.id)
        ).all()
        assert [item.id for item in visible_revisions] == [revision_a.id]
        assert visible_revisions[0].organization_id == org_a
        assert {item.organization_id for item in visible_members} == {org_a}
        assert {item.source_set_revision_id for item in visible_members} == {revision_a.id}
        assert session.get(UsLaceySourceSetRevision, revision_b.id) is None
        cross_tenant_update = session.execute(
            update(UsLaceySourceSetRevision)
            .where(UsLaceySourceSetRevision.id == revision_b.id)
            .values(status="SUPERSEDED")
        )
        assert cross_tenant_update.rowcount == 0
        session.commit()
        session.close()

        claim_a = claim_ready_source_set(
            organization_id=org_a,
            operation_id=operation_a,
            completing_job_id=job_a,
            session_factory=RuntimeFactory,
        )
        assert claim_a.claimed
        assert claim_a.reason == "CLAIMED"
        assert finalize_claim(
            organization_id=org_a,
            claim=claim_a,
            session_factory=RuntimeFactory,
        )

        session = tenant_session(RuntimeFactory, org_b)
        visible_b = session.scalars(
            select(UsLaceySourceSetRevision).order_by(UsLaceySourceSetRevision.id)
        ).all()
        assert [item.id for item in visible_b] == [revision_b.id]
        assert visible_b[0].status == "SEALED"
        assert session.get(UsLaceySourceSetRevision, revision_a.id) is None
        session.close()

        audit = tenant_session(engine2_postgres_session_factory, org_b)
        untouched_b = audit.get(UsLaceySourceSetRevision, revision_b.id)
        assert untouched_b.status == "SEALED"
        assert untouched_b.is_current is True
        audit.close()
    finally:
        runtime_engine.dispose()
