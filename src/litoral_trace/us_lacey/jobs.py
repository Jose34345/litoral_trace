"""Durable PostgreSQL queue primitives for U.S. Lacey document processing."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import text

from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.worker_db import get_us_lacey_worker_db_session


class UsLaceyJobError(RuntimeError):
    pass


STAGE_TIMEOUT_SECONDS = 300


@dataclass(frozen=True)
class UsLaceyJob:
    id: int
    public_id: UUID
    organization_id: int
    operation_id: int
    assurance_document_id: int
    status: str
    attempt_count: int
    max_attempts: int
    available_at: datetime
    locked_by: str | None


def _job_from_row(row) -> UsLaceyJob:
    return UsLaceyJob(
        id=int(row["id"]),
        public_id=UUID(str(row["public_id"])),
        organization_id=int(row["organization_id"]),
        operation_id=int(row["operation_id"]),
        assurance_document_id=int(row["assurance_document_id"]),
        status=str(row["status"]),
        attempt_count=int(row["attempt_count"]),
        max_attempts=int(row["max_attempts"]),
        available_at=row["available_at"],
        locked_by=str(row["locked_by"]) if row["locked_by"] else None,
    )


def enqueue_us_lacey_document_job(
    *,
    organization_id: int,
    operation_id: int,
    assurance_document_id: int,
    max_attempts: int = 3,
) -> UsLaceyJob:
    if organization_id <= 0 or operation_id <= 0 or assurance_document_id <= 0:
        raise UsLaceyJobError("Valid tenant, operation and document identifiers are required.")
    if max_attempts <= 0 or max_attempts > 10:
        raise UsLaceyJobError("max_attempts must be between 1 and 10.")

    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, organization_id)
        row = session.execute(
            text(
                """
                INSERT INTO public.us_lacey_processing_jobs (
                    public_id, organization_id, operation_id, assurance_document_id,
                    status, attempt_count, max_attempts, available_at, created_at, updated_at
                ) VALUES (
                    gen_random_uuid(), :organization_id, :operation_id, :document_id,
                    'QUEUED', 0, :max_attempts, now(), now(), now()
                )
                ON CONFLICT (organization_id, operation_id, assurance_document_id)
                DO UPDATE SET updated_at = public.us_lacey_processing_jobs.updated_at
                RETURNING id, public_id, organization_id, operation_id,
                          assurance_document_id, status, attempt_count, max_attempts,
                          available_at, locked_by
                """
            ),
            {
                "organization_id": organization_id,
                "operation_id": operation_id,
                "document_id": assurance_document_id,
                "max_attempts": max_attempts,
            },
        ).mappings().one()
        session.commit()
        return _job_from_row(row)
    except Exception as exc:
        session.rollback()
        raise UsLaceyJobError("Unable to enqueue document processing.") from exc
    finally:
        session.close()


def retry_failed_us_lacey_operation(
    *,
    organization_id: int,
    operation_public_id: UUID | str,
) -> int:
    """Requeue failed jobs for one failed operation in a single tenant transaction.

    Existing durable job rows are reset rather than duplicated because the queue
    enforces one job per operation/document pair. Documents that failed before
    reaching a reviewable terminal state are returned to UPLOADED; documents
    already in NEEDS_REVIEW remain authoritative and are not destructively reset.

    Returns the number of jobs requeued.
    """
    org_id = int(organization_id)
    if org_id <= 0:
        raise UsLaceyJobError("A valid organization is required.")

    try:
        operation_uuid = UUID(str(operation_public_id))
    except (TypeError, ValueError) as exc:
        raise UsLaceyJobError("A valid operation identifier is required.") from exc

    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, org_id)

        operation = session.execute(
            text(
                """
                SELECT id, status
                FROM public.us_lacey_operations
                WHERE organization_id = :organization_id
                  AND public_id = :operation_public_id
                FOR UPDATE
                """
            ),
            {
                "organization_id": org_id,
                "operation_public_id": operation_uuid,
            },
        ).mappings().one_or_none()

        if operation is None:
            raise UsLaceyJobError("Operation not found.")
        if str(operation["status"]) != "FAILED":
            raise UsLaceyJobError("Only failed operations can be retried.")

        operation_id = int(operation["id"])

        active_count = int(
            session.execute(
                text(
                    """
                    SELECT count(*)
                    FROM public.us_lacey_processing_jobs
                    WHERE organization_id = :organization_id
                      AND operation_id = :operation_id
                      AND status IN ('QUEUED','RUNNING','RETRY')
                    """
                ),
                {
                    "organization_id": org_id,
                    "operation_id": operation_id,
                },
            ).scalar_one()
            or 0
        )
        if active_count:
            raise UsLaceyJobError("This operation already has active processing work.")

        failed_jobs = session.execute(
            text(
                """
                SELECT id, assurance_document_id
                FROM public.us_lacey_processing_jobs
                WHERE organization_id = :organization_id
                  AND operation_id = :operation_id
                  AND status = 'FAILED'
                ORDER BY id ASC
                FOR UPDATE
                """
            ),
            {
                "organization_id": org_id,
                "operation_id": operation_id,
            },
        ).mappings().all()

        if not failed_jobs:
            raise UsLaceyJobError("This failed operation has no failed processing job to retry.")

        # Reset only documents that genuinely failed parsing/processing. A
        # NEEDS_REVIEW document can be the correct durable result even when the
        # worker died later while finalizing the queue/operation state.
        session.execute(
            text(
                """
                UPDATE public.assurance_documents AS document
                SET processing_status = 'UPLOADED',
                    last_error_code = NULL,
                    last_error_message = NULL,
                    updated_at = now()
                WHERE document.organization_id = :organization_id
                  AND document.processing_status = 'FAILED'
                  AND EXISTS (
                      SELECT 1
                      FROM public.us_lacey_processing_jobs AS job
                      WHERE job.organization_id = :organization_id
                        AND job.operation_id = :operation_id
                        AND job.status = 'FAILED'
                        AND job.assurance_document_id = document.id
                  )
                """
            ),
            {
                "organization_id": org_id,
                "operation_id": operation_id,
            },
        )

        updated_jobs = session.execute(
            text(
                """
                UPDATE public.us_lacey_processing_jobs
                SET status = 'QUEUED',
                    attempt_count = 0,
                    available_at = now(),
                    locked_by = NULL,
                    locked_at = NULL,
                    heartbeat_at = NULL,
                    current_stage = NULL,
                    stage_started_at = NULL,
                    started_at = NULL,
                    completed_at = NULL,
                    last_error_code = NULL,
                    last_error_message = NULL,
                    updated_at = now()
                WHERE organization_id = :organization_id
                  AND operation_id = :operation_id
                  AND status = 'FAILED'
                """
            ),
            {
                "organization_id": org_id,
                "operation_id": operation_id,
            },
        ).rowcount

        if int(updated_jobs or 0) != len(failed_jobs):
            raise UsLaceyJobError("Failed jobs changed while the retry was being prepared.")

        updated_operation = session.execute(
            text(
                """
                UPDATE public.us_lacey_operations
                SET status = 'PROCESSING',
                    review_result = NULL,
                    updated_at = now()
                WHERE organization_id = :organization_id
                  AND id = :operation_id
                  AND status = 'FAILED'
                """
            ),
            {
                "organization_id": org_id,
                "operation_id": operation_id,
            },
        ).rowcount

        if int(updated_operation or 0) != 1:
            raise UsLaceyJobError("Operation state changed while the retry was being prepared.")

        session.commit()
        return len(failed_jobs)
    except UsLaceyJobError:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise UsLaceyJobError("Unable to retry failed operation processing.") from exc
    finally:
        session.close()


def has_claimable_us_lacey_job(
    *,
    per_organization_limit: int = 2,
) -> bool:
    """Return whether a job is currently claimable without mutating queue state.

    This lightweight probe lets the supervisor avoid spawning a fresh Python
    interpreter while the queue is idle. It deliberately mirrors the admission
    predicate used by claim_next_us_lacey_job; a child must still claim with
    FOR UPDATE SKIP LOCKED because this read is only a hint and can race.
    """
    if per_organization_limit <= 0 or per_organization_limit > 20:
        raise UsLaceyJobError("per_organization_limit must be between 1 and 20.")

    session = get_us_lacey_worker_db_session()
    try:
        candidate = session.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM public.us_lacey_processing_jobs AS queued
                    WHERE queued.status IN ('QUEUED','RETRY')
                      AND queued.available_at <= now()
                      AND (
                          SELECT count(*)
                          FROM public.us_lacey_processing_jobs AS active
                          WHERE active.organization_id = queued.organization_id
                            AND active.status = 'RUNNING'
                      ) < :per_organization_limit
                )
                """
            ),
            {"per_organization_limit": per_organization_limit},
        ).scalar_one()
        session.rollback()
        return bool(candidate)
    except Exception as exc:
        session.rollback()
        raise UsLaceyJobError("Unable to inspect the processing queue.") from exc
    finally:
        session.close()

def claim_next_us_lacey_job(
    *,
    worker_id: str,
    per_organization_limit: int = 2,
) -> UsLaceyJob | None:
    worker_id = str(worker_id or "").strip()
    if not worker_id or len(worker_id) > 255:
        raise UsLaceyJobError("worker_id is required.")
    if per_organization_limit <= 0 or per_organization_limit > 20:
        raise UsLaceyJobError("per_organization_limit must be between 1 and 20.")

    session = get_us_lacey_worker_db_session()
    try:
        row = session.execute(
            text(
                """
                WITH candidate AS (
                    SELECT queued.id
                    FROM public.us_lacey_processing_jobs AS queued
                    WHERE queued.status IN ('QUEUED','RETRY')
                      AND queued.available_at <= now()
                      AND (
                          SELECT count(*)
                          FROM public.us_lacey_processing_jobs AS active
                          WHERE active.organization_id = queued.organization_id
                            AND active.status = 'RUNNING'
                      ) < :per_organization_limit
                    ORDER BY queued.available_at ASC, queued.created_at ASC, queued.id ASC
                    FOR UPDATE OF queued SKIP LOCKED
                    LIMIT 1
                )
                UPDATE public.us_lacey_processing_jobs AS job
                SET status = 'RUNNING',
                    attempt_count = job.attempt_count + 1,
                    locked_by = :worker_id,
                    locked_at = now(),
                    heartbeat_at = now(),
                    current_stage = 'CLAIMED',
                    stage_started_at = now(),
                    started_at = coalesce(job.started_at, now()),
                    last_error_code = NULL,
                    last_error_message = NULL,
                    updated_at = now()
                FROM candidate
                WHERE job.id = candidate.id
                RETURNING job.id, job.public_id, job.organization_id, job.operation_id,
                          job.assurance_document_id, job.status, job.attempt_count,
                          job.max_attempts, job.available_at, job.locked_by
                """
            ),
            {"worker_id": worker_id, "per_organization_limit": per_organization_limit},
        ).mappings().one_or_none()
        session.commit()
        return _job_from_row(row) if row is not None else None
    except Exception as exc:
        session.rollback()
        raise UsLaceyJobError("Unable to claim a processing job.") from exc
    finally:
        session.close()


def set_us_lacey_job_stage(
    *,
    job_id: int,
    worker_id: str,
    stage: str,
) -> bool:
    """Persist one worker stage without extending the stage deadline on re-entry."""
    normalized_stage = str(stage or "").strip().upper()
    if not normalized_stage or len(normalized_stage) > 64:
        raise UsLaceyJobError("stage must be between 1 and 64 characters.")

    session = get_us_lacey_worker_db_session()
    try:
        updated = session.execute(
            text(
                """
                UPDATE public.us_lacey_processing_jobs
                SET stage_started_at = CASE
                        WHEN current_stage IS DISTINCT FROM :stage THEN now()
                        ELSE coalesce(stage_started_at, now())
                    END,
                    current_stage = :stage,
                    updated_at = now()
                WHERE id = :job_id
                  AND status = 'RUNNING'
                  AND locked_by = :worker_id
                RETURNING id
                """
            ),
            {
                "job_id": job_id,
                "worker_id": worker_id,
                "stage": normalized_stage,
            },
        ).scalar_one_or_none()
        session.commit()
        return updated is not None
    except Exception as exc:
        session.rollback()
        raise UsLaceyJobError("Unable to update processing job stage.") from exc
    finally:
        session.close()


def heartbeat_us_lacey_job(*, job_id: int, worker_id: str) -> bool:
    """Refresh a live lease or atomically fail a stage that exceeded five minutes."""
    session = get_us_lacey_worker_db_session()
    try:
        status = session.execute(
            text(
                """
                UPDATE public.us_lacey_processing_jobs
                SET status = CASE
                        WHEN stage_started_at IS NOT NULL
                         AND stage_started_at
                             < now() - make_interval(secs => :stage_timeout_seconds)
                        THEN 'FAILED'
                        ELSE 'RUNNING'
                    END,
                    completed_at = CASE
                        WHEN stage_started_at IS NOT NULL
                         AND stage_started_at
                             < now() - make_interval(secs => :stage_timeout_seconds)
                        THEN now()
                        ELSE completed_at
                    END,
                    heartbeat_at = CASE
                        WHEN stage_started_at IS NOT NULL
                         AND stage_started_at
                             < now() - make_interval(secs => :stage_timeout_seconds)
                        THEN NULL
                        ELSE now()
                    END,
                    locked_by = CASE
                        WHEN stage_started_at IS NOT NULL
                         AND stage_started_at
                             < now() - make_interval(secs => :stage_timeout_seconds)
                        THEN NULL
                        ELSE locked_by
                    END,
                    locked_at = CASE
                        WHEN stage_started_at IS NOT NULL
                         AND stage_started_at
                             < now() - make_interval(secs => :stage_timeout_seconds)
                        THEN NULL
                        ELSE locked_at
                    END,
                    last_error_code = CASE
                        WHEN stage_started_at IS NOT NULL
                         AND stage_started_at
                             < now() - make_interval(secs => :stage_timeout_seconds)
                        THEN 'STAGE_TIMEOUT'
                        ELSE last_error_code
                    END,
                    last_error_message = CASE
                        WHEN stage_started_at IS NOT NULL
                         AND stage_started_at
                             < now() - make_interval(secs => :stage_timeout_seconds)
                        THEN 'Processing stage exceeded the five-minute execution deadline.'
                        ELSE last_error_message
                    END,
                    updated_at = now()
                WHERE id = :job_id
                  AND status = 'RUNNING'
                  AND locked_by = :worker_id
                RETURNING status
                """
            ),
            {
                "job_id": job_id,
                "worker_id": worker_id,
                "stage_timeout_seconds": STAGE_TIMEOUT_SECONDS,
            },
        ).scalar_one_or_none()
        session.commit()
        return status == "RUNNING"
    except Exception as exc:
        session.rollback()
        raise UsLaceyJobError("Unable to heartbeat processing job.") from exc
    finally:
        session.close()


def complete_us_lacey_job(*, job_id: int, worker_id: str) -> bool:
    session = get_us_lacey_worker_db_session()
    try:
        updated = session.execute(
            text(
                """
                UPDATE public.us_lacey_processing_jobs
                SET status = 'COMPLETED', completed_at = now(), heartbeat_at = now(),
                    current_stage = 'COMPLETED', stage_started_at = NULL,
                    locked_by = NULL, locked_at = NULL, updated_at = now()
                WHERE id = :job_id AND status = 'RUNNING' AND locked_by = :worker_id
                RETURNING id
                """
            ),
            {"job_id": job_id, "worker_id": worker_id},
        ).scalar_one_or_none()
        session.commit()
        return updated is not None
    except Exception as exc:
        session.rollback()
        raise UsLaceyJobError("Unable to complete processing job.") from exc
    finally:
        session.close()


def fail_us_lacey_job(
    *,
    job_id: int,
    worker_id: str,
    error_code: str,
    safe_error_message: str,
    retry_delay_seconds: int = 30,
    retryable: bool = True,
) -> str | None:
    """Record a failure; deterministic policy failures can be permanently non-retryable."""
    if retry_delay_seconds < 0 or retry_delay_seconds > 3600:
        raise UsLaceyJobError("retry_delay_seconds is out of range.")
    error_code = str(error_code or "PROCESSING_ERROR").strip()[:100]
    safe_error_message = str(safe_error_message or "Processing failed.").strip()[:2000]

    session = get_us_lacey_worker_db_session()
    try:
        status = session.execute(
            text(
                """
                UPDATE public.us_lacey_processing_jobs
                SET status = CASE
                        WHEN NOT :retryable THEN 'FAILED'
                        WHEN attempt_count < max_attempts THEN 'RETRY'
                        ELSE 'FAILED'
                    END,
                    available_at = CASE
                        WHEN :retryable AND attempt_count < max_attempts
                        THEN now() + make_interval(secs => :retry_delay_seconds)
                        ELSE available_at
                    END,
                    completed_at = CASE
                        WHEN :retryable AND attempt_count < max_attempts THEN NULL
                        ELSE now()
                    END,
                    locked_by = NULL,
                    locked_at = NULL,
                    heartbeat_at = NULL,
                    current_stage = CASE
                        WHEN :retryable AND attempt_count < max_attempts THEN NULL
                        ELSE current_stage
                    END,
                    stage_started_at = CASE
                        WHEN :retryable AND attempt_count < max_attempts THEN NULL
                        ELSE stage_started_at
                    END,
                    last_error_code = :error_code,
                    last_error_message = :error_message,
                    updated_at = now()
                WHERE id = :job_id AND status = 'RUNNING' AND locked_by = :worker_id
                RETURNING status
                """
            ),
            {
                "job_id": job_id,
                "worker_id": worker_id,
                "error_code": error_code,
                "error_message": safe_error_message,
                "retry_delay_seconds": retry_delay_seconds,
                "retryable": bool(retryable),
            },
        ).scalar_one_or_none()
        session.commit()
        return str(status) if status is not None else None
    except Exception as exc:
        session.rollback()
        raise UsLaceyJobError("Unable to record processing failure.") from exc
    finally:
        session.close()


def recover_stale_us_lacey_jobs(
    *,
    stale_after_seconds: int = 600,
    retry_delay_seconds: int = 30,
) -> tuple[int, int]:
    """Recover abandoned RUNNING jobs after process/container interruption.

    Returns (retried, permanently_failed).
    """
    if stale_after_seconds < 60 or stale_after_seconds > 86400:
        raise UsLaceyJobError("stale_after_seconds must be between 60 and 86400.")
    if retry_delay_seconds < 0 or retry_delay_seconds > 3600:
        raise UsLaceyJobError("retry_delay_seconds is out of range.")

    session = get_us_lacey_worker_db_session()
    try:
        rows = session.execute(
            text(
                """
                UPDATE public.us_lacey_processing_jobs
                SET status = CASE WHEN attempt_count < max_attempts THEN 'RETRY' ELSE 'FAILED' END,
                    available_at = CASE
                        WHEN attempt_count < max_attempts
                        THEN now() + make_interval(secs => :retry_delay_seconds)
                        ELSE available_at
                    END,
                    completed_at = CASE WHEN attempt_count < max_attempts THEN NULL ELSE now() END,
                    locked_by = NULL,
                    locked_at = NULL,
                    heartbeat_at = NULL,
                    current_stage = CASE
                        WHEN attempt_count < max_attempts THEN NULL
                        ELSE current_stage
                    END,
                    stage_started_at = CASE
                        WHEN attempt_count < max_attempts THEN NULL
                        ELSE stage_started_at
                    END,
                    last_error_code = 'WORKER_STALE',
                    last_error_message = 'Processing worker stopped before completing this job.',
                    updated_at = now()
                WHERE status = 'RUNNING'
                  AND coalesce(heartbeat_at, locked_at, started_at, created_at)
                      < now() - make_interval(secs => :stale_after_seconds)
                RETURNING status
                """
            ),
            {
                "stale_after_seconds": stale_after_seconds,
                "retry_delay_seconds": retry_delay_seconds,
            },
        ).scalars().all()
        session.commit()
        retried = sum(1 for status in rows if status == "RETRY")
        failed = sum(1 for status in rows if status == "FAILED")
        return retried, failed
    except Exception as exc:
        session.rollback()
        raise UsLaceyJobError("Unable to recover stale processing jobs.") from exc
    finally:
        session.close()
