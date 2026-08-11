"""Add stale satellite job recovery primitives.

Revision ID: 012_add_satellite_job_stale_recovery
Revises: 011_add_satellite_job_claiming
Create Date: 2026-08-10 00:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "012_add_satellite_job_stale_recovery"
down_revision: Union[str, Sequence[str], None] = "011_add_satellite_job_claiming"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"


def _create_stale_recovery_index() -> None:
    op.create_index(
        "ix_satellite_jobs_running_heartbeat_at",
        "satellite_jobs",
        ["heartbeat_at", "id"],
        unique=False,
        postgresql_where=sa.text(
            "status = 'running' AND heartbeat_at IS NOT NULL"
        ),
    )


def _drop_stale_recovery_index() -> None:
    op.drop_index(
        "ix_satellite_jobs_running_heartbeat_at",
        table_name="satellite_jobs",
    )


def _create_worker_stale_recovery_function() -> None:
    op.execute(
        """
        CREATE FUNCTION public.worker_recover_stale_satellite_jobs(
            requested_batch_size integer DEFAULT 10
        )
        RETURNS TABLE (
            requeued_count integer,
            failed_count integer
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            recovery_now timestamptz := clock_timestamp();
            stale_cutoff timestamptz := recovery_now - interval '90 seconds';
            effective_batch integer := greatest(
                1,
                least(coalesce(requested_batch_size, 10), 100)
            );
        BEGIN
            RETURN QUERY
            WITH candidate AS (
                SELECT
                    jobs.id,
                    jobs.attempt_count,
                    jobs.max_attempts
                FROM public.satellite_jobs AS jobs
                WHERE jobs.status = 'running'
                  AND jobs.finished_at IS NULL
                  AND jobs.locked_at IS NOT NULL
                  AND jobs.locked_by IS NOT NULL
                  AND jobs.heartbeat_at IS NOT NULL
                  AND jobs.lease_token IS NOT NULL
                  AND jobs.heartbeat_at < stale_cutoff
                ORDER BY
                    jobs.heartbeat_at ASC,
                    jobs.id ASC
                FOR UPDATE OF jobs SKIP LOCKED
                LIMIT effective_batch
            ),
            recovered AS (
                UPDATE public.satellite_jobs AS jobs
                SET status = CASE
                        WHEN candidate.attempt_count < candidate.max_attempts
                            THEN 'queued'
                        ELSE 'failed'
                    END,
                    locked_at = NULL,
                    locked_by = NULL,
                    heartbeat_at = NULL,
                    lease_token = CASE
                        WHEN candidate.attempt_count < candidate.max_attempts
                            THEN NULL
                        ELSE jobs.lease_token
                    END,
                    next_attempt_at = CASE
                        WHEN candidate.attempt_count < candidate.max_attempts
                            THEN recovery_now
                        ELSE jobs.next_attempt_at
                    END,
                    finished_at = CASE
                        WHEN candidate.attempt_count < candidate.max_attempts
                            THEN NULL
                        ELSE recovery_now
                    END,
                    error_code = CASE
                        WHEN candidate.attempt_count < candidate.max_attempts
                            THEN NULL
                        ELSE 'stale_recovery_exhausted'
                    END,
                    error_message = CASE
                        WHEN candidate.attempt_count < candidate.max_attempts
                            THEN NULL
                        ELSE (
                            'Satellite job heartbeat expired before completion '
                            'and max attempts were exhausted.'
                        )
                    END,
                    updated_at = recovery_now
                FROM candidate
                WHERE jobs.id = candidate.id
                  AND jobs.status = 'running'
                  AND jobs.finished_at IS NULL
                  AND jobs.locked_at IS NOT NULL
                  AND jobs.locked_by IS NOT NULL
                  AND jobs.heartbeat_at IS NOT NULL
                  AND jobs.lease_token IS NOT NULL
                  AND jobs.heartbeat_at < stale_cutoff
                RETURNING
                    CASE
                        WHEN candidate.attempt_count < candidate.max_attempts
                            THEN 1
                        ELSE 0
                    END AS requeued_inc,
                    CASE
                        WHEN candidate.attempt_count < candidate.max_attempts
                            THEN 0
                        ELSE 1
                    END AS failed_inc
            )
            SELECT
                coalesce(sum(recovered.requeued_inc), 0)::integer,
                coalesce(sum(recovered.failed_inc), 0)::integer
            FROM recovered;
        END;
        $$;
        """
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.worker_recover_stale_satellite_jobs(integer) "
        "FROM PUBLIC"
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.worker_recover_stale_satellite_jobs(integer) "
        f"FROM {RUNTIME_ROLE}"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION public.worker_recover_stale_satellite_jobs(integer) "
        f"TO {WORKER_EXECUTOR_ROLE}"
    )


def _drop_worker_stale_recovery_function() -> None:
    op.execute(
        "REVOKE ALL ON FUNCTION public.worker_recover_stale_satellite_jobs(integer) "
        f"FROM {WORKER_EXECUTOR_ROLE}"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS public.worker_recover_stale_satellite_jobs(integer)"
    )


def upgrade() -> None:
    _create_stale_recovery_index()
    _create_worker_stale_recovery_function()


def downgrade() -> None:
    _drop_worker_stale_recovery_function()
    _drop_stale_recovery_index()
