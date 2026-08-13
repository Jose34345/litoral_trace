"""Add aggregate-only satellite queue metrics.

Revision ID: 015_add_satellite_queue_metrics
Revises: 014_harden_audit_log_runtime_privileges
Create Date: 2026-08-12 00:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "015_add_satellite_queue_metrics"
down_revision: Union[str, Sequence[str], None] = (
    "014_harden_audit_log_runtime_privileges"
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
METRICS_FUNCTION = "public.worker_get_satellite_queue_metrics()"


def _create_running_locked_at_index() -> None:
    op.create_index(
        "ix_satellite_jobs_running_locked_at",
        "satellite_jobs",
        ["locked_at"],
        unique=False,
        postgresql_where=sa.text(
            "status = 'running' AND finished_at IS NULL"
        ),
    )


def _create_queue_metrics_function() -> None:
    op.execute(
        """
        CREATE FUNCTION public.worker_get_satellite_queue_metrics()
        RETURNS TABLE (
            snapshot_time timestamptz,
            queued_ready_count bigint,
            queued_delayed_count bigint,
            running_count bigint,
            running_stale_count bigint,
            running_invalid_count bigint,
            oldest_ready_age_seconds double precision,
            oldest_active_lease_age_seconds double precision,
            oldest_heartbeat_age_seconds double precision,
            next_delayed_ready_in_seconds double precision
        )
        LANGUAGE sql
        VOLATILE
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
            WITH snapshot AS (
                SELECT pg_catalog.clock_timestamp() AS measured_at
            ),
            classified AS (
                SELECT
                    jobs.*,
                    snapshot.measured_at,
                    (
                        jobs.status = 'queued'
                        AND jobs.attempt_count < jobs.max_attempts
                        AND jobs.finished_at IS NULL
                        AND jobs.locked_at IS NULL
                        AND jobs.locked_by IS NULL
                        AND jobs.heartbeat_at IS NULL
                        AND jobs.lease_token IS NULL
                    ) AS queued_eligible,
                    (
                        jobs.status = 'running'
                        AND jobs.finished_at IS NULL
                        AND jobs.locked_at IS NOT NULL
                        AND jobs.locked_by IS NOT NULL
                        AND jobs.heartbeat_at IS NOT NULL
                        AND jobs.lease_token IS NOT NULL
                    ) AS running_valid
                FROM public.satellite_jobs AS jobs
                CROSS JOIN snapshot
            )
            , aggregated AS (
            SELECT
                count(*) FILTER (
                    WHERE queued_eligible
                      AND next_attempt_at <= measured_at
                ) AS queued_ready_count,
                count(*) FILTER (
                    WHERE queued_eligible
                      AND next_attempt_at > measured_at
                ) AS queued_delayed_count,
                count(*) FILTER (
                    WHERE status = 'running'
                ) AS running_count,
                count(*) FILTER (
                    WHERE running_valid
                      AND heartbeat_at
                          < measured_at - interval '90 seconds'
                ) AS running_stale_count,
                count(*) FILTER (
                    WHERE status = 'running'
                      AND NOT running_valid
                ) AS running_invalid_count,
                greatest(
                    pg_catalog.date_part(
                        'epoch', (
                            max(measured_at)
                            - (min(created_at) FILTER (
                                WHERE queued_eligible
                                  AND next_attempt_at <= measured_at
                            ))
                        )
                    ),
                    0
                )::double precision AS oldest_ready_age_seconds,
                greatest(
                    pg_catalog.date_part(
                        'epoch', (
                            max(measured_at)
                            - (min(locked_at) FILTER (
                                WHERE running_valid
                            ))
                        )
                    ),
                    0
                )::double precision
                    AS oldest_active_lease_age_seconds,
                greatest(
                    pg_catalog.date_part(
                        'epoch', (
                            max(measured_at)
                            - (min(heartbeat_at) FILTER (
                                WHERE running_valid
                            ))
                        )
                    ),
                    0
                )::double precision AS oldest_heartbeat_age_seconds,
                greatest(
                    pg_catalog.date_part(
                        'epoch', (
                            (min(next_attempt_at) FILTER (
                                WHERE queued_eligible
                                  AND next_attempt_at > measured_at
                            ))
                            - max(measured_at)
                        )
                    ),
                    0
                )::double precision AS next_delayed_ready_in_seconds
            FROM classified
            )
            SELECT
                snapshot.measured_at AS snapshot_time,
                aggregated.queued_ready_count,
                aggregated.queued_delayed_count,
                aggregated.running_count,
                aggregated.running_stale_count,
                aggregated.running_invalid_count,
                aggregated.oldest_ready_age_seconds,
                aggregated.oldest_active_lease_age_seconds,
                aggregated.oldest_heartbeat_age_seconds,
                aggregated.next_delayed_ready_in_seconds
            FROM snapshot
            CROSS JOIN aggregated;
        $$;
        """
    )
    op.execute(
        f"REVOKE ALL ON FUNCTION {METRICS_FUNCTION} FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL ON FUNCTION {METRICS_FUNCTION} FROM {RUNTIME_ROLE}"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {METRICS_FUNCTION} "
        f"TO {WORKER_EXECUTOR_ROLE}"
    )


def upgrade() -> None:
    _create_running_locked_at_index()
    _create_queue_metrics_function()


def downgrade() -> None:
    op.execute(
        f"REVOKE ALL ON FUNCTION {METRICS_FUNCTION} "
        f"FROM {WORKER_EXECUTOR_ROLE}"
    )
    op.execute(
        f"DROP FUNCTION IF EXISTS {METRICS_FUNCTION}"
    )
    op.drop_index(
        "ix_satellite_jobs_running_locked_at",
        table_name="satellite_jobs",
    )
