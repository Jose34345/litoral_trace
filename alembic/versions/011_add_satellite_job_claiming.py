"""Add atomic satellite job claiming primitives.

Revision ID: 011_add_satellite_job_claiming
Revises: 010_add_satellite_jobs
Create Date: 2026-08-09 12:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "011_add_satellite_job_claiming"
down_revision: Union[str, Sequence[str], None] = "010_add_satellite_jobs"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"


def _ensure_worker_capability_role() -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM pg_roles
                WHERE rolname = '{WORKER_EXECUTOR_ROLE}'
            ) THEN
                ALTER ROLE {WORKER_EXECUTOR_ROLE}
                    WITH NOLOGIN
                    NOSUPERUSER
                    NOCREATEDB
                    NOCREATEROLE
                    NOINHERIT
                    NOBYPASSRLS;
            ELSE
                CREATE ROLE {WORKER_EXECUTOR_ROLE}
                    WITH NOLOGIN
                    NOSUPERUSER
                    NOCREATEDB
                    NOCREATEROLE
                    NOINHERIT
                    NOBYPASSRLS;
            END IF;
        END
        $$;
        """
    )


def _revoke_worker_direct_satellite_job_access() -> None:
    op.execute(
        "REVOKE ALL ON TABLE public.satellite_jobs "
        f"FROM {WORKER_EXECUTOR_ROLE}"
    )
    op.execute(
        "REVOKE ALL ON SEQUENCE public.satellite_jobs_id_seq "
        f"FROM {WORKER_EXECUTOR_ROLE}"
    )


def _create_worker_claim_function() -> None:
    op.execute(
        """
        CREATE FUNCTION public.worker_claim_next_satellite_job(
            requested_worker_id text
        )
        RETURNS TABLE (
            id integer,
            organization_id integer,
            lote_id integer,
            job_type text,
            status text,
            attempt_count integer,
            max_attempts integer,
            next_attempt_at timestamptz,
            locked_by text,
            locked_at timestamptz,
            heartbeat_at timestamptz,
            lease_token uuid,
            started_at timestamptz,
            request_start_date date,
            request_end_date date,
            max_cloud_pct double precision,
            geometry_hash text,
            algorithm_version text,
            polygon_wkt_snapshot text
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            normalized_worker_id text := btrim(coalesce(requested_worker_id, ''));
        BEGIN
            IF normalized_worker_id = '' THEN
                RAISE EXCEPTION 'worker_id is required'
                    USING ERRCODE = '22023';
            END IF;

            IF char_length(normalized_worker_id) > 255 THEN
                RAISE EXCEPTION 'worker_id exceeds 255 characters'
                    USING ERRCODE = '22023';
            END IF;

            RETURN QUERY
            WITH candidate AS (
                SELECT jobs.id
                FROM public.satellite_jobs AS jobs
                WHERE jobs.status = 'queued'
                  AND jobs.next_attempt_at <= now()
                  AND jobs.attempt_count < jobs.max_attempts
                  AND jobs.finished_at IS NULL
                  AND jobs.locked_at IS NULL
                  AND jobs.locked_by IS NULL
                  AND jobs.heartbeat_at IS NULL
                  AND jobs.lease_token IS NULL
                ORDER BY
                    jobs.next_attempt_at ASC,
                    jobs.created_at ASC,
                    jobs.id ASC
                FOR UPDATE OF jobs SKIP LOCKED
                LIMIT 1
            )
            UPDATE public.satellite_jobs AS jobs
            SET status = 'running',
                attempt_count = jobs.attempt_count + 1,
                locked_by = normalized_worker_id,
                locked_at = now(),
                heartbeat_at = now(),
                started_at = coalesce(jobs.started_at, now()),
                lease_token = pg_catalog.gen_random_uuid(),
                updated_at = now()
            FROM candidate
            WHERE jobs.id = candidate.id
            RETURNING
                jobs.id,
                jobs.organization_id,
                jobs.lote_id,
                jobs.job_type::text,
                jobs.status::text,
                jobs.attempt_count,
                jobs.max_attempts,
                jobs.next_attempt_at,
                jobs.locked_by::text,
                jobs.locked_at,
                jobs.heartbeat_at,
                jobs.lease_token,
                jobs.started_at,
                jobs.request_start_date,
                jobs.request_end_date,
                jobs.max_cloud_pct,
                jobs.geometry_hash::text,
                jobs.algorithm_version::text,
                jobs.polygon_wkt_snapshot::text;
        END;
        $$;
        """
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.worker_claim_next_satellite_job(text) "
        "FROM PUBLIC"
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.worker_claim_next_satellite_job(text) "
        f"FROM {RUNTIME_ROLE}"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION public.worker_claim_next_satellite_job(text) "
        f"TO {WORKER_EXECUTOR_ROLE}"
    )


def _drop_worker_claim_function() -> None:
    op.execute(
        "REVOKE ALL ON FUNCTION public.worker_claim_next_satellite_job(text) "
        f"FROM {WORKER_EXECUTOR_ROLE}"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS public.worker_claim_next_satellite_job(text)"
    )


def _maybe_drop_worker_capability_role() -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM pg_roles
                WHERE rolname = '{WORKER_EXECUTOR_ROLE}'
            ) THEN
                BEGIN
                    DROP ROLE {WORKER_EXECUTOR_ROLE};
                EXCEPTION
                    WHEN dependent_objects_still_exist OR object_in_use THEN
                        NULL;
                END;
            END IF;
        END
        $$;
        """
    )


def upgrade() -> None:
    op.add_column(
        "satellite_jobs",
        sa.Column("lease_token", postgresql.UUID(as_uuid=False), nullable=True),
    )
    op.create_index(
        "uq_satellite_jobs_lease_token_non_null",
        "satellite_jobs",
        ["lease_token"],
        unique=True,
        postgresql_where=sa.text("lease_token IS NOT NULL"),
    )
    _ensure_worker_capability_role()
    _revoke_worker_direct_satellite_job_access()
    _create_worker_claim_function()


def downgrade() -> None:
    _drop_worker_claim_function()
    _revoke_worker_direct_satellite_job_access()
    _maybe_drop_worker_capability_role()
    op.drop_index(
        "uq_satellite_jobs_lease_token_non_null",
        table_name="satellite_jobs",
    )
    op.drop_column("satellite_jobs", "lease_token")
