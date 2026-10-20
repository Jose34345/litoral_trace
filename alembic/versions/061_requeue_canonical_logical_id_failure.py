"""Requeue one production operation after the logical-document canonical hotfix.

Revision ID: 061_requeue_canonical_logical_id_failure
Revises: 060_us_lacey_processing_stages

This is an idempotent emergency data migration. It touches only the explicitly
identified failed operation and mirrors retry_failed_us_lacey_operation().
"""
from __future__ import annotations

from alembic import op


revision = "061_requeue_canonical_logical_id_failure"
down_revision = "060_us_lacey_processing_stages"
branch_labels = None
depends_on = None


_OPERATION_UUID = "02fe7759-f8b5-4b30-8277-51f3ebc21a26"


def upgrade() -> None:
    op.execute(
        f"""
        DO $recovery$
        DECLARE
            v_operation_uuid uuid := '{_OPERATION_UUID}'::uuid;
            v_operation_id integer;
            v_organization_id integer;
            v_jobs_requeued integer := 0;
        BEGIN
            SELECT id, organization_id
            INTO v_operation_id, v_organization_id
            FROM public.us_lacey_operations
            WHERE public_id = v_operation_uuid
              AND status = 'FAILED'
            FOR UPDATE;

            IF NOT FOUND THEN
                RAISE NOTICE 'Recovery operation % is absent or no longer FAILED; no-op.',
                    v_operation_uuid;
                RETURN;
            END IF;

            IF EXISTS (
                SELECT 1
                FROM public.us_lacey_processing_jobs
                WHERE operation_id = v_operation_id
                  AND organization_id = v_organization_id
                  AND status IN ('QUEUED', 'RUNNING', 'RETRY')
            ) THEN
                RAISE NOTICE 'Recovery operation % already has active work; no-op.',
                    v_operation_uuid;
                RETURN;
            END IF;

            UPDATE public.assurance_documents AS document
            SET processing_status = 'UPLOADED',
                last_error_code = NULL,
                last_error_message = NULL,
                updated_at = now()
            WHERE document.organization_id = v_organization_id
              AND document.processing_status = 'FAILED'
              AND EXISTS (
                  SELECT 1
                  FROM public.us_lacey_processing_jobs AS job
                  WHERE job.organization_id = v_organization_id
                    AND job.operation_id = v_operation_id
                    AND job.status = 'FAILED'
                    AND job.assurance_document_id = document.id
              );

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
                last_error_code = 'REQUEUED_CANONICAL_LOGICAL_ID_HOTFIX',
                last_error_message = 'Requeued after canonical logical-document ID hotfix',
                updated_at = now()
            WHERE operation_id = v_operation_id
              AND organization_id = v_organization_id
              AND status = 'FAILED';

            GET DIAGNOSTICS v_jobs_requeued = ROW_COUNT;

            IF v_jobs_requeued = 0 THEN
                RAISE NOTICE 'Recovery operation % has no FAILED jobs; leaving operation unchanged.',
                    v_operation_uuid;
                RETURN;
            END IF;

            UPDATE public.us_lacey_operations
            SET status = 'PROCESSING',
                review_result = NULL,
                updated_at = now()
            WHERE id = v_operation_id
              AND organization_id = v_organization_id
              AND status = 'FAILED';

            RAISE NOTICE 'Recovery operation % requeued % failed jobs.',
                v_operation_uuid, v_jobs_requeued;
        END
        $recovery$;
        """
    )


def downgrade() -> None:
    # Operational recovery is intentionally not reversed: a downgrade must never
    # move a successfully retried shipment back to FAILED.
    pass
