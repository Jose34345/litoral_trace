"""Fix sandbox purge ordering for post-053 U.S. Lacey child tables.

Revision ID: 072_fix_sandbox_purge_fk_order
Revises: 071_us_lacey_reusable_supplier_evidence
"""
from __future__ import annotations

from alembic import op


revision = "072_fix_sandbox_purge_fk_order"
down_revision = "071_us_lacey_reusable_supplier_evidence"
branch_labels = None
depends_on = None

PLATFORM_ROLE = "litoral_trace_platform_definer"

_EFFECTIVE_DEADLINE = """
CASE
    WHEN org.support_debug_consent
        THEN org.debug_retention_until
    ELSE org.sandbox_expires_at
END
"""


def _grant_temp_platform_set() -> None:
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )


def _revoke_temp_platform_set() -> None:
    op.execute(
        f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER"
    )


def _replace_purge_function(*, delete_post_053_children: bool) -> None:
    child_cleanup = ""
    if delete_post_053_children:
        child_cleanup = """
            DELETE FROM public.us_lacey_evidence_snapshot_documents
            WHERE organization_id = target_org_id;

            DELETE FROM public.us_lacey_source_set_members
            WHERE organization_id = target_org_id;
"""

    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.us_lacey_sandbox_purge_database(
            requested_job_id bigint,
            requested_worker_id text,
            expected_manifest jsonb
        )
        RETURNS boolean
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            target_org_id integer;
            target_expires_at timestamptz;
            current_manifest jsonb;
            deleted_orgs integer;
        BEGIN
            IF expected_manifest IS NULL
               OR jsonb_typeof(expected_manifest) <> 'array' THEN
                RAISE EXCEPTION 'invalid expected manifest'
                    USING ERRCODE = '22023';
            END IF;

            SELECT job.organization_id, job.expires_at
            INTO target_org_id, target_expires_at
            FROM public.us_lacey_sandbox_purge_jobs AS job
            WHERE job.id = requested_job_id
              AND job.state = 'DB_DELETING'
              AND job.locked_by = requested_worker_id
              AND job.storage_confirmed_at IS NOT NULL
            FOR UPDATE;

            IF NOT FOUND THEN
                RAISE EXCEPTION 'purge job is not ready for database deletion'
                    USING ERRCODE = '42501';
            END IF;

            PERFORM org.id
            FROM public.organizations AS org
            WHERE org.id = target_org_id
              AND org.is_sandbox = true
              AND ({_EFFECTIVE_DEADLINE}) = target_expires_at
              AND ({_EFFECTIVE_DEADLINE}) <= now()
            FOR UPDATE;

            IF NOT FOUND THEN
                RAISE EXCEPTION 'sandbox tenant no longer exists or is not expired'
                    USING ERRCODE = '55000';
            END IF;

            SELECT COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'id', vault.id,
                        'bucket', vault.storage_bucket,
                        'key', vault.object_key,
                        'version_id', vault.storage_version_id
                    )
                    ORDER BY vault.id
                ),
                '[]'::jsonb
            )
            INTO current_manifest
            FROM public.vault_documents AS vault
            WHERE vault.organization_id = target_org_id;

            IF current_manifest IS DISTINCT FROM expected_manifest THEN
                RAISE EXCEPTION 'sandbox purge manifest changed'
                    USING ERRCODE = 'P0001';
            END IF;

            DELETE FROM public.batch_evidence_links
            WHERE organization_id = target_org_id;

            DELETE FROM public.integration_documents
            WHERE organization_id = target_org_id;

            DELETE FROM public.assurance_suppliers
            WHERE organization_id = target_org_id;

            {child_cleanup}

            DELETE FROM public.us_lacey_operations
            WHERE organization_id = target_org_id;

            DELETE FROM public.vault_documents
            WHERE organization_id = target_org_id;

            DELETE FROM public.organizations
            WHERE id = target_org_id
              AND is_sandbox = true;

            GET DIAGNOSTICS deleted_orgs = ROW_COUNT;

            IF deleted_orgs <> 1 THEN
                RAISE EXCEPTION 'sandbox organization deletion failed'
                    USING ERRCODE = 'P0001';
            END IF;

            UPDATE public.us_lacey_sandbox_purge_jobs
            SET
                state = 'COMPLETED',
                completed_at = now(),
                heartbeat_at = now(),
                locked_by = NULL,
                locked_at = NULL,
                last_error = NULL,
                last_error_at = NULL,
                updated_at = now()
            WHERE id = requested_job_id;

            RETURN true;
        END;
        $$;
        """
    )


def _run_with_platform_owner(*, delete_post_053_children: bool) -> None:
    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")
    _replace_purge_function(
        delete_post_053_children=delete_post_053_children
    )
    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()


def upgrade() -> None:
    _run_with_platform_owner(delete_post_053_children=True)


def downgrade() -> None:
    _run_with_platform_owner(delete_post_053_children=False)
