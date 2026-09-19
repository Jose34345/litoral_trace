"""Add durable physical purge queue for zero-touch sandbox tenants.

Revision ID: 053_sandbox_purge_queue
Revises: 052_us_lacey_ephemeral_sandbox
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "053_sandbox_purge_queue"
down_revision = "052_us_lacey_ephemeral_sandbox"
branch_labels = None
depends_on = None


RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"

TRIGGER_FUNCTION = "public.us_lacey_sandbox_enqueue_purge_job()"
MANIFEST_FUNCTION = "public.us_lacey_sandbox_purge_manifest(bigint,text)"
DATABASE_PURGE_FUNCTION = "public.us_lacey_sandbox_purge_database(bigint,text,jsonb)"


def _grant_temp_platform_set() -> None:
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )


def _revoke_temp_platform_set() -> None:
    op.execute(
        f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER"
    )


def _create_expired_sandbox_delete_policy(table_name: str) -> None:
    policy_name = f"{table_name}_sandbox_purge_platform_delete"
    op.execute(f"DROP POLICY IF EXISTS {policy_name} ON public.{table_name}")
    op.execute(
        f"""
        CREATE POLICY {policy_name}
        ON public.{table_name}
        FOR DELETE
        TO {PLATFORM_ROLE}
        USING (
            EXISTS (
                SELECT 1
                FROM public.organizations AS sandbox_org
                WHERE sandbox_org.id = organization_id
                  AND sandbox_org.is_sandbox = true
                  AND sandbox_org.sandbox_expires_at <= now()
            )
        )
        """
    )
    op.execute(
        f"GRANT DELETE ON TABLE public.{table_name} TO {PLATFORM_ROLE}"
    )
    # DELETE ... WHERE organization_id = ... also requires SELECT privilege
    # on the predicate column. Keep this column-scoped instead of granting
    # cross-tenant table-wide reads to the non-login definer.
    op.execute(
        f"GRANT SELECT (organization_id) "
        f"ON TABLE public.{table_name} TO {PLATFORM_ROLE}"
    )


def upgrade() -> None:
    op.create_table(
        "us_lacey_sandbox_purge_jobs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "state",
            sa.String(32),
            nullable=False,
            server_default="PENDING",
        ),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("locked_by", sa.String(255), nullable=True),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_error_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("storage_confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "organization_id",
            name="uq_us_lacey_sandbox_purge_jobs_org",
        ),
        sa.CheckConstraint(
            """
            state IN (
                'PENDING',
                'STORAGE_DELETING',
                'DB_DELETING',
                'RETRY',
                'COMPLETED',
                'FAILED'
            )
            """,
            name="ck_us_lacey_sandbox_purge_jobs_state",
        ),
        sa.CheckConstraint(
            "attempt_count >= 0",
            name="ck_us_lacey_sandbox_purge_jobs_attempt_count",
        ),
        sa.CheckConstraint(
            """
            (
                state = 'COMPLETED'
                AND completed_at IS NOT NULL
            )
            OR
            (
                state <> 'COMPLETED'
                AND completed_at IS NULL
            )
            """,
            name="ck_us_lacey_sandbox_purge_jobs_completed_state",
        ),
    )

    op.create_index(
        "ix_us_lacey_sandbox_purge_jobs_claim",
        "us_lacey_sandbox_purge_jobs",
        ["state", "available_at", "expires_at", "id"],
        postgresql_where=sa.text("state IN ('PENDING', 'RETRY')"),
    )

    op.execute(
        """
        INSERT INTO public.us_lacey_sandbox_purge_jobs (
            organization_id,
            expires_at,
            state,
            attempt_count,
            available_at,
            created_at,
            updated_at
        )
        SELECT
            id,
            sandbox_expires_at,
            'PENDING',
            0,
            sandbox_expires_at,
            now(),
            now()
        FROM public.organizations
        WHERE is_sandbox = true
          AND sandbox_expires_at IS NOT NULL
        ON CONFLICT (organization_id) DO NOTHING
        """
    )

    op.execute(
        "ALTER TABLE public.us_lacey_sandbox_purge_jobs ENABLE ROW LEVEL SECURITY"
    )
    op.execute(
        "ALTER TABLE public.us_lacey_sandbox_purge_jobs FORCE ROW LEVEL SECURITY"
    )

    op.execute(
        f"""
        CREATE POLICY sandbox_purge_platform_all
        ON public.us_lacey_sandbox_purge_jobs
        FOR ALL
        TO {PLATFORM_ROLE}
        USING (true)
        WITH CHECK (true)
        """
    )
    op.execute(
        f"""
        CREATE POLICY sandbox_purge_worker_select
        ON public.us_lacey_sandbox_purge_jobs
        FOR SELECT
        TO {WORKER_ROLE}
        USING (true)
        """
    )
    op.execute(
        f"""
        CREATE POLICY sandbox_purge_worker_update
        ON public.us_lacey_sandbox_purge_jobs
        FOR UPDATE
        TO {WORKER_ROLE}
        USING (true)
        WITH CHECK (true)
        """
    )

    op.execute(
        "REVOKE ALL ON TABLE public.us_lacey_sandbox_purge_jobs FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL ON TABLE public.us_lacey_sandbox_purge_jobs FROM {RUNTIME_ROLE}"
    )
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE, DELETE "
        f"ON TABLE public.us_lacey_sandbox_purge_jobs TO {PLATFORM_ROLE}"
    )
    op.execute(
        f"GRANT USAGE, SELECT "
        f"ON SEQUENCE public.us_lacey_sandbox_purge_jobs_id_seq TO {PLATFORM_ROLE}"
    )
    op.execute(
        f"GRANT SELECT, UPDATE "
        f"ON TABLE public.us_lacey_sandbox_purge_jobs TO {WORKER_ROLE}"
    )

    op.execute(
        f"GRANT DELETE ON TABLE public.organizations TO {PLATFORM_ROLE}"
    )
    op.execute(
        """
        DROP POLICY IF EXISTS organizations_sandbox_purge_platform_delete
        ON public.organizations
        """
    )
    op.execute(
        f"""
        CREATE POLICY organizations_sandbox_purge_platform_delete
        ON public.organizations
        FOR DELETE
        TO {PLATFORM_ROLE}
        USING (
            is_sandbox = true
            AND sandbox_expires_at IS NOT NULL
            AND sandbox_expires_at <= now()
        )
        """
    )

    op.execute(
        f"GRANT SELECT, DELETE ON TABLE public.vault_documents TO {PLATFORM_ROLE}"
    )
    op.execute(
        """
        DROP POLICY IF EXISTS vault_documents_sandbox_purge_platform_select
        ON public.vault_documents
        """
    )
    op.execute(
        f"""
        CREATE POLICY vault_documents_sandbox_purge_platform_select
        ON public.vault_documents
        FOR SELECT
        TO {PLATFORM_ROLE}
        USING (
            EXISTS (
                SELECT 1
                FROM public.organizations AS sandbox_org
                WHERE sandbox_org.id = organization_id
                  AND sandbox_org.is_sandbox = true
                  AND sandbox_org.sandbox_expires_at <= now()
            )
        )
        """
    )

    for table_name in (
        "vault_documents",
        "us_lacey_operations",
        "assurance_suppliers",
        "batch_evidence_links",
        "integration_documents",
    ):
        _create_expired_sandbox_delete_policy(table_name)

    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_sandbox_enqueue_purge_job()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        BEGIN
            IF NEW.is_sandbox THEN
                IF NEW.sandbox_expires_at IS NULL THEN
                    RAISE EXCEPTION 'sandbox expiration is required'
                        USING ERRCODE = '23514';
                END IF;

                INSERT INTO public.us_lacey_sandbox_purge_jobs (
                    organization_id,
                    expires_at,
                    state,
                    attempt_count,
                    available_at,
                    created_at,
                    updated_at
                )
                VALUES (
                    NEW.id,
                    NEW.sandbox_expires_at,
                    'PENDING',
                    0,
                    NEW.sandbox_expires_at,
                    now(),
                    now()
                )
                ON CONFLICT (organization_id) DO NOTHING;
            END IF;

            RETURN NEW;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_sandbox_purge_manifest(
            requested_job_id bigint,
            requested_worker_id text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            target_org_id integer;
            target_expires_at timestamptz;
            result_manifest jsonb;
        BEGIN
            IF requested_job_id IS NULL OR requested_job_id <= 0 THEN
                RAISE EXCEPTION 'invalid purge job'
                    USING ERRCODE = '22023';
            END IF;

            IF btrim(coalesce(requested_worker_id, '')) = ''
               OR char_length(requested_worker_id) > 255 THEN
                RAISE EXCEPTION 'invalid purge worker'
                    USING ERRCODE = '22023';
            END IF;

            SELECT job.organization_id, job.expires_at
            INTO target_org_id, target_expires_at
            FROM public.us_lacey_sandbox_purge_jobs AS job
            WHERE job.id = requested_job_id
              AND job.state = 'STORAGE_DELETING'
              AND job.locked_by = requested_worker_id
              AND job.expires_at <= now();

            IF NOT FOUND THEN
                RAISE EXCEPTION 'purge job is not owned by this worker'
                    USING ERRCODE = '42501';
            END IF;

            IF NOT EXISTS (
                SELECT 1
                FROM public.organizations AS org
                WHERE org.id = target_org_id
                  AND org.is_sandbox = true
                  AND org.sandbox_expires_at = target_expires_at
                  AND org.sandbox_expires_at <= now()
            ) THEN
                RAISE EXCEPTION 'sandbox tenant is not eligible for purge'
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
            INTO result_manifest
            FROM public.vault_documents AS vault
            WHERE vault.organization_id = target_org_id;

            RETURN result_manifest;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_sandbox_purge_database(
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
              AND org.sandbox_expires_at = target_expires_at
              AND org.sandbox_expires_at <= now()
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

            DELETE FROM public.us_lacey_operations
            WHERE organization_id = target_org_id;

            DELETE FROM public.vault_documents
            WHERE organization_id = target_org_id;

            DELETE FROM public.organizations
            WHERE id = target_org_id
              AND is_sandbox = true
              AND sandbox_expires_at <= now();

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

    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()

    op.execute(
        """
        DROP TRIGGER IF EXISTS trg_us_lacey_sandbox_enqueue_purge
        ON public.organizations
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_us_lacey_sandbox_enqueue_purge
        AFTER INSERT
        ON public.organizations
        FOR EACH ROW
        WHEN (NEW.is_sandbox = true)
        EXECUTE FUNCTION public.us_lacey_sandbox_enqueue_purge_job()
        """
    )

    for signature in (
        TRIGGER_FUNCTION,
        MANIFEST_FUNCTION,
        DATABASE_PURGE_FUNCTION,
    ):
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")
        op.execute(
            f"REVOKE ALL ON FUNCTION {signature} FROM {RUNTIME_ROLE}"
        )

    op.execute(
        f"GRANT EXECUTE ON FUNCTION {MANIFEST_FUNCTION} TO {WORKER_ROLE}"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {DATABASE_PURGE_FUNCTION} TO {WORKER_ROLE}"
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TRIGGER IF EXISTS trg_us_lacey_sandbox_enqueue_purge
        ON public.organizations
        """
    )

    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {MANIFEST_FUNCTION} FROM {WORKER_ROLE}"
    )
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {DATABASE_PURGE_FUNCTION} FROM {WORKER_ROLE}"
    )

    _grant_temp_platform_set()
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")
    op.execute(f"DROP FUNCTION IF EXISTS {DATABASE_PURGE_FUNCTION}")
    op.execute(f"DROP FUNCTION IF EXISTS {MANIFEST_FUNCTION}")
    op.execute(f"DROP FUNCTION IF EXISTS {TRIGGER_FUNCTION}")
    op.execute("RESET ROLE")
    _revoke_temp_platform_set()

    for table_name in (
        "integration_documents",
        "batch_evidence_links",
        "assurance_suppliers",
        "us_lacey_operations",
        "vault_documents",
    ):
        op.execute(
            f"DROP POLICY IF EXISTS "
            f"{table_name}_sandbox_purge_platform_delete "
            f"ON public.{table_name}"
        )

    op.execute(
        """
        DROP POLICY IF EXISTS vault_documents_sandbox_purge_platform_select
        ON public.vault_documents
        """
    )
    op.execute(
        """
        DROP POLICY IF EXISTS organizations_sandbox_purge_platform_delete
        ON public.organizations
        """
    )
    op.execute(
        """
        DROP POLICY IF EXISTS sandbox_purge_worker_update
        ON public.us_lacey_sandbox_purge_jobs
        """
    )
    op.execute(
        """
        DROP POLICY IF EXISTS sandbox_purge_worker_select
        ON public.us_lacey_sandbox_purge_jobs
        """
    )
    op.execute(
        """
        DROP POLICY IF EXISTS sandbox_purge_platform_all
        ON public.us_lacey_sandbox_purge_jobs
        """
    )

    op.drop_index(
        "ix_us_lacey_sandbox_purge_jobs_claim",
        table_name="us_lacey_sandbox_purge_jobs",
    )
    op.drop_table("us_lacey_sandbox_purge_jobs")
