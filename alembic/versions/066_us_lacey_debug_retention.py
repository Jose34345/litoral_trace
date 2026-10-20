"""Add explicit sandbox support-debug consent and bounded 72-hour retention.

Revision ID: 066_us_lacey_debug_retention
Revises: 065_us_lacey_pilot_watch
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "066_us_lacey_debug_retention"
down_revision = "065_us_lacey_pilot_watch"
branch_labels = None
depends_on = None


RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"

SET_CONSENT_FUNCTION = (
    "public.us_lacey_sandbox_set_debug_consent(text,integer,boolean)"
)
DEBUG_POLICY_FUNCTION = "public.us_lacey_sandbox_debug_policy(integer)"
DEBUG_TRIGGER_FUNCTION = "public._us_lacey_enforce_debug_retention()"
MANIFEST_FUNCTION = "public.us_lacey_sandbox_purge_manifest(bigint,text)"
DATABASE_PURGE_FUNCTION = (
    "public.us_lacey_sandbox_purge_database(bigint,text,jsonb)"
)


def _grant_temp_platform_set() -> None:
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )


def _revoke_temp_platform_set() -> None:
    op.execute(
        f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER"
    )


def _replace_purge_policies(*, debug_aware: bool) -> None:
    deadline = (
        "CASE WHEN sandbox_org.support_debug_consent "
        "THEN sandbox_org.debug_retention_until "
        "ELSE sandbox_org.sandbox_expires_at END"
        if debug_aware
        else "sandbox_org.sandbox_expires_at"
    )
    org_deadline = (
        "CASE WHEN support_debug_consent "
        "THEN debug_retention_until "
        "ELSE sandbox_expires_at END"
        if debug_aware
        else "sandbox_expires_at"
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
            AND ({org_deadline}) IS NOT NULL
            AND ({org_deadline}) <= now()
        )
        """
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
                  AND ({deadline}) IS NOT NULL
                  AND ({deadline}) <= now()
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
        policy_name = f"{table_name}_sandbox_purge_platform_delete"
        op.execute(
            f"DROP POLICY IF EXISTS {policy_name} ON public.{table_name}"
        )
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
                      AND ({deadline}) IS NOT NULL
                      AND ({deadline}) <= now()
                )
            )
            """
        )


def _replace_purge_functions(*, debug_aware: bool) -> None:
    effective_deadline = (
        """
        CASE
            WHEN org.support_debug_consent
                THEN org.debug_retention_until
            ELSE org.sandbox_expires_at
        END
        """
        if debug_aware
        else "org.sandbox_expires_at"
    )

    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.us_lacey_sandbox_purge_manifest(
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
                  AND ({effective_deadline}) = target_expires_at
                  AND ({effective_deadline}) <= now()
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
              AND ({effective_deadline}) = target_expires_at
              AND ({effective_deadline}) <= now()
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


def upgrade() -> None:
    op.add_column(
        "organizations",
        sa.Column(
            "support_debug_consent",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "organizations",
        sa.Column(
            "debug_retention_until",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.create_check_constraint(
        "ck_organizations_debug_retention_consent",
        "organizations",
        """
        (
            support_debug_consent = false
            AND debug_retention_until IS NULL
        )
        OR
        (
            support_debug_consent = true
            AND debug_retention_until IS NOT NULL
        )
        """,
    )
    op.create_index(
        "ix_organizations_debug_retention_until",
        "organizations",
        ["debug_retention_until"],
        unique=False,
        postgresql_where=sa.text("support_debug_consent = true"),
    )

    # SECURITY DEFINER code receives only the columns required for the
    # retention decision. Keep the application login on EXECUTE-only access.
    op.execute(
        f"GRANT SELECT (id, is_sandbox, sandbox_expires_at, "
        f"sandbox_started_at, support_debug_consent, debug_retention_until) "
        f"ON TABLE public.organizations TO {PLATFORM_ROLE}"
    )
    op.execute(
        f"GRANT UPDATE (support_debug_consent, debug_retention_until, updated_at) "
        f"ON TABLE public.organizations TO {PLATFORM_ROLE}"
    )
    op.execute(
        f"GRANT SELECT (organization_id, token_hash, revoked_at, expires_at) "
        f"ON TABLE public.user_sessions TO {PLATFORM_ROLE}"
    )

    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")

    op.execute(
        """
        CREATE FUNCTION public._us_lacey_enforce_debug_retention()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        BEGIN
            IF NEW.is_sandbox IS DISTINCT FROM true THEN
                NEW.support_debug_consent := false;
                NEW.debug_retention_until := NULL;
                RETURN NEW;
            END IF;

            IF NEW.support_debug_consent THEN
                IF NEW.debug_retention_until IS NULL THEN
                    RAISE EXCEPTION 'debug retention deadline is required'
                        USING ERRCODE = '23514';
                END IF;
            ELSE
                NEW.debug_retention_until := NULL;
            END IF;

            RETURN NEW;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_sandbox_set_debug_consent(
            requested_token_hash text,
            requested_organization_id integer,
            requested_consent boolean
        )
        RETURNS timestamptz
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            purge_job record;
            sandbox_org record;
            retention_until timestamptz;
        BEGIN
            IF requested_token_hash IS NULL
               OR requested_token_hash !~ '^[0-9a-f]{64}$' THEN
                RAISE EXCEPTION 'invalid sandbox session token'
                    USING ERRCODE = '22023';
            END IF;
            IF requested_organization_id IS NULL
               OR requested_organization_id <= 0 THEN
                RAISE EXCEPTION 'invalid sandbox organization'
                    USING ERRCODE = '22023';
            END IF;
            IF requested_consent IS NULL THEN
                RAISE EXCEPTION 'debug consent decision is required'
                    USING ERRCODE = '22023';
            END IF;

            IF NOT EXISTS (
                SELECT 1
                FROM public.user_sessions AS session
                WHERE session.organization_id = requested_organization_id
                  AND session.token_hash = requested_token_hash
                  AND session.revoked_at IS NULL
                  AND session.expires_at > now()
            ) THEN
                RAISE EXCEPTION 'sandbox session is not active'
                    USING ERRCODE = '28000';
            END IF;

            SELECT job.id, job.state
            INTO purge_job
            FROM public.us_lacey_sandbox_purge_jobs AS job
            WHERE job.organization_id = requested_organization_id
            FOR UPDATE;

            IF NOT FOUND OR purge_job.state <> 'PENDING' THEN
                RAISE EXCEPTION 'sandbox retention can no longer be changed'
                    USING ERRCODE = '55000';
            END IF;

            SELECT
                org.id,
                org.is_sandbox,
                org.sandbox_expires_at,
                org.sandbox_started_at
            INTO sandbox_org
            FROM public.organizations AS org
            WHERE org.id = requested_organization_id
            FOR UPDATE;

            IF NOT FOUND
               OR sandbox_org.is_sandbox IS DISTINCT FROM true
               OR sandbox_org.sandbox_expires_at IS NULL THEN
                RAISE EXCEPTION 'organization is not an active sandbox'
                    USING ERRCODE = '55000';
            END IF;

            IF requested_consent THEN
                retention_until :=
                    COALESCE(sandbox_org.sandbox_started_at, now())
                    + interval '72 hours';

                UPDATE public.organizations
                SET
                    support_debug_consent = true,
                    debug_retention_until = retention_until,
                    updated_at = now()
                WHERE id = requested_organization_id;

                UPDATE public.us_lacey_sandbox_purge_jobs
                SET
                    expires_at = retention_until,
                    available_at = retention_until,
                    updated_at = now()
                WHERE id = purge_job.id;
            ELSE
                retention_until := NULL;

                UPDATE public.organizations
                SET
                    support_debug_consent = false,
                    debug_retention_until = NULL,
                    updated_at = now()
                WHERE id = requested_organization_id;

                UPDATE public.us_lacey_sandbox_purge_jobs
                SET
                    expires_at = sandbox_org.sandbox_expires_at,
                    available_at = sandbox_org.sandbox_expires_at,
                    updated_at = now()
                WHERE id = purge_job.id;
            END IF;

            RETURN retention_until;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_sandbox_debug_policy(
            requested_organization_id integer
        )
        RETURNS TABLE(
            support_debug_consent boolean,
            debug_retention_until timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            current_org integer;
        BEGIN
            IF requested_organization_id IS NULL
               OR requested_organization_id <= 0 THEN
                RAISE EXCEPTION 'invalid sandbox organization'
                    USING ERRCODE = '22023';
            END IF;

            current_org := NULLIF(
                current_setting('app.current_organization_id', true),
                ''
            )::integer;

            IF current_org IS NULL
               OR current_org <> requested_organization_id THEN
                RAISE EXCEPTION 'tenant context mismatch'
                    USING ERRCODE = '42501';
            END IF;

            RETURN QUERY
            SELECT
                org.support_debug_consent,
                org.debug_retention_until
            FROM public.organizations AS org
            WHERE org.id = requested_organization_id;

            IF NOT FOUND THEN
                RAISE EXCEPTION 'organization not found'
                    USING ERRCODE = '22023';
            END IF;
        END;
        $$;
        """
    )

    _replace_purge_functions(debug_aware=True)

    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()

    op.execute(
        """
        DROP TRIGGER IF EXISTS trg_us_lacey_debug_retention
        ON public.organizations
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_us_lacey_debug_retention
        BEFORE INSERT OR UPDATE OF
            is_sandbox,
            support_debug_consent,
            debug_retention_until
        ON public.organizations
        FOR EACH ROW
        EXECUTE FUNCTION public._us_lacey_enforce_debug_retention()
        """
    )

    _replace_purge_policies(debug_aware=True)

    op.execute(
        f"REVOKE ALL ON FUNCTION {SET_CONSENT_FUNCTION} FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL ON FUNCTION {DEBUG_POLICY_FUNCTION} FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL ON FUNCTION {SET_CONSENT_FUNCTION} FROM {WORKER_ROLE}"
    )
    op.execute(
        f"REVOKE ALL ON FUNCTION {DEBUG_POLICY_FUNCTION} FROM {WORKER_ROLE}"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SET_CONSENT_FUNCTION} TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {DEBUG_POLICY_FUNCTION} TO {RUNTIME_ROLE}"
    )


def downgrade() -> None:
    # Restore four-hour purge scheduling before removing the retention columns.
    op.execute(
        """
        UPDATE public.us_lacey_sandbox_purge_jobs AS job
        SET
            expires_at = org.sandbox_expires_at,
            available_at = org.sandbox_expires_at,
            updated_at = now()
        FROM public.organizations AS org
        WHERE org.id = job.organization_id
          AND org.is_sandbox = true
          AND job.state IN ('PENDING', 'RETRY')
        """
    )
    op.execute(
        """
        UPDATE public.organizations
        SET
            support_debug_consent = false,
            debug_retention_until = NULL,
            updated_at = now()
        WHERE support_debug_consent = true
           OR debug_retention_until IS NOT NULL
        """
    )

    op.execute(
        """
        DROP TRIGGER IF EXISTS trg_us_lacey_debug_retention
        ON public.organizations
        """
    )

    _grant_temp_platform_set()
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {SET_CONSENT_FUNCTION} FROM {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {DEBUG_POLICY_FUNCTION} FROM {RUNTIME_ROLE}"
    )
    op.execute("RESET ROLE")
    _revoke_temp_platform_set()

    _replace_purge_policies(debug_aware=False)

    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")
    _replace_purge_functions(debug_aware=False)
    op.execute(f"DROP FUNCTION IF EXISTS {DEBUG_POLICY_FUNCTION}")
    op.execute(f"DROP FUNCTION IF EXISTS {SET_CONSENT_FUNCTION}")
    op.execute(f"DROP FUNCTION IF EXISTS {DEBUG_TRIGGER_FUNCTION}")
    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()

    op.drop_index(
        "ix_organizations_debug_retention_until",
        table_name="organizations",
    )
    op.drop_constraint(
        "ck_organizations_debug_retention_consent",
        "organizations",
        type_="check",
    )
    op.drop_column("organizations", "debug_retention_until")
    op.drop_column("organizations", "support_debug_consent")
