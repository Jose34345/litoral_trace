"""Add sandbox provenance and growth attribution.

Revision ID: 056_us_lacey_sandbox_growth_attribution
Revises: 055_us_lacey_readonly_impersonation
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "056_us_lacey_sandbox_growth_attribution"
down_revision: Union[str, Sequence[str], None] = (
    "055_us_lacey_readonly_impersonation"
)
branch_labels = None
depends_on = None


RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"

CONVERT_FUNCTION = (
    "public.platform_admin_convert_sandbox_to_commercial(text,integer)"
)
COHORT_FUNCTION = (
    "public.platform_admin_sandbox_conversion_cohorts("
    "text,timestamptz,timestamptz)"
)
PROVENANCE_TRIGGER_FUNCTION = (
    "public._us_lacey_apply_sandbox_provenance()"
)


def _grant_temp_platform_set() -> None:
    op.execute(
        "GRANT litoral_trace_platform_definer TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )


def _revoke_temp_platform_set() -> None:
    op.execute(
        "REVOKE litoral_trace_platform_definer "
        "FROM CURRENT_USER GRANTED BY CURRENT_USER"
    )


def upgrade() -> None:
    # Durable provenance survives the sandbox -> commercial state transition.
    op.add_column(
        "organizations",
        sa.Column(
            "created_as_sandbox",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "organizations",
        sa.Column(
            "sandbox_started_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "organizations",
        sa.Column(
            "sandbox_converted_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )

    op.execute(
        """
        UPDATE public.organizations
        SET
            created_as_sandbox = true,
            sandbox_started_at = created_at
        WHERE is_sandbox = true
        """
    )

    op.create_check_constraint(
        "ck_organizations_sandbox_provenance",
        "organizations",
        """
        (
            created_as_sandbox = false
            AND is_sandbox = false
            AND sandbox_started_at IS NULL
            AND sandbox_converted_at IS NULL
        )
        OR
        (
            created_as_sandbox = true
            AND sandbox_started_at IS NOT NULL
            AND (
                (
                    is_sandbox = true
                    AND sandbox_converted_at IS NULL
                )
                OR
                (
                    is_sandbox = false
                    AND sandbox_converted_at IS NOT NULL
                    AND sandbox_converted_at >= sandbox_started_at
                )
            )
        )
        """,
    )

    op.create_index(
        "ix_organizations_sandbox_growth_cohort",
        "organizations",
        ["sandbox_started_at"],
        unique=False,
        postgresql_where=sa.text("created_as_sandbox = true"),
    )

    # Migration 052 predates the provenance columns. This BEFORE trigger keeps
    # every future sandbox creation path attributable without rewriting 052.
    op.execute(
        """
        CREATE FUNCTION public._us_lacey_apply_sandbox_provenance()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY INVOKER
        SET search_path = public, pg_temp
        AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.is_sandbox = true THEN
                    NEW.created_as_sandbox := true;
                    NEW.sandbox_started_at := COALESCE(
                        NEW.sandbox_started_at,
                        NEW.created_at,
                        now()
                    );
                    NEW.sandbox_converted_at := NULL;
                END IF;

                RETURN NEW;
            END IF;

            IF OLD.created_as_sandbox = true
               AND NEW.created_as_sandbox = false THEN
                RAISE EXCEPTION 'sandbox provenance is immutable'
                    USING ERRCODE = '55000';
            END IF;

            IF OLD.created_as_sandbox = true
               AND NEW.sandbox_started_at IS DISTINCT
                   FROM OLD.sandbox_started_at THEN
                RAISE EXCEPTION 'sandbox start timestamp is immutable'
                    USING ERRCODE = '55000';
            END IF;

            IF OLD.sandbox_converted_at IS NOT NULL
               AND NEW.sandbox_converted_at IS DISTINCT
                   FROM OLD.sandbox_converted_at THEN
                RAISE EXCEPTION 'sandbox conversion timestamp is immutable'
                    USING ERRCODE = '55000';
            END IF;

            IF OLD.is_sandbox = false
               AND NEW.is_sandbox = true THEN
                IF OLD.created_as_sandbox = true THEN
                    RAISE EXCEPTION 'converted sandbox cannot be reopened'
                        USING ERRCODE = '55000';
                END IF;

                NEW.created_as_sandbox := true;
                NEW.sandbox_started_at := COALESCE(
                    NEW.sandbox_started_at,
                    now()
                );
                NEW.sandbox_converted_at := NULL;
            END IF;

            RETURN NEW;
        END;
        $$;
        """
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        "public._us_lacey_apply_sandbox_provenance() FROM PUBLIC"
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        "public._us_lacey_apply_sandbox_provenance() FROM litoral_trace_app"
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        "public._us_lacey_apply_sandbox_provenance() "
        "FROM litoral_trace_worker_executor"
    )
    op.execute(
        """
        CREATE TRIGGER trg_us_lacey_sandbox_provenance
        BEFORE INSERT OR UPDATE OF
            is_sandbox,
            created_as_sandbox,
            sandbox_started_at,
            sandbox_converted_at
        ON public.organizations
        FOR EACH ROW
        EXECUTE FUNCTION public._us_lacey_apply_sandbox_provenance()
        """
    )

    # Converted tenants must leave the physical-destruction queue permanently.
    op.drop_constraint(
        "ck_us_lacey_sandbox_purge_jobs_state",
        "us_lacey_sandbox_purge_jobs",
        type_="check",
    )
    op.create_check_constraint(
        "ck_us_lacey_sandbox_purge_jobs_state",
        "us_lacey_sandbox_purge_jobs",
        """
        state IN (
            'PENDING',
            'STORAGE_DELETING',
            'DB_DELETING',
            'RETRY',
            'COMPLETED',
            'FAILED',
            'CANCELED'
        )
        """,
    )

    _grant_temp_platform_set()
    op.execute(
        "GRANT CREATE ON SCHEMA public TO litoral_trace_platform_definer"
    )
    op.execute("SET ROLE litoral_trace_platform_definer")

    op.execute(
        """
        CREATE FUNCTION public.platform_admin_convert_sandbox_to_commercial(
            actor_refresh_token_hash text,
            target_organization_id integer
        )
        RETURNS TABLE(
            organization_id integer,
            converted_at timestamptz,
            purge_job_state text
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            actor record;
            purge_job record;
            org_record record;
            effective_converted_at timestamptz;
            canceled_jobs integer;
        BEGIN
            SELECT *
            INTO actor
            FROM public._platform_superadmin_session_actor(
                actor_refresh_token_hash
            );

            IF target_organization_id IS NULL
               OR target_organization_id <= 0 THEN
                RAISE EXCEPTION 'invalid organization id'
                    USING ERRCODE = '22023';
            END IF;

            /*
             * Lock the purge row before the organization. The physical purge
             * function uses this same job -> organization order, which prevents
             * a conversion-vs-DB_DELETING deadlock. A worker claiming PENDING
             * with FOR UPDATE SKIP LOCKED will skip this row while conversion
             * holds the lock.
             */
            SELECT
                job.id,
                job.state
            INTO purge_job
            FROM public.us_lacey_sandbox_purge_jobs AS job
            WHERE job.organization_id = target_organization_id
            FOR UPDATE;

            IF NOT FOUND THEN
                RAISE EXCEPTION 'sandbox purge job not found'
                    USING ERRCODE = '55000';
            END IF;

            IF purge_job.state <> 'PENDING' THEN
                RAISE EXCEPTION 'sandbox purge has already started'
                    USING ERRCODE = '55000';
            END IF;

            SELECT
                org.id,
                org.created_as_sandbox,
                org.is_sandbox,
                org.sandbox_expires_at,
                org.sandbox_started_at,
                org.sandbox_converted_at
            INTO org_record
            FROM public.organizations AS org
            WHERE org.id = target_organization_id
            FOR UPDATE;

            IF NOT FOUND THEN
                RAISE EXCEPTION 'organization not found'
                    USING ERRCODE = '22023';
            END IF;

            IF org_record.created_as_sandbox IS DISTINCT FROM true
               OR org_record.is_sandbox IS DISTINCT FROM true THEN
                RAISE EXCEPTION 'organization is not an active sandbox'
                    USING ERRCODE = '55000';
            END IF;

            IF org_record.sandbox_converted_at IS NOT NULL THEN
                RAISE EXCEPTION 'sandbox has already been converted'
                    USING ERRCODE = '55000';
            END IF;

            effective_converted_at := now();

            UPDATE public.organizations AS org
            SET
                is_sandbox = false,
                sandbox_expires_at = NULL,
                sandbox_converted_at = effective_converted_at,
                updated_at = effective_converted_at
            WHERE org.id = target_organization_id;

            UPDATE public.us_lacey_sandbox_purge_jobs AS job
            SET
                state = 'CANCELED',
                locked_by = NULL,
                locked_at = NULL,
                heartbeat_at = NULL,
                updated_at = effective_converted_at
            WHERE job.id = purge_job.id
              AND job.organization_id = target_organization_id
              AND job.state = 'PENDING';

            GET DIAGNOSTICS canceled_jobs = ROW_COUNT;

            IF canceled_jobs <> 1 THEN
                RAISE EXCEPTION
                    'sandbox purge cancellation lost concurrent race'
                    USING ERRCODE = '55000';
            END IF;

            PERFORM public._us_lacey_admin_audit(
                actor.actor_user_id,
                actor.actor_organization_id,
                'SANDBOX_CONVERTED',
                target_organization_id,
                NULL,
                jsonb_build_object(
                    'created_as_sandbox',
                        org_record.created_as_sandbox,
                    'is_sandbox',
                        org_record.is_sandbox,
                    'sandbox_expires_at',
                        org_record.sandbox_expires_at,
                    'sandbox_started_at',
                        org_record.sandbox_started_at,
                    'sandbox_converted_at',
                        org_record.sandbox_converted_at,
                    'purge_job_state',
                        purge_job.state
                ),
                jsonb_build_object(
                    'created_as_sandbox', true,
                    'is_sandbox', false,
                    'sandbox_expires_at', NULL,
                    'sandbox_started_at',
                        org_record.sandbox_started_at,
                    'sandbox_converted_at',
                        effective_converted_at,
                    'purge_job_state', 'CANCELED'
                ),
                NULL,
                jsonb_build_object(
                    'purge_job_id', purge_job.id
                )
            );

            PERFORM public._platform_insert_audit_log(
                actor.actor_user_id,
                NULL,
                'superadmin',
                actor.actor_organization_id,
                target_organization_id,
                'SANDBOX_CONVERTED',
                'organization',
                target_organization_id,
                jsonb_build_object(
                    'purge_job_id', purge_job.id,
                    'sandbox_started_at',
                        org_record.sandbox_started_at,
                    'sandbox_converted_at',
                        effective_converted_at
                )
            );

            RETURN QUERY
            SELECT
                target_organization_id,
                effective_converted_at,
                'CANCELED'::text;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.platform_admin_sandbox_conversion_cohorts(
            actor_refresh_token_hash text,
            requested_from timestamptz,
            requested_to timestamptz
        )
        RETURNS TABLE(
            cohort_day timestamptz,
            sandboxes_created bigint,
            converted_to_commercial bigint,
            conversion_rate_pct numeric
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        BEGIN
            PERFORM 1
            FROM public._platform_superadmin_session_actor(
                actor_refresh_token_hash
            );

            IF requested_from IS NOT NULL
               AND requested_to IS NOT NULL
               AND requested_to <= requested_from THEN
                RAISE EXCEPTION 'invalid cohort window'
                    USING ERRCODE = '22023';
            END IF;

            RETURN QUERY
            WITH sandbox_cohort AS (
                SELECT
                    org.id AS organization_id,
                    date_trunc(
                        'day',
                        COALESCE(
                            org.sandbox_started_at,
                            org.created_at
                        )
                    ) AS cohort_day,
                    org.sandbox_converted_at
                FROM public.organizations AS org
                WHERE org.created_as_sandbox = true
                  AND (
                      requested_from IS NULL
                      OR COALESCE(
                          org.sandbox_started_at,
                          org.created_at
                      ) >= requested_from
                  )
                  AND (
                      requested_to IS NULL
                      OR COALESCE(
                          org.sandbox_started_at,
                          org.created_at
                      ) < requested_to
                  )
            )
            SELECT
                cohort.cohort_day,
                count(*)::bigint AS sandboxes_created,
                count(*) FILTER (
                    WHERE
                        cohort.sandbox_converted_at IS NOT NULL
                        AND EXISTS (
                            SELECT 1
                            FROM public.us_lacey_subscriptions
                                AS subscription
                            WHERE
                                subscription.organization_id =
                                    cohort.organization_id
                                AND subscription.status = 'ACTIVE'
                                AND subscription.price_cents > 0
                                AND subscription.plan_code <> 'SANDBOX'
                        )
                )::bigint AS converted_to_commercial,
                round(
                    100.0
                    * count(*) FILTER (
                        WHERE
                            cohort.sandbox_converted_at IS NOT NULL
                            AND EXISTS (
                                SELECT 1
                                FROM public.us_lacey_subscriptions
                                    AS subscription
                                WHERE
                                    subscription.organization_id =
                                        cohort.organization_id
                                    AND subscription.status = 'ACTIVE'
                                    AND subscription.price_cents > 0
                                    AND subscription.plan_code <> 'SANDBOX'
                            )
                    )
                    / NULLIF(count(*), 0),
                    2
                ) AS conversion_rate_pct
            FROM sandbox_cohort AS cohort
            GROUP BY cohort.cohort_day
            ORDER BY cohort.cohort_day ASC;
        END;
        $$;
        """
    )

    for signature in (CONVERT_FUNCTION, COHORT_FUNCTION):
        op.execute(
            "REVOKE ALL ON FUNCTION " + signature + " FROM PUBLIC"
        )
        op.execute(
            "REVOKE ALL ON FUNCTION " + signature
            + " FROM litoral_trace_worker_executor"
        )
        op.execute(
            "GRANT EXECUTE ON FUNCTION " + signature
            + " TO litoral_trace_app"
        )

    op.execute("RESET ROLE")
    op.execute(
        "REVOKE CREATE ON SCHEMA public FROM litoral_trace_platform_definer"
    )
    _revoke_temp_platform_set()


def downgrade() -> None:
    _grant_temp_platform_set()
    op.execute("SET ROLE litoral_trace_platform_definer")
    op.execute(f"DROP FUNCTION IF EXISTS {COHORT_FUNCTION}")
    op.execute(f"DROP FUNCTION IF EXISTS {CONVERT_FUNCTION}")
    op.execute("RESET ROLE")
    _revoke_temp_platform_set()

    op.execute(
        """
        DROP TRIGGER IF EXISTS trg_us_lacey_sandbox_provenance
        ON public.organizations
        """
    )
    op.execute(
        f"DROP FUNCTION IF EXISTS {PROVENANCE_TRIGGER_FUNCTION}"
    )

    # CANCELED does not exist before 056. Remove those durable tombstones before
    # restoring the historical state constraint or PostgreSQL will reject it.
    op.execute(
        """
        DELETE FROM public.us_lacey_sandbox_purge_jobs
        WHERE state = 'CANCELED'
        """
    )

    op.drop_constraint(
        "ck_us_lacey_sandbox_purge_jobs_state",
        "us_lacey_sandbox_purge_jobs",
        type_="check",
    )
    op.create_check_constraint(
        "ck_us_lacey_sandbox_purge_jobs_state",
        "us_lacey_sandbox_purge_jobs",
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
    )

    op.drop_index(
        "ix_organizations_sandbox_growth_cohort",
        table_name="organizations",
    )
    op.drop_constraint(
        "ck_organizations_sandbox_provenance",
        "organizations",
        type_="check",
    )
    op.drop_column("organizations", "sandbox_converted_at")
    op.drop_column("organizations", "sandbox_started_at")
    op.drop_column("organizations", "created_as_sandbox")
