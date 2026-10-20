"""Add read-only U.S. Lacey owner/admin account overview.

Revision ID: 042_us_lacey_owner_admin
Revises: 041_us_lacey_lemon
Create Date: 2026-09-03 15:10:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "042_us_lacey_owner_admin"
down_revision: Union[str, Sequence[str], None] = "041_us_lacey_lemon"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"
OVERVIEW_SIGNATURE = "public.platform_us_lacey_account_overview(text)"


def _grant_temp_platform_set() -> None:
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )


def _revoke_temp_platform_set() -> None:
    op.execute(f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER")


def upgrade() -> None:
    """Expose a least-privilege, read-only cross-tenant owner view.

    The web runtime receives EXECUTE only. All underlying U.S. Lacey tables keep
    their existing FORCE-RLS policies and grants. Cross-tenant visibility exists
    solely inside a SECURITY DEFINER function whose first operation validates a
    persistent superadmin platform session through the hardened 028 control plane.
    """
    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET ROLE {PLATFORM_ROLE}")

    op.execute(
        """
        CREATE FUNCTION public.platform_us_lacey_account_overview(
            actor_refresh_token_hash text
        )
        RETURNS TABLE (
            organization_id integer,
            organization_name text,
            legal_name text,
            business_type text,
            admin_contact_email text,
            account_status text,
            payment_provider text,
            payment_status text,
            payment_amount_cents integer,
            subscription_status text,
            monthly_operation_limit integer,
            used_operations integer,
            queued_jobs bigint,
            running_jobs bigint,
            retry_jobs bigint,
            failed_jobs bigint,
            last_payment_event_at timestamptz,
            account_created_at timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        BEGIN
            PERFORM 1
            FROM public._platform_superadmin_session_actor(actor_refresh_token_hash);

            RETURN QUERY
            SELECT
                profile.organization_id,
                organization.name::text,
                profile.legal_name::text,
                profile.business_type::text,
                profile.admin_contact_email::text,
                profile.account_status::text,
                payment.provider::text,
                payment.status::text,
                payment.amount_cents,
                subscription.status::text,
                subscription.monthly_operation_limit,
                subscription.used_operations,
                coalesce(job_counts.queued_jobs, 0)::bigint,
                coalesce(job_counts.running_jobs, 0)::bigint,
                coalesce(job_counts.retry_jobs, 0)::bigint,
                coalesce(job_counts.failed_jobs, 0)::bigint,
                payment_event.last_payment_event_at,
                profile.created_at
            FROM public.us_lacey_organization_profiles AS profile
            JOIN public.organizations AS organization
              ON organization.id = profile.organization_id
            LEFT JOIN public.us_lacey_subscriptions AS subscription
              ON subscription.organization_id = profile.organization_id
            LEFT JOIN LATERAL (
                SELECT
                    candidate.provider,
                    candidate.status,
                    candidate.amount_cents
                FROM public.us_lacey_payments AS candidate
                WHERE candidate.organization_id = profile.organization_id
                ORDER BY candidate.created_at DESC, candidate.id DESC
                LIMIT 1
            ) AS payment ON true
            LEFT JOIN LATERAL (
                SELECT
                    count(*) FILTER (WHERE job.status = 'QUEUED') AS queued_jobs,
                    count(*) FILTER (WHERE job.status = 'RUNNING') AS running_jobs,
                    count(*) FILTER (WHERE job.status = 'RETRY') AS retry_jobs,
                    count(*) FILTER (WHERE job.status = 'FAILED') AS failed_jobs
                FROM public.us_lacey_processing_jobs AS job
                WHERE job.organization_id = profile.organization_id
            ) AS job_counts ON true
            LEFT JOIN LATERAL (
                SELECT max(event.processed_at) AS last_payment_event_at
                FROM public.us_lacey_payment_events AS event
                WHERE event.organization_id = profile.organization_id
            ) AS payment_event ON true
            ORDER BY profile.organization_id ASC;
        END;
        $$;
        """
    )

    # The owner console is intentionally read-only. Runtime gets only EXECUTE on
    # this curated projection; the worker and PUBLIC get no access at all.
    op.execute(f"REVOKE ALL ON FUNCTION {OVERVIEW_SIGNATURE} FROM PUBLIC")
    op.execute(f"REVOKE ALL ON FUNCTION {OVERVIEW_SIGNATURE} FROM {WORKER_ROLE}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {OVERVIEW_SIGNATURE} TO {RUNTIME_ROLE}")

    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()


def downgrade() -> None:
    _grant_temp_platform_set()
    op.execute(f"SET ROLE {PLATFORM_ROLE}")
    op.execute(f"DROP FUNCTION IF EXISTS {OVERVIEW_SIGNATURE}")
    op.execute("RESET ROLE")
    _revoke_temp_platform_set()
