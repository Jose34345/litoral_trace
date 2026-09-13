"""Fix ambiguous PL/pgSQL output-column references in U.S. account transitions.

Revision ID: 036_fix_us_lacey_status_ambiguity
Revises: 035_us_lacey_self_service
Create Date: 2026-08-30 16:05:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "036_fix_us_lacey_status_ambiguity"
down_revision: Union[str, Sequence[str], None] = "035_us_lacey_self_service"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

PLATFORM_ROLE = "litoral_trace_platform_definer"


def _grant_temp_platform_set() -> None:
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )


def _revoke_temp_platform_set() -> None:
    op.execute(f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER")


def _replace_functions(*, qualified: bool) -> None:
    profile_email_predicate = (
        "us_lacey_organization_profiles.account_status = 'PENDING_EMAIL'"
        if qualified
        else "account_status = 'PENDING_EMAIL'"
    )
    profile_payment_org = (
        "us_lacey_organization_profiles.organization_id"
        if qualified
        else "organization_id"
    )
    profile_payment_status = (
        "us_lacey_organization_profiles.account_status"
        if qualified
        else "account_status"
    )

    # CREATE OR REPLACE still requires CREATE on the containing schema for the
    # effective function owner. Mirror the bounded ownership pattern from 028/035:
    # grant only for this transactional replacement, then revoke before return.
    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")

    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.us_lacey_verify_email(requested_token_hash text)
        RETURNS TABLE (organization_id integer, user_id integer, account_status text)
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            verification_id integer;
            target_org_id integer;
            target_user_id integer;
        BEGIN
            SELECT
                us_lacey_email_verifications.id,
                us_lacey_email_verifications.organization_id,
                us_lacey_email_verifications.user_id
            INTO verification_id, target_org_id, target_user_id
            FROM public.us_lacey_email_verifications
            WHERE us_lacey_email_verifications.token_hash = requested_token_hash
              AND us_lacey_email_verifications.used_at IS NULL
              AND us_lacey_email_verifications.expires_at > now()
            FOR UPDATE;

            IF verification_id IS NULL THEN
                RAISE EXCEPTION 'verification token invalid or expired'
                    USING ERRCODE = '22023';
            END IF;

            UPDATE public.us_lacey_email_verifications
            SET used_at = now()
            WHERE us_lacey_email_verifications.id = verification_id;

            UPDATE public.us_lacey_organization_profiles
            SET account_status = 'PAYMENT_PENDING', updated_at = now()
            WHERE us_lacey_organization_profiles.organization_id = target_org_id
              AND {profile_email_predicate};

            RETURN QUERY SELECT
                target_org_id,
                target_user_id,
                'PAYMENT_PENDING'::text;
        END;
        $$;
        """
    )

    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.us_lacey_verify_payment(
            actor_refresh_token_hash text,
            target_organization_id integer,
            target_payment_public_id uuid
        )
        RETURNS TABLE (
            payment_status text,
            subscription_status text,
            account_status text
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            payment_subscription_id integer;
        BEGIN
            PERFORM 1
            FROM public._platform_superadmin_session_actor(actor_refresh_token_hash);

            SELECT us_lacey_payments.subscription_id
            INTO payment_subscription_id
            FROM public.us_lacey_payments
            WHERE us_lacey_payments.organization_id = target_organization_id
              AND us_lacey_payments.public_id = target_payment_public_id
              AND us_lacey_payments.status = 'PENDING'
            FOR UPDATE;

            IF payment_subscription_id IS NULL THEN
                RAISE EXCEPTION 'pending payment not found'
                    USING ERRCODE = 'P0002';
            END IF;

            UPDATE public.us_lacey_payments
            SET status = 'VERIFIED',
                paid_at = coalesce(us_lacey_payments.paid_at, now()),
                verified_at = now(),
                updated_at = now()
            WHERE us_lacey_payments.organization_id = target_organization_id
              AND us_lacey_payments.public_id = target_payment_public_id;

            UPDATE public.us_lacey_subscriptions
            SET status = 'ACTIVE',
                started_at = coalesce(us_lacey_subscriptions.started_at, now()),
                updated_at = now()
            WHERE us_lacey_subscriptions.organization_id = target_organization_id
              AND us_lacey_subscriptions.id = payment_subscription_id;

            UPDATE public.us_lacey_organization_profiles
            SET account_status = 'ACTIVE', updated_at = now()
            WHERE {profile_payment_org} = target_organization_id
              AND {profile_payment_status} IN ('PAYMENT_PENDING','PILOT');

            RETURN QUERY SELECT
                'VERIFIED'::text,
                'ACTIVE'::text,
                'ACTIVE'::text;
        END;
        $$;
        """
    )

    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()


def upgrade() -> None:
    _replace_functions(qualified=True)


def downgrade() -> None:
    # Restore the exact 035 definitions when intentionally stepping back to 035.
    _replace_functions(qualified=False)
