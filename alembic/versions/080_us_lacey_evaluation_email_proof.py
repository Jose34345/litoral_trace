"""Email-ownership challenge for product-led U.S. Lacey evaluation claims.

Revision ID: 080_us_lacey_evaluation_email_proof
Revises: 079_us_lacey_assurance_v2_claim_snapshots
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "080_us_lacey_evaluation_email_proof"
down_revision = "079_us_lacey_assurance_v2_claim_snapshots"
branch_labels = None
depends_on = None

PLATFORM_ROLE = "litoral_trace_platform_definer"
RUNTIME_ROLE = "litoral_trace_app"
BEGIN_FN = "public.us_lacey_evaluation_claim_begin(text,text,text)"
FINISH_FN = "public.us_lacey_evaluation_claim_finish(text,text)"


def _set_platform_role():
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")


def _reset_platform_role():
    op.execute("RESET ROLE")
    op.execute(f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER")


def upgrade() -> None:
    # Keep signup's existing one-use token table and privileges; evaluation
    # challenges are distinguishable by requested_email IS NOT NULL.
    op.add_column(
        "us_lacey_email_verifications",
        sa.Column("requested_email", sa.String(255), nullable=True),
    )
    op.create_index(
        "ix_us_lacey_eval_email_challenge_org_time",
        "us_lacey_email_verifications",
        ["organization_id", "created_at"],
    )
    _set_platform_role()
    op.execute(
        """
        CREATE FUNCTION public.us_lacey_evaluation_claim_begin(
            requested_session_hash text,
            requested_work_email text,
            requested_challenge_hash text
        )
        RETURNS TABLE(organization_id integer, user_id integer)
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            actor record;
            evaluation record;
            normalized_email text;
            recent_count integer;
        BEGIN
            normalized_email := lower(btrim(coalesce(requested_work_email, '')));
            IF normalized_email = ''
               OR char_length(normalized_email) > 255
               OR normalized_email !~ '^[^[:space:]@]+@[^[:space:]@]+[.][^[:space:]@]+$'
               OR requested_challenge_hash !~ '^[a-f0-9]{64}$' THEN
                RAISE EXCEPTION 'verification request is invalid' USING ERRCODE = '22023';
            END IF;

            SELECT s.organization_id, s.user_id INTO actor
            FROM public.user_sessions AS s
            JOIN public.users AS usr ON usr.id = s.user_id
                AND usr.organization_id = s.organization_id AND usr.is_active
            JOIN public.organizations AS org ON org.id = s.organization_id
                AND org.is_active
            WHERE s.token_hash = requested_session_hash
              AND s.revoked_at IS NULL AND s.expires_at > now()
            FOR UPDATE OF s;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'evaluation session is invalid or expired' USING ERRCODE = '42501';
            END IF;

            SELECT e.status, e.successful_operations_used INTO evaluation
            FROM public.us_lacey_evaluations AS e
            WHERE e.organization_id = actor.organization_id FOR UPDATE;
            IF NOT FOUND OR evaluation.status <> 'ANONYMOUS'
               OR evaluation.successful_operations_used < 1
               OR NOT EXISTS (
                   SELECT 1 FROM public.organizations AS org
                   WHERE org.id = actor.organization_id
                     AND org.is_sandbox AND org.sandbox_expires_at > now()
               ) THEN
                RAISE EXCEPTION 'evaluation cannot be saved yet' USING ERRCODE = '55000';
            END IF;

            SELECT count(*)::integer INTO recent_count
            FROM public.us_lacey_email_verifications AS v
            WHERE v.organization_id = actor.organization_id
              AND v.requested_email IS NOT NULL
              AND v.created_at > now() - interval '1 hour';
            IF recent_count >= 3 THEN
                RAISE EXCEPTION 'verification email limit reached' USING ERRCODE = '42901';
            END IF;

            INSERT INTO public.us_lacey_email_verifications(
                organization_id, user_id, token_hash, requested_email, expires_at
            ) VALUES (
                actor.organization_id, actor.user_id, requested_challenge_hash,
                normalized_email, now() + interval '30 minutes'
            );
            RETURN QUERY SELECT actor.organization_id, actor.user_id;
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE FUNCTION public.us_lacey_evaluation_claim_finish(
            requested_session_hash text,
            requested_challenge_hash text
        )
        RETURNS TABLE(
            organization_id integer,
            user_id integer,
            evaluation_status text,
            successful_operations_used integer,
            operation_limit integer,
            inactive_expires_at timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            actor record;
            challenge record;
        BEGIN
            SELECT s.organization_id, s.user_id INTO actor
            FROM public.user_sessions AS s
            JOIN public.users AS usr ON usr.id = s.user_id
                AND usr.organization_id = s.organization_id AND usr.is_active
            JOIN public.organizations AS org ON org.id = s.organization_id
                AND org.is_active
            WHERE s.token_hash = requested_session_hash
              AND s.revoked_at IS NULL AND s.expires_at > now()
            FOR UPDATE OF s;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'verification link is invalid or expired' USING ERRCODE = '42501';
            END IF;

            SELECT v.id, v.requested_email INTO challenge
            FROM public.us_lacey_email_verifications AS v
            WHERE v.token_hash = requested_challenge_hash
              AND v.organization_id = actor.organization_id
              AND v.user_id = actor.user_id
              AND v.requested_email IS NOT NULL
              AND v.used_at IS NULL
              AND v.expires_at > now()
            FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'verification link is invalid or expired' USING ERRCODE = '42501';
            END IF;

            UPDATE public.us_lacey_email_verifications AS v
            SET used_at = now() WHERE v.id = challenge.id;

            RETURN QUERY
            SELECT result.organization_id, result.user_id,
                   result.evaluation_status, result.successful_operations_used,
                   result.operation_limit, result.inactive_expires_at
            FROM public.us_lacey_evaluation_claim(
                requested_session_hash, challenge.requested_email
            ) AS result;
        END;
        $$;
        """
    )
    for signature in (BEGIN_FN, FINISH_FN):
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")
        op.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {RUNTIME_ROLE}")
    _reset_platform_role()


def downgrade() -> None:
    _set_platform_role()
    for signature in (FINISH_FN, BEGIN_FN):
        op.execute(f"DROP FUNCTION IF EXISTS {signature}")
    _reset_platform_role()
    op.drop_index(
        "ix_us_lacey_eval_email_challenge_org_time",
        table_name="us_lacey_email_verifications",
    )
    op.drop_column("us_lacey_email_verifications", "requested_email")
