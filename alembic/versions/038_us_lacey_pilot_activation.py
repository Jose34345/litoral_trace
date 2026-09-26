"""Add audited, non-payment U.S. Lacey PILOT activation.

Revision ID: 038_us_lacey_pilot_activation
Revises: 037_us_lacey_portal_auth
"""
from __future__ import annotations

from alembic import op


revision = "038_us_lacey_pilot_activation"
down_revision = "037_us_lacey_portal_auth"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
FUNCTION = "public.us_lacey_activate_pilot(text,integer,text)"


def _grant_platform_role() -> None:
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )


def _revoke_platform_role() -> None:
    op.execute(f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER")


def upgrade() -> None:
    _grant_platform_role()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")
    op.execute(
        """
        CREATE FUNCTION public.us_lacey_activate_pilot(
            actor_refresh_token_hash text,
            target_organization_id integer,
            requested_reason text
        )
        RETURNS TABLE (
            organization_id integer,
            previous_account_status text,
            account_status text,
            idempotent boolean
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            actor_user_id integer;
            actor_organization_id integer;
            actor_username text;
            actor_role text;
            profile_id integer;
            prior_status text;
            normalized_reason text;
            was_idempotent boolean := false;
        BEGIN
            SELECT identity.actor_user_id, identity.actor_organization_id,
                   users.username, users.role
            INTO actor_user_id, actor_organization_id, actor_username, actor_role
            FROM public._platform_superadmin_session_actor(actor_refresh_token_hash) AS identity
            JOIN public.users AS users ON users.id = identity.actor_user_id;

            normalized_reason := regexp_replace(btrim(coalesce(requested_reason, '')), '\\s+', ' ', 'g');
            IF normalized_reason = '' OR char_length(normalized_reason) > 500
               OR lower(normalized_reason) ~ '(password|secret|token|api[ _-]?key)' THEN
                RAISE EXCEPTION 'pilot activation reason is invalid' USING ERRCODE = '22023';
            END IF;

            SELECT id, account_status INTO profile_id, prior_status
            FROM public.us_lacey_organization_profiles
            WHERE organization_id = target_organization_id
            FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'organization not found' USING ERRCODE = 'P0002';
            END IF;
            IF prior_status = 'PILOT' THEN
                was_idempotent := true;
            ELSIF prior_status = 'PAYMENT_PENDING' THEN
                UPDATE public.us_lacey_organization_profiles
                SET account_status = 'PILOT', updated_at = now()
                WHERE id = profile_id;
            ELSE
                RAISE EXCEPTION 'pilot activation is not allowed for the current account status'
                    USING ERRCODE = '22023';
            END IF;

            PERFORM public._platform_insert_audit_log(
                actor_user_id, actor_username, actor_role, actor_organization_id,
                target_organization_id, 'US_LACEY_PILOT_ACTIVATED',
                'us_lacey_organization_profile', profile_id,
                jsonb_build_object(
                    'previous_status', prior_status, 'new_status', 'PILOT',
                    'reason', normalized_reason, 'idempotent', was_idempotent
                )
            );
            RETURN QUERY SELECT target_organization_id, prior_status, 'PILOT'::text, was_idempotent;
        END;
        $$;
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {FUNCTION} TO {RUNTIME_ROLE}")
    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_platform_role()


def downgrade() -> None:
    _grant_platform_role()
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")
    op.execute(f"DROP FUNCTION IF EXISTS {FUNCTION}")
    op.execute("RESET ROLE")
    _revoke_platform_role()
