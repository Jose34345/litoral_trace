"""Add hardened U.S. portal authentication primitives behind FORCE RLS.

Revision ID: 037_us_lacey_portal_auth
Revises: 036_fix_us_lacey_status_ambiguity
Create Date: 2026-08-30 16:18:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "037_us_lacey_portal_auth"
down_revision: Union[str, Sequence[str], None] = "036_fix_us_lacey_status_ambiguity"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"

FUNCTIONS = (
    "public.us_lacey_portal_login_lookup(text)",
    "public.us_lacey_portal_create_session(integer,integer,text,text,timestamptz,text,text)",
    "public.us_lacey_portal_session_lookup(text)",
    "public.us_lacey_portal_revoke_session(text)",
)


def _grant_temp_platform_set() -> None:
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )


def _revoke_temp_platform_set() -> None:
    op.execute(f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER")


def _grant_session_insert_capability() -> None:
    op.execute(
        "DROP POLICY IF EXISTS user_sessions_platform_insert "
        "ON public.user_sessions"
    )
    op.execute(
        "CREATE POLICY user_sessions_platform_insert "
        "ON public.user_sessions FOR INSERT "
        f"TO {PLATFORM_ROLE} WITH CHECK (true)"
    )
    op.execute(
        f"GRANT INSERT ON TABLE public.user_sessions TO {PLATFORM_ROLE}"
    )
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE public.user_sessions_id_seq TO {PLATFORM_ROLE}"
    )


def _create_functions() -> None:
    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_portal_login_lookup(requested_email text)
        RETURNS TABLE (
            user_id integer,
            organization_id integer,
            password_hash text,
            user_is_active boolean,
            organization_is_active boolean,
            account_status text
        )
        LANGUAGE sql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
            SELECT
                users.id,
                users.organization_id,
                users.password_hash,
                users.is_active,
                organizations.is_active,
                profiles.account_status
            FROM public.users AS users
            JOIN public.organizations AS organizations
              ON organizations.id = users.organization_id
            JOIN public.us_lacey_organization_profiles AS profiles
              ON profiles.organization_id = users.organization_id
            WHERE users.username = lower(btrim(requested_email))
              AND users.email = lower(btrim(requested_email))
            LIMIT 1
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_portal_create_session(
            requested_user_id integer,
            requested_organization_id integer,
            requested_token_hash text,
            requested_family_id text,
            requested_expires_at timestamptz,
            requested_ip text,
            requested_user_agent text
        )
        RETURNS TABLE (session_id integer)
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            new_session_id integer;
        BEGIN
            IF requested_user_id IS NULL OR requested_user_id <= 0
               OR requested_organization_id IS NULL OR requested_organization_id <= 0 THEN
                RAISE EXCEPTION 'invalid portal identity' USING ERRCODE = '22023';
            END IF;
            IF requested_token_hash IS NULL
               OR requested_token_hash !~ '^[0-9a-f]{64}$' THEN
                RAISE EXCEPTION 'invalid portal session token' USING ERRCODE = '22023';
            END IF;
            IF btrim(coalesce(requested_family_id, '')) = ''
               OR char_length(requested_family_id) > 36 THEN
                RAISE EXCEPTION 'invalid portal session family' USING ERRCODE = '22023';
            END IF;
            IF requested_expires_at IS NULL
               OR requested_expires_at <= now()
               OR requested_expires_at > now() + interval '31 days' THEN
                RAISE EXCEPTION 'invalid portal session expiry' USING ERRCODE = '22023';
            END IF;

            IF NOT EXISTS (
                SELECT 1
                FROM public.users AS users
                JOIN public.organizations AS organizations
                  ON organizations.id = users.organization_id
                JOIN public.us_lacey_organization_profiles AS profiles
                  ON profiles.organization_id = users.organization_id
                WHERE users.id = requested_user_id
                  AND users.organization_id = requested_organization_id
                  AND users.is_active
                  AND organizations.is_active
                  AND profiles.account_status IN ('PAYMENT_PENDING','PILOT','ACTIVE')
            ) THEN
                RAISE EXCEPTION 'portal account unavailable' USING ERRCODE = '28000';
            END IF;

            INSERT INTO public.user_sessions (
                user_id,
                organization_id,
                family_id,
                token_hash,
                issued_at,
                expires_at,
                created_ip,
                user_agent
            ) VALUES (
                requested_user_id,
                requested_organization_id,
                requested_family_id,
                requested_token_hash,
                now(),
                requested_expires_at,
                NULLIF(left(btrim(coalesce(requested_ip, '')), 45), ''),
                NULLIF(left(btrim(coalesce(requested_user_agent, '')), 512), '')
            )
            RETURNING id INTO new_session_id;

            RETURN QUERY SELECT new_session_id;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_portal_session_lookup(requested_token_hash text)
        RETURNS TABLE (
            session_id integer,
            user_id integer,
            organization_id integer,
            email text,
            full_name text,
            legal_name text,
            business_type text,
            account_status text,
            expires_at timestamptz
        )
        LANGUAGE sql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
            SELECT
                sessions.id,
                users.id,
                users.organization_id,
                users.email,
                coalesce(users.full_name, users.email),
                profiles.legal_name,
                profiles.business_type,
                profiles.account_status,
                sessions.expires_at
            FROM public.user_sessions AS sessions
            JOIN public.users AS users
              ON users.id = sessions.user_id
             AND users.organization_id = sessions.organization_id
            JOIN public.organizations AS organizations
              ON organizations.id = users.organization_id
            JOIN public.us_lacey_organization_profiles AS profiles
              ON profiles.organization_id = users.organization_id
            WHERE sessions.token_hash = requested_token_hash
              AND sessions.revoked_at IS NULL
              AND sessions.expires_at > now()
              AND users.is_active
              AND organizations.is_active
              AND profiles.account_status IN ('PAYMENT_PENDING','PILOT','ACTIVE')
            LIMIT 1
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_portal_revoke_session(requested_token_hash text)
        RETURNS boolean
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            changed integer;
        BEGIN
            UPDATE public.user_sessions AS sessions
            SET revoked_at = coalesce(sessions.revoked_at, now())
            WHERE sessions.token_hash = requested_token_hash
              AND sessions.revoked_at IS NULL;
            GET DIAGNOSTICS changed = ROW_COUNT;
            RETURN changed > 0;
        END;
        $$;
        """
    )

    for signature in FUNCTIONS:
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")
        op.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {RUNTIME_ROLE}")

    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()


def upgrade() -> None:
    _grant_session_insert_capability()
    _create_functions()


def downgrade() -> None:
    _grant_temp_platform_set()
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")
    for signature in reversed(FUNCTIONS):
        op.execute(f"DROP FUNCTION IF EXISTS {signature}")
    op.execute("RESET ROLE")
    _revoke_temp_platform_set()

    op.execute(
        "DROP POLICY IF EXISTS user_sessions_platform_insert "
        "ON public.user_sessions"
    )
    op.execute(
        f"REVOKE INSERT ON TABLE public.user_sessions FROM {PLATFORM_ROLE}"
    )
    op.execute(
        f"REVOKE USAGE, SELECT ON SEQUENCE public.user_sessions_id_seq FROM {PLATFORM_ROLE}"
    )
