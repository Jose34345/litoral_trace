"""Bootstrap platform control-plane tenant context under forced auth RLS.

Revision ID: 027_fix_platform_control_plane_rls_bootstrap
Revises: 026_add_eudr_acceptance_attempts
Create Date: 2026-08-23 18:20:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "027_fix_platform_control_plane_rls_bootstrap"
down_revision: Union[str, Sequence[str], None] = "026_add_eudr_acceptance_attempts"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


PLATFORM_ACTOR_FUNCTION = "public._platform_superadmin_session_actor(text)"


def _create_platform_actor_with_rls_bootstrap() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public._platform_superadmin_session_actor(
            actor_refresh_token_hash text
        )
        RETURNS TABLE (
            actor_user_id integer,
            actor_session_id integer,
            actor_organization_id integer
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            bootstrap_organization_id integer;
        BEGIN
            IF actor_refresh_token_hash IS NULL OR btrim(actor_refresh_token_hash) = '' THEN
                RAISE EXCEPTION 'invalid platform session'
                    USING ERRCODE = '28000';
            END IF;

            /*
             * users/user_sessions are FORCE RLS tables. Resolve only the tenant
             * identifier through the existing SECURITY DEFINER auth bootstrap,
             * then bind this transaction to that tenant before reading auth rows.
             * The normal platform checks below remain authoritative.
             */
            SELECT bootstrap.organization_id
            INTO bootstrap_organization_id
            FROM public.bootstrap_auth_session_by_token_hash(
                actor_refresh_token_hash
            ) AS bootstrap
            LIMIT 1;

            IF bootstrap_organization_id IS NULL THEN
                RAISE EXCEPTION 'invalid platform session'
                    USING ERRCODE = '28000';
            END IF;

            PERFORM set_config(
                'app.current_organization_id',
                bootstrap_organization_id::text,
                true
            );

            RETURN QUERY
            SELECT
                users.id,
                user_sessions.id,
                users.organization_id
            FROM public.user_sessions AS user_sessions
            JOIN public.users AS users
                ON users.id = user_sessions.user_id
               AND users.organization_id = user_sessions.organization_id
            JOIN public.organizations AS organizations
                ON organizations.id = users.organization_id
            WHERE user_sessions.token_hash = actor_refresh_token_hash
              AND user_sessions.organization_id = bootstrap_organization_id
              AND user_sessions.revoked_at IS NULL
              AND user_sessions.expires_at > now()
              AND users.is_active
              AND organizations.is_active
              AND users.role = 'superadmin';

            IF NOT FOUND THEN
                IF EXISTS (
                    SELECT 1
                    FROM public.user_sessions AS user_sessions
                    JOIN public.users AS users
                        ON users.id = user_sessions.user_id
                       AND users.organization_id = user_sessions.organization_id
                    JOIN public.organizations AS organizations
                        ON organizations.id = users.organization_id
                    WHERE user_sessions.token_hash = actor_refresh_token_hash
                      AND user_sessions.organization_id = bootstrap_organization_id
                      AND user_sessions.revoked_at IS NULL
                      AND user_sessions.expires_at > now()
                      AND users.is_active
                      AND organizations.is_active
                ) THEN
                    RAISE EXCEPTION 'platform permission denied'
                        USING ERRCODE = '42501';
                END IF;

                RAISE EXCEPTION 'invalid platform session'
                    USING ERRCODE = '28000';
            END IF;
        END;
        $$;
        """
    )
    op.execute(
        f"REVOKE ALL ON FUNCTION {PLATFORM_ACTOR_FUNCTION} FROM PUBLIC"
    )


def _restore_platform_actor_without_bootstrap() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public._platform_superadmin_session_actor(
            actor_refresh_token_hash text
        )
        RETURNS TABLE (
            actor_user_id integer,
            actor_session_id integer,
            actor_organization_id integer
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        BEGIN
            IF actor_refresh_token_hash IS NULL OR btrim(actor_refresh_token_hash) = '' THEN
                RAISE EXCEPTION 'invalid platform session'
                    USING ERRCODE = '28000';
            END IF;

            RETURN QUERY
            SELECT
                users.id,
                user_sessions.id,
                users.organization_id
            FROM public.user_sessions AS user_sessions
            JOIN public.users AS users
                ON users.id = user_sessions.user_id
               AND users.organization_id = user_sessions.organization_id
            JOIN public.organizations AS organizations
                ON organizations.id = users.organization_id
            WHERE user_sessions.token_hash = actor_refresh_token_hash
              AND user_sessions.revoked_at IS NULL
              AND user_sessions.expires_at > now()
              AND users.is_active
              AND organizations.is_active
              AND users.role = 'superadmin';

            IF NOT FOUND THEN
                IF EXISTS (
                    SELECT 1
                    FROM public.user_sessions AS user_sessions
                    JOIN public.users AS users
                        ON users.id = user_sessions.user_id
                       AND users.organization_id = user_sessions.organization_id
                    JOIN public.organizations AS organizations
                        ON organizations.id = users.organization_id
                    WHERE user_sessions.token_hash = actor_refresh_token_hash
                      AND user_sessions.revoked_at IS NULL
                      AND user_sessions.expires_at > now()
                      AND users.is_active
                      AND organizations.is_active
                ) THEN
                    RAISE EXCEPTION 'platform permission denied'
                        USING ERRCODE = '42501';
                END IF;

                RAISE EXCEPTION 'invalid platform session'
                    USING ERRCODE = '28000';
            END IF;
        END;
        $$;
        """
    )
    op.execute(
        f"REVOKE ALL ON FUNCTION {PLATFORM_ACTOR_FUNCTION} FROM PUBLIC"
    )


def upgrade() -> None:
    _create_platform_actor_with_rls_bootstrap()


def downgrade() -> None:
    _restore_platform_actor_without_bootstrap()
