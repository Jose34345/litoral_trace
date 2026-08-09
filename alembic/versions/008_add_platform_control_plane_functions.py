"""Add persistent platform control-plane functions.

Revision ID: 008_add_platform_control_plane_functions
Revises: 007_add_auth_tenant_rls
Create Date: 2026-08-08 16:30:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "008_add_platform_control_plane_functions"
down_revision: Union[str, Sequence[str], None] = "007_add_auth_tenant_rls"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


RUNTIME_ROLE = "litoral_trace_app"


def _create_platform_helper_functions() -> None:
    op.execute(
        """
        CREATE FUNCTION public._platform_superadmin_session_actor(
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
        "REVOKE ALL ON FUNCTION public._platform_superadmin_session_actor(text) "
        "FROM PUBLIC"
    )


def _create_platform_list_function() -> None:
    op.execute(
        """
        CREATE FUNCTION public.platform_list_organizations(
            actor_refresh_token_hash text
        )
        RETURNS TABLE (
            id integer,
            name text,
            slug text,
            tax_id text,
            tier text,
            is_active boolean,
            admin_user_id integer,
            admin_email text,
            admin_username text,
            license_id integer,
            license_plan_type text,
            license_max_lotes integer,
            license_max_volume_tons double precision,
            license_max_batch_rows integer,
            license_valid_until timestamptz,
            license_is_active boolean,
            created_at timestamptz,
            updated_at timestamptz
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
                organizations.id,
                organizations.name::text,
                organizations.slug::text,
                organizations.tax_id::text,
                organizations.tier::text,
                organizations.is_active,
                admin_user.id,
                admin_user.email::text,
                admin_user.username::text,
                licenses.id,
                licenses.plan_type::text,
                licenses.max_lotes,
                licenses.max_volume_tons,
                licenses.max_batch_rows,
                licenses.valid_until,
                licenses.is_active,
                organizations.created_at,
                organizations.updated_at
            FROM public.organizations AS organizations
            LEFT JOIN LATERAL (
                SELECT users.id, users.email, users.username
                FROM public.users AS users
                WHERE users.organization_id = organizations.id
                ORDER BY
                    CASE WHEN users.role = 'admin' THEN 0 ELSE 1 END,
                    users.is_active DESC,
                    users.id ASC
                LIMIT 1
            ) AS admin_user ON TRUE
            LEFT JOIN public.licenses AS licenses
                ON licenses.organization_id = organizations.id
            ORDER BY organizations.id ASC;
        END;
        $$;
        """
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.platform_list_organizations(text) FROM PUBLIC"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION public.platform_list_organizations(text) "
        f"TO {RUNTIME_ROLE}"
    )


def _create_platform_create_function() -> None:
    op.execute(
        """
        CREATE FUNCTION public.platform_create_organization(
            actor_refresh_token_hash text,
            requested_organization_name text,
            requested_organization_slug text,
            requested_organization_tax_id text,
            requested_organization_tier text,
            requested_organization_description text,
            requested_admin_email text,
            requested_admin_username text,
            requested_admin_password_hash text,
            requested_admin_full_name text,
            requested_license_plan_type text,
            requested_license_max_lotes integer,
            requested_license_max_volume_tons double precision,
            requested_license_max_batch_rows integer,
            requested_license_valid_until timestamptz,
            requested_license_is_active boolean
        )
        RETURNS TABLE (
            organization_id integer,
            organization_name text,
            organization_slug text,
            organization_is_active boolean,
            admin_user_id integer,
            admin_email text,
            admin_username text,
            license_id integer,
            license_plan_type text,
            license_max_lotes integer,
            license_max_volume_tons double precision,
            license_max_batch_rows integer,
            license_valid_until timestamptz,
            license_is_active boolean
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            new_organization_id integer;
            new_admin_user_id integer;
            new_license_id integer;
        BEGIN
            PERFORM 1
            FROM public._platform_superadmin_session_actor(actor_refresh_token_hash);

            IF requested_organization_name IS NULL OR btrim(requested_organization_name) = '' THEN
                RAISE EXCEPTION 'organization_name is required'
                    USING ERRCODE = '22023';
            END IF;
            IF requested_organization_slug IS NULL OR btrim(requested_organization_slug) = '' THEN
                RAISE EXCEPTION 'organization_slug is required'
                    USING ERRCODE = '22023';
            END IF;
            IF requested_organization_tax_id IS NULL OR btrim(requested_organization_tax_id) = '' THEN
                RAISE EXCEPTION 'organization_tax_id is required'
                    USING ERRCODE = '22023';
            END IF;
            IF requested_admin_email IS NULL OR btrim(requested_admin_email) = '' THEN
                RAISE EXCEPTION 'admin_email is required'
                    USING ERRCODE = '22023';
            END IF;
            IF requested_admin_username IS NULL OR btrim(requested_admin_username) = '' THEN
                RAISE EXCEPTION 'admin_username is required'
                    USING ERRCODE = '22023';
            END IF;
            IF requested_admin_password_hash IS NULL OR btrim(requested_admin_password_hash) = '' THEN
                RAISE EXCEPTION 'admin_password_hash is required'
                    USING ERRCODE = '22023';
            END IF;
            IF requested_license_max_lotes <= 0 OR requested_license_max_volume_tons <= 0 OR requested_license_max_batch_rows <= 0 THEN
                RAISE EXCEPTION 'license limits must be positive'
                    USING ERRCODE = '22023';
            END IF;

            INSERT INTO public.organizations (
                name,
                slug,
                tax_id,
                tier,
                description,
                is_active,
                created_at,
                updated_at
            )
            VALUES (
                btrim(requested_organization_name),
                btrim(requested_organization_slug),
                btrim(requested_organization_tax_id),
                lower(btrim(COALESCE(requested_organization_tier, 'pro'))),
                NULLIF(btrim(COALESCE(requested_organization_description, '')), ''),
                TRUE,
                now(),
                now()
            )
            RETURNING id INTO new_organization_id;

            INSERT INTO public.users (
                organization_id,
                email,
                username,
                password_hash,
                role,
                full_name,
                is_active,
                created_at,
                updated_at
            )
            VALUES (
                new_organization_id,
                btrim(requested_admin_email),
                btrim(requested_admin_username),
                requested_admin_password_hash,
                'admin',
                NULLIF(btrim(COALESCE(requested_admin_full_name, '')), ''),
                TRUE,
                now(),
                now()
            )
            RETURNING id INTO new_admin_user_id;

            INSERT INTO public.licenses (
                organization_id,
                plan_type,
                max_lotes,
                max_volume_tons,
                max_batch_rows,
                valid_until,
                is_active,
                created_at,
                updated_at
            )
            VALUES (
                new_organization_id,
                lower(btrim(COALESCE(requested_license_plan_type, requested_organization_tier, 'pro'))),
                requested_license_max_lotes,
                requested_license_max_volume_tons,
                requested_license_max_batch_rows,
                requested_license_valid_until,
                COALESCE(requested_license_is_active, TRUE),
                now(),
                now()
            )
            RETURNING id INTO new_license_id;

            RETURN QUERY
            SELECT
                organizations.id,
                organizations.name::text,
                organizations.slug::text,
                organizations.is_active,
                users.id,
                users.email::text,
                users.username::text,
                licenses.id,
                licenses.plan_type::text,
                licenses.max_lotes,
                licenses.max_volume_tons,
                licenses.max_batch_rows,
                licenses.valid_until,
                licenses.is_active
            FROM public.organizations AS organizations
            JOIN public.users AS users
                ON users.id = new_admin_user_id
            JOIN public.licenses AS licenses
                ON licenses.id = new_license_id
            WHERE organizations.id = new_organization_id;
        END;
        $$;
        """
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.platform_create_organization("
        "text, text, text, text, text, text, text, text, text, text, text, integer, double precision, integer, timestamptz, boolean"
        ") FROM PUBLIC"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION public.platform_create_organization("
        "text, text, text, text, text, text, text, text, text, text, text, integer, double precision, integer, timestamptz, boolean"
        f") TO {RUNTIME_ROLE}"
    )


def _create_platform_toggle_function() -> None:
    op.execute(
        """
        CREATE FUNCTION public.platform_toggle_organization_status(
            actor_refresh_token_hash text,
            target_organization_id integer
        )
        RETURNS TABLE (
            organization_id integer,
            is_active boolean,
            revoked_session_count integer
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            next_state boolean;
            revoked_count integer := 0;
        BEGIN
            PERFORM 1
            FROM public._platform_superadmin_session_actor(actor_refresh_token_hash);

            UPDATE public.organizations
            SET is_active = NOT organizations.is_active,
                updated_at = now()
            WHERE organizations.id = target_organization_id
            RETURNING organizations.is_active INTO next_state;

            IF NOT FOUND THEN
                RAISE EXCEPTION 'organization not found'
                    USING ERRCODE = 'P0002';
            END IF;

            IF next_state = FALSE THEN
                UPDATE public.user_sessions
                SET revoked_at = now(),
                    updated_at = now()
                WHERE public.user_sessions.organization_id = target_organization_id
                  AND revoked_at IS NULL;
                GET DIAGNOSTICS revoked_count = ROW_COUNT;
            END IF;

            RETURN QUERY
            SELECT target_organization_id, next_state, revoked_count;
        END;
        $$;
        """
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.platform_toggle_organization_status(text, integer) "
        "FROM PUBLIC"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION public.platform_toggle_organization_status(text, integer) "
        f"TO {RUNTIME_ROLE}"
    )


def _create_platform_license_function() -> None:
    op.execute(
        """
        CREATE FUNCTION public.platform_upsert_license(
            actor_refresh_token_hash text,
            target_organization_id integer,
            requested_plan_type text,
            requested_max_lotes integer,
            requested_max_volume_tons double precision,
            requested_max_batch_rows integer,
            requested_valid_until timestamptz,
            requested_is_active boolean
        )
        RETURNS TABLE (
            license_id integer,
            result_organization_id integer,
            plan_type text,
            max_lotes integer,
            max_volume_tons double precision,
            max_batch_rows integer,
            valid_until timestamptz,
            is_active boolean
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        BEGIN
            PERFORM 1
            FROM public._platform_superadmin_session_actor(actor_refresh_token_hash);

            IF requested_max_lotes <= 0 OR requested_max_volume_tons <= 0 OR requested_max_batch_rows <= 0 THEN
                RAISE EXCEPTION 'license limits must be positive'
                    USING ERRCODE = '22023';
            END IF;

            IF NOT EXISTS (
                SELECT 1
                FROM public.organizations
                WHERE organizations.id = target_organization_id
            ) THEN
                RAISE EXCEPTION 'organization not found'
                    USING ERRCODE = 'P0002';
            END IF;

            RETURN QUERY
            INSERT INTO public.licenses (
                organization_id,
                plan_type,
                max_lotes,
                max_volume_tons,
                max_batch_rows,
                valid_until,
                is_active,
                created_at,
                updated_at
            )
            VALUES (
                target_organization_id,
                lower(btrim(COALESCE(requested_plan_type, 'pro'))),
                requested_max_lotes,
                requested_max_volume_tons,
                requested_max_batch_rows,
                requested_valid_until,
                COALESCE(requested_is_active, TRUE),
                now(),
                now()
            )
            ON CONFLICT (organization_id)
            DO UPDATE
            SET plan_type = excluded.plan_type,
                max_lotes = excluded.max_lotes,
                max_volume_tons = excluded.max_volume_tons,
                max_batch_rows = excluded.max_batch_rows,
                valid_until = excluded.valid_until,
                is_active = excluded.is_active,
                updated_at = now()
            RETURNING
                licenses.id,
                licenses.organization_id,
                licenses.plan_type::text,
                licenses.max_lotes,
                licenses.max_volume_tons,
                licenses.max_batch_rows,
                licenses.valid_until,
                licenses.is_active;
        END;
        $$;
        """
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.platform_upsert_license("
        "text, integer, text, integer, double precision, integer, timestamptz, boolean"
        ") FROM PUBLIC"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION public.platform_upsert_license("
        "text, integer, text, integer, double precision, integer, timestamptz, boolean"
        f") TO {RUNTIME_ROLE}"
    )


def _drop_platform_functions() -> None:
    op.execute(
        "REVOKE ALL ON FUNCTION public.platform_upsert_license("
        "text, integer, text, integer, double precision, integer, timestamptz, boolean"
        f") FROM {RUNTIME_ROLE}"
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.platform_toggle_organization_status(text, integer) "
        f"FROM {RUNTIME_ROLE}"
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.platform_create_organization("
        "text, text, text, text, text, text, text, text, text, text, text, integer, double precision, integer, timestamptz, boolean"
        f") FROM {RUNTIME_ROLE}"
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.platform_list_organizations(text) "
        f"FROM {RUNTIME_ROLE}"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS public.platform_upsert_license("
        "text, integer, text, integer, double precision, integer, timestamptz, boolean"
        ")"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS public.platform_toggle_organization_status(text, integer)"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS public.platform_create_organization("
        "text, text, text, text, text, text, text, text, text, text, text, integer, double precision, integer, timestamptz, boolean"
        ")"
    )
    op.execute("DROP FUNCTION IF EXISTS public.platform_list_organizations(text)")
    op.execute(
        "DROP FUNCTION IF EXISTS public._platform_superadmin_session_actor(text)"
    )


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS public.ix_licenses_organization_id")
    op.execute(
        "CREATE UNIQUE INDEX ix_licenses_organization_id "
        "ON public.licenses (organization_id)"
    )
    _create_platform_helper_functions()
    _create_platform_list_function()
    _create_platform_create_function()
    _create_platform_toggle_function()
    _create_platform_license_function()


def downgrade() -> None:
    _drop_platform_functions()
    op.execute("DROP INDEX IF EXISTS public.ix_licenses_organization_id")
    op.execute(
        "CREATE INDEX ix_licenses_organization_id "
        "ON public.licenses (organization_id)"
    )
