"""Harden platform control-plane audit atomicity.

Revision ID: 009_harden_platform_control_plane_audit
Revises: 008_add_platform_control_plane_functions
Create Date: 2026-08-08 18:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "009_harden_platform_control_plane_audit"
down_revision: Union[str, Sequence[str], None] = "008_add_platform_control_plane_functions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _create_platform_audit_helper() -> None:
    op.execute(
        """
        CREATE FUNCTION public._platform_insert_audit_log(
            actor_user_id integer,
            actor_username text,
            actor_role text,
            actor_organization_id integer,
            target_organization_id integer,
            audit_action text,
            audit_entity_type text,
            audit_entity_id integer,
            audit_metadata jsonb DEFAULT NULL
        )
        RETURNS void
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            sanitized_metadata jsonb := coalesce(audit_metadata, '{}'::jsonb);
        BEGIN
            sanitized_metadata := sanitized_metadata
                - 'password'
                - 'password_hash'
                - 'refresh_token'
                - 'token'
                - 'access_token'
                - 'token_hash'
                - 'refresh_token_hash'
                - 'authorization'
                - 'cookie'
                - 'set-cookie'
                - 'api_key'
                - 'apikey'
                - 'secret'
                - 'database_url'
                - 'migration_database_url'
                - 'jwt';

            IF actor_organization_id IS DISTINCT FROM target_organization_id THEN
                sanitized_metadata := sanitized_metadata || jsonb_build_object(
                    'actor_organization_id',
                    actor_organization_id
                );
            END IF;

            INSERT INTO public.audit_logs (
                organization_id,
                user_id,
                username,
                action,
                entity_type,
                entity_id,
                before_data,
                after_data,
                detail,
                ip_address
            )
            VALUES (
                target_organization_id,
                actor_user_id,
                NULLIF(btrim(coalesce(actor_username, '')), ''),
                audit_action,
                audit_entity_type,
                audit_entity_id,
                NULL,
                jsonb_strip_nulls(
                    jsonb_build_object(
                        'outcome',
                        'success',
                        'actor_role',
                        NULLIF(btrim(coalesce(actor_role, '')), ''),
                        'metadata',
                        CASE
                            WHEN sanitized_metadata = '{}'::jsonb THEN NULL
                            ELSE sanitized_metadata
                        END
                    )
                )::json,
                NULL,
                NULL
            );
        END;
        $$;
        """
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public._platform_insert_audit_log("
        "integer, text, text, integer, integer, text, text, integer, jsonb"
        ") FROM PUBLIC"
    )


def _create_platform_create_function_with_atomic_audit() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.platform_create_organization(
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
            actor_user_id integer;
            actor_organization_id integer;
            actor_username text;
            actor_role text;
            new_organization_id integer;
            new_admin_user_id integer;
            new_license_id integer;
        BEGIN
            SELECT
                identity.actor_user_id,
                identity.actor_organization_id,
                users.username,
                users.role
            INTO
                actor_user_id,
                actor_organization_id,
                actor_username,
                actor_role
            FROM public._platform_superadmin_session_actor(actor_refresh_token_hash) AS identity
            JOIN public.users AS users
                ON users.id = identity.actor_user_id;

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
                lower(btrim(coalesce(requested_organization_tier, 'pro'))),
                NULLIF(btrim(coalesce(requested_organization_description, '')), ''),
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
                NULLIF(btrim(coalesce(requested_admin_full_name, '')), ''),
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
                lower(btrim(coalesce(requested_license_plan_type, requested_organization_tier, 'pro'))),
                requested_license_max_lotes,
                requested_license_max_volume_tons,
                requested_license_max_batch_rows,
                requested_license_valid_until,
                coalesce(requested_license_is_active, TRUE),
                now(),
                now()
            )
            RETURNING id INTO new_license_id;

            PERFORM public._platform_insert_audit_log(
                actor_user_id,
                actor_username,
                actor_role,
                actor_organization_id,
                new_organization_id,
                'platform.organization.create',
                'organization',
                new_organization_id,
                jsonb_build_object(
                    'target_organization_id',
                    new_organization_id,
                    'organization_name',
                    btrim(requested_organization_name),
                    'tax_id',
                    btrim(requested_organization_tax_id),
                    'tier',
                    lower(btrim(coalesce(requested_organization_tier, 'pro')))
                )
            );

            PERFORM public._platform_insert_audit_log(
                actor_user_id,
                actor_username,
                actor_role,
                actor_organization_id,
                new_organization_id,
                'platform.organization_admin.create',
                'user',
                new_admin_user_id,
                jsonb_build_object(
                    'target_organization_id',
                    new_organization_id,
                    'admin_username',
                    btrim(requested_admin_username)
                )
            );

            PERFORM public._platform_insert_audit_log(
                actor_user_id,
                actor_username,
                actor_role,
                actor_organization_id,
                new_organization_id,
                'platform.license.create',
                'license',
                new_license_id,
                jsonb_build_object(
                    'target_organization_id',
                    new_organization_id,
                    'plan_type',
                    lower(btrim(coalesce(requested_license_plan_type, requested_organization_tier, 'pro')))
                )
            );

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


def _create_platform_toggle_function_with_atomic_audit() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.platform_toggle_organization_status(
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
            actor_user_id integer;
            actor_organization_id integer;
            actor_username text;
            actor_role text;
            next_state boolean;
            revoked_count integer := 0;
        BEGIN
            SELECT
                identity.actor_user_id,
                identity.actor_organization_id,
                users.username,
                users.role
            INTO
                actor_user_id,
                actor_organization_id,
                actor_username,
                actor_role
            FROM public._platform_superadmin_session_actor(actor_refresh_token_hash) AS identity
            JOIN public.users AS users
                ON users.id = identity.actor_user_id;

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

            PERFORM public._platform_insert_audit_log(
                actor_user_id,
                actor_username,
                actor_role,
                actor_organization_id,
                target_organization_id,
                'platform.organization.status_change',
                'organization',
                target_organization_id,
                jsonb_build_object(
                    'target_organization_id',
                    target_organization_id,
                    'is_active',
                    next_state,
                    'revoked_session_count',
                    revoked_count
                )
            );

            RETURN QUERY
            SELECT target_organization_id, next_state, revoked_count;
        END;
        $$;
        """
    )


def _create_platform_license_function_with_atomic_audit() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.platform_upsert_license(
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
        DECLARE
            actor_user_id integer;
            actor_organization_id integer;
            actor_username text;
            actor_role text;
            license_already_exists boolean := FALSE;
            persisted_license_id integer;
            persisted_plan_type text;
            persisted_max_lotes integer;
            persisted_max_volume_tons double precision;
            persisted_max_batch_rows integer;
            persisted_valid_until timestamptz;
            persisted_is_active boolean;
            audit_action text;
        BEGIN
            SELECT
                identity.actor_user_id,
                identity.actor_organization_id,
                users.username,
                users.role
            INTO
                actor_user_id,
                actor_organization_id,
                actor_username,
                actor_role
            FROM public._platform_superadmin_session_actor(actor_refresh_token_hash) AS identity
            JOIN public.users AS users
                ON users.id = identity.actor_user_id;

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

            SELECT EXISTS (
                SELECT 1
                FROM public.licenses
                WHERE public.licenses.organization_id = target_organization_id
            )
            INTO license_already_exists;

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
                lower(btrim(coalesce(requested_plan_type, 'pro'))),
                requested_max_lotes,
                requested_max_volume_tons,
                requested_max_batch_rows,
                requested_valid_until,
                coalesce(requested_is_active, TRUE),
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
                licenses.plan_type::text,
                licenses.max_lotes,
                licenses.max_volume_tons,
                licenses.max_batch_rows,
                licenses.valid_until,
                licenses.is_active
            INTO
                persisted_license_id,
                persisted_plan_type,
                persisted_max_lotes,
                persisted_max_volume_tons,
                persisted_max_batch_rows,
                persisted_valid_until,
                persisted_is_active;

            audit_action := CASE
                WHEN license_already_exists THEN 'platform.license.update'
                ELSE 'platform.license.create'
            END;

            PERFORM public._platform_insert_audit_log(
                actor_user_id,
                actor_username,
                actor_role,
                actor_organization_id,
                target_organization_id,
                audit_action,
                'license',
                persisted_license_id,
                jsonb_build_object(
                    'target_organization_id',
                    target_organization_id,
                    'plan_type',
                    persisted_plan_type
                )
            );

            RETURN QUERY
            SELECT
                persisted_license_id,
                target_organization_id,
                persisted_plan_type,
                persisted_max_lotes,
                persisted_max_volume_tons,
                persisted_max_batch_rows,
                persisted_valid_until,
                persisted_is_active;
        END;
        $$;
        """
    )


def _restore_platform_create_function_without_atomic_audit() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.platform_create_organization(
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
                lower(btrim(coalesce(requested_organization_tier, 'pro'))),
                NULLIF(btrim(coalesce(requested_organization_description, '')), ''),
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
                NULLIF(btrim(coalesce(requested_admin_full_name, '')), ''),
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
                lower(btrim(coalesce(requested_license_plan_type, requested_organization_tier, 'pro'))),
                requested_license_max_lotes,
                requested_license_max_volume_tons,
                requested_license_max_batch_rows,
                requested_license_valid_until,
                coalesce(requested_license_is_active, TRUE),
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


def _restore_platform_toggle_function_without_atomic_audit() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.platform_toggle_organization_status(
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


def _restore_platform_license_function_without_atomic_audit() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.platform_upsert_license(
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
                lower(btrim(coalesce(requested_plan_type, 'pro'))),
                requested_max_lotes,
                requested_max_volume_tons,
                requested_max_batch_rows,
                requested_valid_until,
                coalesce(requested_is_active, TRUE),
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


def upgrade() -> None:
    _create_platform_audit_helper()
    _create_platform_create_function_with_atomic_audit()
    _create_platform_toggle_function_with_atomic_audit()
    _create_platform_license_function_with_atomic_audit()


def downgrade() -> None:
    _restore_platform_create_function_without_atomic_audit()
    _restore_platform_toggle_function_without_atomic_audit()
    _restore_platform_license_function_without_atomic_audit()
    op.execute(
        "DROP FUNCTION IF EXISTS public._platform_insert_audit_log("
        "integer, text, text, integer, integer, text, text, integer, jsonb"
        ")"
    )
