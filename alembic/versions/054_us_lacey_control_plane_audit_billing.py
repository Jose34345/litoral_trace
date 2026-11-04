"""Add immutable U.S. Lacey admin audit trail and billing sync metadata.

Revision ID: 054_us_lacey_control_plane_audit_billing
Revises: 053_sandbox_purge_queue
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "054_us_lacey_control_plane_audit_billing"
down_revision: Union[str, Sequence[str], None] = "053_sandbox_purge_queue"
branch_labels = None
depends_on = None


RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"

AUDIT_FUNCTION = (
    "public._us_lacey_admin_audit("
    "integer,integer,text,integer,integer,jsonb,jsonb,uuid,jsonb"
    ")"
)

PROMOTE_FUNCTION = "public.platform_admin_promote_existing_user(text,text)"
STATUS_FUNCTION = (
    "public.platform_admin_set_us_lacey_account_status(text,integer,text)"
)
LIMIT_FUNCTION = (
    "public.platform_admin_set_us_lacey_operation_limit(text,integer,integer)"
)
REVOKE_FUNCTION = "public.platform_admin_revoke_user_sessions(text,integer)"
RESET_FUNCTION = "public.platform_admin_reset_pilot_account(text,integer)"

MUTATION_FUNCTIONS = (
    PROMOTE_FUNCTION,
    STATUS_FUNCTION,
    LIMIT_FUNCTION,
    REVOKE_FUNCTION,
    RESET_FUNCTION,
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


def _enter_platform_role() -> None:
    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET ROLE {PLATFORM_ROLE}")


def _leave_platform_role() -> None:
    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()


def _create_admin_audit_helper() -> None:
    op.execute(
        """
        CREATE FUNCTION public._us_lacey_admin_audit(
            requested_admin_user_id integer,
            requested_admin_organization_id integer,
            requested_action_type text,
            requested_target_organization_id integer,
            requested_target_user_id integer,
            requested_previous_state jsonb,
            requested_new_state jsonb,
            requested_request_id uuid,
            requested_metadata jsonb
        )
        RETURNS void
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            normalized_action text;
            safe_previous jsonb;
            safe_new jsonb;
            safe_metadata jsonb;
        BEGIN
            IF requested_admin_user_id IS NULL
               OR requested_admin_user_id <= 0 THEN
                RAISE EXCEPTION 'admin user id is required'
                    USING ERRCODE = '22023';
            END IF;

            IF requested_admin_organization_id IS NULL
               OR requested_admin_organization_id <= 0 THEN
                RAISE EXCEPTION 'admin organization id is required'
                    USING ERRCODE = '22023';
            END IF;

            normalized_action :=
                upper(btrim(coalesce(requested_action_type, '')));

            IF normalized_action = ''
               OR normalized_action !~ '^[A-Z0-9_]{3,64}$' THEN
                RAISE EXCEPTION 'invalid admin audit action'
                    USING ERRCODE = '22023';
            END IF;

            safe_previous := coalesce(requested_previous_state, '{}'::jsonb);
            safe_new := coalesce(requested_new_state, '{}'::jsonb);
            safe_metadata := coalesce(requested_metadata, '{}'::jsonb);

            IF jsonb_typeof(safe_previous) <> 'object'
               OR jsonb_typeof(safe_new) <> 'object'
               OR jsonb_typeof(safe_metadata) <> 'object' THEN
                RAISE EXCEPTION 'admin audit payloads must be JSON objects'
                    USING ERRCODE = '22023';
            END IF;

            safe_metadata := safe_metadata
                - 'password'
                - 'password_hash'
                - 'refresh_token'
                - 'access_token'
                - 'token'
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

            INSERT INTO public.us_lacey_admin_audit_logs (
                admin_user_id,
                admin_organization_id,
                action_type,
                target_organization_id,
                target_user_id,
                previous_state,
                new_state,
                request_id,
                metadata,
                timestamp
            )
            VALUES (
                requested_admin_user_id,
                requested_admin_organization_id,
                normalized_action,
                requested_target_organization_id,
                requested_target_user_id,
                safe_previous,
                safe_new,
                requested_request_id,
                safe_metadata,
                now()
            );
        END;
        $$;
        """
    )


def _replace_platform_mutations_with_dual_audit() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.platform_admin_promote_existing_user(
            actor_refresh_token_hash text,
            target_email text
        )
        RETURNS TABLE(
            user_id integer,
            organization_id integer,
            email text,
            role text,
            revoked_session_count integer
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            actor record;
            target record;
            matches integer;
            revoked integer;
        BEGIN
            SELECT *
            INTO actor
            FROM public._platform_superadmin_session_actor(
                actor_refresh_token_hash
            );

            SELECT count(*)
            INTO matches
            FROM public.users AS u
            WHERE lower(btrim(u.email)) = lower(btrim(target_email));

            IF matches <> 1 THEN
                RAISE EXCEPTION
                    'founder identity must match exactly one existing user'
                    USING ERRCODE = '22023';
            END IF;

            SELECT
                u.id,
                u.organization_id,
                u.email,
                u.role,
                u.is_active
            INTO target
            FROM public.users AS u
            WHERE lower(btrim(u.email)) = lower(btrim(target_email))
            FOR UPDATE;

            UPDATE public.users AS u
            SET role = 'superadmin',
                is_active = true
            WHERE u.id = target.id
              AND u.organization_id = target.organization_id;

            UPDATE public.user_sessions AS s
            SET revoked_at = coalesce(s.revoked_at, now())
            WHERE s.user_id = target.id
              AND s.organization_id = target.organization_id
              AND s.revoked_at IS NULL
              AND (
                    target.id <> actor.actor_user_id
                    OR s.id <> actor.actor_session_id
              );

            GET DIAGNOSTICS revoked = ROW_COUNT;

            PERFORM public._us_lacey_admin_audit(
                actor.actor_user_id,
                actor.actor_organization_id,
                'PROMOTE_USER',
                target.organization_id,
                target.id,
                jsonb_build_object(
                    'role', target.role,
                    'is_active', target.is_active
                ),
                jsonb_build_object(
                    'role', 'superadmin',
                    'is_active', true
                ),
                NULL,
                jsonb_build_object(
                    'email', target.email,
                    'revoked_session_count', revoked
                )
            );

            PERFORM public._platform_insert_audit_log(
                actor.actor_user_id,
                NULL,
                'superadmin',
                actor.actor_organization_id,
                target.organization_id,
                'FOUNDER_PROMOTED',
                'user',
                target.id,
                jsonb_build_object(
                    'email', target.email,
                    'role', 'superadmin',
                    'revoked_session_count', revoked
                )
            );

            RETURN QUERY
            SELECT
                target.id,
                target.organization_id,
                target.email::text,
                'superadmin'::text,
                revoked;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.platform_admin_set_us_lacey_account_status(
            actor_refresh_token_hash text,
            target_organization_id integer,
            requested_status text
        )
        RETURNS TABLE(
            organization_id integer,
            account_status text
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            actor record;
            previous text;
            normalized text;
            new_action text;
        BEGIN
            SELECT *
            INTO actor
            FROM public._platform_superadmin_session_actor(
                actor_refresh_token_hash
            );

            normalized := upper(btrim(coalesce(requested_status, '')));

            IF normalized NOT IN ('PILOT', 'ACTIVE', 'SUSPENDED') THEN
                RAISE EXCEPTION 'unsupported account status'
                    USING ERRCODE = '22023';
            END IF;

            SELECT profile.account_status
            INTO previous
            FROM public.us_lacey_organization_profiles AS profile
            WHERE profile.organization_id = target_organization_id
            FOR UPDATE;

            IF previous IS NULL THEN
                RAISE EXCEPTION 'U.S. Lacey account not found'
                    USING ERRCODE = '22023';
            END IF;

            UPDATE public.us_lacey_organization_profiles AS profile
            SET account_status = normalized,
                updated_at = now()
            WHERE profile.organization_id = target_organization_id;

            new_action := CASE
                WHEN normalized = 'SUSPENDED' THEN 'SUSPEND_ACCOUNT'
                ELSE 'SET_ACCOUNT_STATUS'
            END;

            PERFORM public._us_lacey_admin_audit(
                actor.actor_user_id,
                actor.actor_organization_id,
                new_action,
                target_organization_id,
                NULL,
                jsonb_build_object('account_status', previous),
                jsonb_build_object('account_status', normalized),
                NULL,
                '{}'::jsonb
            );

            PERFORM public._platform_insert_audit_log(
                actor.actor_user_id,
                NULL,
                'superadmin',
                actor.actor_organization_id,
                target_organization_id,
                'ACCOUNT_STATUS_CHANGED',
                'us_lacey_account',
                target_organization_id,
                jsonb_build_object(
                    'before', previous,
                    'after', normalized
                )
            );

            RETURN QUERY
            SELECT target_organization_id, normalized;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.platform_admin_set_us_lacey_operation_limit(
            actor_refresh_token_hash text,
            target_organization_id integer,
            requested_limit integer
        )
        RETURNS TABLE(
            organization_id integer,
            monthly_operation_limit integer
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            actor record;
            previous integer;
        BEGIN
            SELECT *
            INTO actor
            FROM public._platform_superadmin_session_actor(
                actor_refresh_token_hash
            );

            IF requested_limit IS NULL
               OR requested_limit < 1
               OR requested_limit > 100000 THEN
                RAISE EXCEPTION
                    'operation limit is outside safe bounds'
                    USING ERRCODE = '22023';
            END IF;

            SELECT subscription.monthly_operation_limit
            INTO previous
            FROM public.us_lacey_subscriptions AS subscription
            WHERE subscription.organization_id = target_organization_id
            FOR UPDATE;

            IF previous IS NULL THEN
                RAISE EXCEPTION
                    'U.S. Lacey subscription not found'
                    USING ERRCODE = '22023';
            END IF;

            UPDATE public.us_lacey_subscriptions AS subscription
            SET monthly_operation_limit = requested_limit,
                updated_at = now()
            WHERE subscription.organization_id = target_organization_id;

            PERFORM public._us_lacey_admin_audit(
                actor.actor_user_id,
                actor.actor_organization_id,
                'SET_LIMIT',
                target_organization_id,
                NULL,
                jsonb_build_object(
                    'monthly_operation_limit', previous
                ),
                jsonb_build_object(
                    'monthly_operation_limit', requested_limit
                ),
                NULL,
                '{}'::jsonb
            );

            PERFORM public._platform_insert_audit_log(
                actor.actor_user_id,
                NULL,
                'superadmin',
                actor.actor_organization_id,
                target_organization_id,
                'OPERATION_LIMIT_CHANGED',
                'us_lacey_subscription',
                target_organization_id,
                jsonb_build_object(
                    'before', previous,
                    'after', requested_limit
                )
            );

            RETURN QUERY
            SELECT target_organization_id, requested_limit;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.platform_admin_revoke_user_sessions(
            actor_refresh_token_hash text,
            target_user_id integer
        )
        RETURNS TABLE(
            user_id integer,
            organization_id integer,
            revoked_session_count integer
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            actor record;
            target_org integer;
            active_before integer;
            revoked integer;
        BEGIN
            SELECT *
            INTO actor
            FROM public._platform_superadmin_session_actor(
                actor_refresh_token_hash
            );

            SELECT u.organization_id
            INTO target_org
            FROM public.users AS u
            WHERE u.id = target_user_id;

            IF target_org IS NULL THEN
                RAISE EXCEPTION 'user not found'
                    USING ERRCODE = '22023';
            END IF;

            SELECT count(*)::integer
            INTO active_before
            FROM public.user_sessions AS s
            WHERE s.user_id = target_user_id
              AND s.revoked_at IS NULL;

            UPDATE public.user_sessions AS s
            SET revoked_at = now(),
                updated_at = now()
            WHERE s.user_id = target_user_id
              AND s.revoked_at IS NULL;

            GET DIAGNOSTICS revoked = ROW_COUNT;

            PERFORM public._us_lacey_admin_audit(
                actor.actor_user_id,
                actor.actor_organization_id,
                'REVOKE_SESSIONS',
                target_org,
                target_user_id,
                jsonb_build_object(
                    'active_sessions', active_before
                ),
                jsonb_build_object(
                    'active_sessions',
                    greatest(active_before - revoked, 0)
                ),
                NULL,
                jsonb_build_object(
                    'revoked_session_count', revoked
                )
            );

            PERFORM public._platform_insert_audit_log(
                actor.actor_user_id,
                NULL,
                'superadmin',
                actor.actor_organization_id,
                target_org,
                'SESSIONS_REVOKED',
                'user',
                target_user_id,
                jsonb_build_object(
                    'revoked_session_count', revoked
                )
            );

            RETURN QUERY
            SELECT target_user_id, target_org, revoked;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.platform_admin_reset_pilot_account(
            actor_refresh_token_hash text,
            target_organization_id integer
        )
        RETURNS TABLE(
            operations_deleted integer,
            jobs_deleted integer
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            actor record;
            status_value text;
            paid_exists boolean;
            jobs integer;
            operations integer;
        BEGIN
            SELECT *
            INTO actor
            FROM public._platform_superadmin_session_actor(
                actor_refresh_token_hash
            );

            SELECT profile.account_status
            INTO status_value
            FROM public.us_lacey_organization_profiles AS profile
            WHERE profile.organization_id = target_organization_id
            FOR UPDATE;

            IF status_value IS DISTINCT FROM 'PILOT' THEN
                RAISE EXCEPTION 'only PILOT accounts can be reset'
                    USING ERRCODE = '42501';
            END IF;

            SELECT EXISTS (
                SELECT 1
                FROM public.us_lacey_payments AS payment
                WHERE payment.organization_id = target_organization_id
                  AND payment.provider = 'LEMON_SQUEEZY'
                  AND payment.status = 'VERIFIED'
            )
            INTO paid_exists;

            IF paid_exists THEN
                RAISE EXCEPTION 'commercial account cannot be reset'
                    USING ERRCODE = '42501';
            END IF;

            SELECT count(*)::integer
            INTO jobs
            FROM public.us_lacey_processing_jobs AS job
            WHERE job.organization_id = target_organization_id;

            SELECT count(*)::integer
            INTO operations
            FROM public.us_lacey_operations AS operation
            WHERE operation.organization_id = target_organization_id;

            DELETE FROM public.us_lacey_operations AS operation
            WHERE operation.organization_id = target_organization_id;

            PERFORM public._us_lacey_admin_audit(
                actor.actor_user_id,
                actor.actor_organization_id,
                'RESET_PILOT',
                target_organization_id,
                NULL,
                jsonb_build_object(
                    'account_status', status_value,
                    'operation_count', operations,
                    'processing_job_count', jobs
                ),
                jsonb_build_object(
                    'account_status', status_value,
                    'operation_count', 0,
                    'processing_job_count', 0
                ),
                NULL,
                '{}'::jsonb
            );

            PERFORM public._platform_insert_audit_log(
                actor.actor_user_id,
                NULL,
                'superadmin',
                actor.actor_organization_id,
                target_organization_id,
                'PILOT_TEST_RESET',
                'us_lacey_account',
                target_organization_id,
                jsonb_build_object(
                    'operations_deleted', operations,
                    'jobs_deleted', jobs
                )
            );

            RETURN QUERY
            SELECT operations, jobs;
        END;
        $$;
        """
    )


def _restore_044_platform_mutations() -> None:
    # These five definitions are copied verbatim from migration 044.
    # Drop/recreate keeps those definitions exact rather than importing history.
    for signature in MUTATION_FUNCTIONS:
        op.execute(f"DROP FUNCTION IF EXISTS {signature}")

    op.execute(
        r"""CREATE FUNCTION public.platform_admin_promote_existing_user(actor_refresh_token_hash text, target_email text)
    RETURNS TABLE(user_id integer, organization_id integer, email text, role text, revoked_session_count integer)
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
    DECLARE actor record; target record; matches integer; revoked integer;
    BEGIN
      SELECT * INTO actor FROM public._platform_superadmin_session_actor(actor_refresh_token_hash);
      SELECT count(*) INTO matches FROM public.users AS u WHERE lower(btrim(u.email)) = lower(btrim(target_email));
      IF matches <> 1 THEN RAISE EXCEPTION 'founder identity must match exactly one existing user' USING ERRCODE='22023'; END IF;
      SELECT u.id, u.organization_id, u.email INTO target FROM public.users AS u WHERE lower(btrim(u.email)) = lower(btrim(target_email));
      UPDATE public.users AS u SET role='superadmin', is_active=true WHERE u.id=target.id AND u.organization_id=target.organization_id;
      UPDATE public.user_sessions AS s SET revoked_at=coalesce(s.revoked_at, now()) WHERE s.user_id=target.id AND s.organization_id=target.organization_id AND s.revoked_at IS NULL AND (target.id <> actor.actor_user_id OR s.id <> actor.actor_session_id);
      GET DIAGNOSTICS revoked = ROW_COUNT;
      PERFORM public._platform_insert_audit_log(actor.actor_user_id, NULL, 'superadmin', actor.actor_organization_id, target.organization_id, 'FOUNDER_PROMOTED', 'user', target.id, jsonb_build_object('email', target.email, 'role', 'superadmin', 'revoked_session_count', revoked));
      RETURN QUERY SELECT target.id, target.organization_id, target.email::text, 'superadmin'::text, revoked;
    END $$;"""
    )
    op.execute(
        r"""CREATE FUNCTION public.platform_admin_set_us_lacey_account_status(actor_refresh_token_hash text, target_organization_id integer, requested_status text)
    RETURNS TABLE(organization_id integer, account_status text)
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
    DECLARE actor record; previous text; normalized text;
    BEGIN
      SELECT * INTO actor FROM public._platform_superadmin_session_actor(actor_refresh_token_hash);
      normalized := upper(btrim(coalesce(requested_status,'')));
      IF normalized NOT IN ('PILOT','ACTIVE','SUSPENDED') THEN RAISE EXCEPTION 'unsupported account status' USING ERRCODE='22023'; END IF;
      SELECT profile.account_status INTO previous FROM public.us_lacey_organization_profiles AS profile WHERE profile.organization_id=target_organization_id FOR UPDATE;
      IF previous IS NULL THEN RAISE EXCEPTION 'U.S. Lacey account not found' USING ERRCODE='22023'; END IF;
      UPDATE public.us_lacey_organization_profiles AS profile SET account_status=normalized WHERE profile.organization_id=target_organization_id;
      PERFORM public._platform_insert_audit_log(actor.actor_user_id, NULL, 'superadmin', actor.actor_organization_id, target_organization_id, 'ACCOUNT_STATUS_CHANGED', 'us_lacey_account', target_organization_id, jsonb_build_object('before', previous, 'after', normalized));
      RETURN QUERY SELECT target_organization_id, normalized;
    END $$;"""
    )
    op.execute(
        r"""CREATE FUNCTION public.platform_admin_set_us_lacey_operation_limit(actor_refresh_token_hash text, target_organization_id integer, requested_limit integer)
    RETURNS TABLE(organization_id integer, monthly_operation_limit integer)
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
    DECLARE actor record; previous integer;
    BEGIN
      SELECT * INTO actor FROM public._platform_superadmin_session_actor(actor_refresh_token_hash);
      IF requested_limit IS NULL OR requested_limit < 1 OR requested_limit > 100000 THEN RAISE EXCEPTION 'operation limit is outside safe bounds' USING ERRCODE='22023'; END IF;
      SELECT subscription.monthly_operation_limit INTO previous FROM public.us_lacey_subscriptions AS subscription WHERE subscription.organization_id=target_organization_id FOR UPDATE;
      IF previous IS NULL THEN RAISE EXCEPTION 'U.S. Lacey subscription not found' USING ERRCODE='22023'; END IF;
      UPDATE public.us_lacey_subscriptions AS subscription SET monthly_operation_limit=requested_limit WHERE subscription.organization_id=target_organization_id;
      PERFORM public._platform_insert_audit_log(actor.actor_user_id, NULL, 'superadmin', actor.actor_organization_id, target_organization_id, 'OPERATION_LIMIT_CHANGED', 'us_lacey_subscription', target_organization_id, jsonb_build_object('before', previous, 'after', requested_limit));
      RETURN QUERY SELECT target_organization_id, requested_limit;
    END $$;"""
    )
    op.execute(
        r"""CREATE FUNCTION public.platform_admin_revoke_user_sessions(actor_refresh_token_hash text, target_user_id integer)
    RETURNS TABLE(user_id integer, organization_id integer, revoked_session_count integer)
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
    DECLARE actor record; target_org integer; revoked integer;
    BEGIN
      SELECT * INTO actor FROM public._platform_superadmin_session_actor(actor_refresh_token_hash);
      SELECT u.organization_id INTO target_org FROM public.users AS u WHERE u.id=target_user_id;
      IF target_org IS NULL THEN RAISE EXCEPTION 'user not found' USING ERRCODE='22023'; END IF;
      UPDATE public.user_sessions AS s SET revoked_at=now() WHERE s.user_id=target_user_id AND s.revoked_at IS NULL;
      GET DIAGNOSTICS revoked = ROW_COUNT;
      PERFORM public._platform_insert_audit_log(actor.actor_user_id, NULL, 'superadmin', actor.actor_organization_id, target_org, 'SESSIONS_REVOKED', 'user', target_user_id, jsonb_build_object('revoked_session_count', revoked));
      RETURN QUERY SELECT target_user_id, target_org, revoked;
    END $$;"""
    )
    op.execute(
        r"""CREATE FUNCTION public.platform_admin_reset_pilot_account(actor_refresh_token_hash text, target_organization_id integer)
    RETURNS TABLE(operations_deleted integer, jobs_deleted integer)
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
    DECLARE actor record; status_value text; paid_exists boolean; jobs integer; operations integer;
    BEGIN
      SELECT * INTO actor FROM public._platform_superadmin_session_actor(actor_refresh_token_hash);
      SELECT profile.account_status INTO status_value FROM public.us_lacey_organization_profiles AS profile WHERE profile.organization_id=target_organization_id FOR UPDATE;
      IF status_value IS DISTINCT FROM 'PILOT' THEN RAISE EXCEPTION 'only PILOT accounts can be reset' USING ERRCODE='42501'; END IF;
      SELECT EXISTS(SELECT 1 FROM public.us_lacey_payments AS payment WHERE payment.organization_id=target_organization_id AND payment.provider='LEMON_SQUEEZY' AND payment.status='VERIFIED') INTO paid_exists;
      IF paid_exists THEN RAISE EXCEPTION 'commercial account cannot be reset' USING ERRCODE='42501'; END IF;
      SELECT count(*)::integer INTO jobs FROM public.us_lacey_processing_jobs AS job WHERE job.organization_id=target_organization_id;
      DELETE FROM public.us_lacey_operations AS operation WHERE operation.organization_id=target_organization_id;
      GET DIAGNOSTICS operations = ROW_COUNT;
      PERFORM public._platform_insert_audit_log(actor.actor_user_id, NULL, 'superadmin', actor.actor_organization_id, target_organization_id, 'PILOT_TEST_RESET', 'us_lacey_account', target_organization_id, jsonb_build_object('operations_deleted', operations, 'jobs_deleted', jobs));
      RETURN QUERY SELECT operations, jobs;
    END $$;"""
    )

    for signature in MUTATION_FUNCTIONS:
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM {WORKER_ROLE}")
        op.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {RUNTIME_ROLE}")


def upgrade() -> None:
    op.add_column(
        "us_lacey_subscriptions",
        sa.Column(
            "billing_provider",
            sa.String(length=32),
            nullable=False,
            server_default="NONE",
        ),
    )
    op.add_column(
        "us_lacey_subscriptions",
        sa.Column(
            "provider_customer_id",
            sa.String(length=255),
            nullable=True,
        ),
    )
    op.add_column(
        "us_lacey_subscriptions",
        sa.Column(
            "provider_subscription_id",
            sa.String(length=255),
            nullable=True,
        ),
    )
    op.add_column(
        "us_lacey_subscriptions",
        sa.Column(
            "billing_sync_status",
            sa.String(length=24),
            nullable=False,
            server_default="NEVER_SYNCED",
        ),
    )
    op.add_column(
        "us_lacey_subscriptions",
        sa.Column(
            "billing_last_sync_attempt_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "us_lacey_subscriptions",
        sa.Column(
            "billing_last_synced_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "us_lacey_subscriptions",
        sa.Column(
            "billing_last_error_code",
            sa.String(length=100),
            nullable=True,
        ),
    )
    op.add_column(
        "us_lacey_subscriptions",
        sa.Column(
            "provider_updated_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )

    op.create_check_constraint(
        "ck_us_lacey_subscriptions_billing_provider",
        "us_lacey_subscriptions",
        """
        billing_provider IN (
            'NONE',
            'MANUAL',
            'LEMON_SQUEEZY',
            'STRIPE'
        )
        """,
    )
    op.create_check_constraint(
        "ck_us_lacey_subscriptions_billing_sync_status",
        "us_lacey_subscriptions",
        """
        billing_sync_status IN (
            'NEVER_SYNCED',
            'PENDING',
            'IN_SYNC',
            'ERROR'
        )
        """,
    )

    op.create_index(
        "uq_us_lacey_subscriptions_provider_subscription",
        "us_lacey_subscriptions",
        ["billing_provider", "provider_subscription_id"],
        unique=True,
        postgresql_where=sa.text(
            "provider_subscription_id IS NOT NULL"
        ),
    )
    op.create_index(
        "ix_us_lacey_subscriptions_billing_sync",
        "us_lacey_subscriptions",
        ["billing_sync_status", "billing_last_sync_attempt_at"],
    )

    op.create_table(
        "us_lacey_admin_audit_logs",
        sa.Column(
            "id",
            sa.BigInteger(),
            primary_key=True,
            autoincrement=True,
        ),
        # Deliberately no foreign keys: these tombstones must survive
        # deletion of admins, users, or target organizations.
        sa.Column("admin_user_id", sa.Integer(), nullable=False),
        sa.Column("admin_organization_id", sa.Integer(), nullable=False),
        sa.Column("action_type", sa.String(length=64), nullable=False),
        sa.Column("target_organization_id", sa.Integer(), nullable=True),
        sa.Column("target_user_id", sa.Integer(), nullable=True),
        sa.Column(
            "previous_state",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "new_state",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "request_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "timestamp",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "action_type ~ '^[A-Z0-9_]{3,64}$'",
            name="ck_us_lacey_admin_audit_action_type",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(previous_state) = 'object'",
            name="ck_us_lacey_admin_audit_previous_object",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(new_state) = 'object'",
            name="ck_us_lacey_admin_audit_new_object",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(metadata) = 'object'",
            name="ck_us_lacey_admin_audit_metadata_object",
        ),
    )

    op.execute(
        """
        CREATE INDEX ix_us_lacey_admin_audit_target_time
        ON public.us_lacey_admin_audit_logs
            (target_organization_id, "timestamp" DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX ix_us_lacey_admin_audit_actor_time
        ON public.us_lacey_admin_audit_logs
            (admin_user_id, "timestamp" DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX ix_us_lacey_admin_audit_action_time
        ON public.us_lacey_admin_audit_logs
            (action_type, "timestamp" DESC)
        """
    )

    op.execute(
        "ALTER TABLE public.us_lacey_admin_audit_logs "
        "ENABLE ROW LEVEL SECURITY"
    )
    op.execute(
        "ALTER TABLE public.us_lacey_admin_audit_logs "
        "FORCE ROW LEVEL SECURITY"
    )

    op.execute(
        f"""
        CREATE POLICY us_lacey_admin_audit_platform_select
        ON public.us_lacey_admin_audit_logs
        FOR SELECT
        TO {PLATFORM_ROLE}
        USING (true)
        """
    )
    op.execute(
        f"""
        CREATE POLICY us_lacey_admin_audit_platform_insert
        ON public.us_lacey_admin_audit_logs
        FOR INSERT
        TO {PLATFORM_ROLE}
        WITH CHECK (true)
        """
    )

    op.execute(
        "REVOKE ALL ON TABLE public.us_lacey_admin_audit_logs FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL ON TABLE public.us_lacey_admin_audit_logs "
        f"FROM {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE ALL ON TABLE public.us_lacey_admin_audit_logs "
        f"FROM {WORKER_ROLE}"
    )
    op.execute(
        f"REVOKE ALL ON TABLE public.us_lacey_admin_audit_logs "
        f"FROM {PLATFORM_ROLE}"
    )
    op.execute(
        f"GRANT SELECT, INSERT "
        f"ON TABLE public.us_lacey_admin_audit_logs TO {PLATFORM_ROLE}"
    )

    op.execute(
        "REVOKE ALL ON SEQUENCE "
        "public.us_lacey_admin_audit_logs_id_seq FROM PUBLIC"
    )
    op.execute(
        f"REVOKE ALL ON SEQUENCE "
        f"public.us_lacey_admin_audit_logs_id_seq FROM {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE ALL ON SEQUENCE "
        f"public.us_lacey_admin_audit_logs_id_seq FROM {WORKER_ROLE}"
    )
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE "
        f"public.us_lacey_admin_audit_logs_id_seq TO {PLATFORM_ROLE}"
    )

    _enter_platform_role()
    _create_admin_audit_helper()
    _replace_platform_mutations_with_dual_audit()

    op.execute(f"REVOKE ALL ON FUNCTION {AUDIT_FUNCTION} FROM PUBLIC")
    op.execute(f"REVOKE ALL ON FUNCTION {AUDIT_FUNCTION} FROM {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL ON FUNCTION {AUDIT_FUNCTION} FROM {WORKER_ROLE}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {AUDIT_FUNCTION} TO {PLATFORM_ROLE}")

    for signature in MUTATION_FUNCTIONS:
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM {WORKER_ROLE}")
        op.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {RUNTIME_ROLE}")

    _leave_platform_role()


def downgrade() -> None:
    _enter_platform_role()

    # Restore all five 044 bodies before removing the helper they no longer use.
    _restore_044_platform_mutations()
    op.execute(f"DROP FUNCTION IF EXISTS {AUDIT_FUNCTION}")

    _leave_platform_role()

    op.execute(
        """
        DROP POLICY IF EXISTS us_lacey_admin_audit_platform_insert
        ON public.us_lacey_admin_audit_logs
        """
    )
    op.execute(
        """
        DROP POLICY IF EXISTS us_lacey_admin_audit_platform_select
        ON public.us_lacey_admin_audit_logs
        """
    )

    op.execute(
        "DROP INDEX IF EXISTS public.ix_us_lacey_admin_audit_action_time"
    )
    op.execute(
        "DROP INDEX IF EXISTS public.ix_us_lacey_admin_audit_actor_time"
    )
    op.execute(
        "DROP INDEX IF EXISTS public.ix_us_lacey_admin_audit_target_time"
    )
    op.drop_table("us_lacey_admin_audit_logs")

    op.drop_index(
        "ix_us_lacey_subscriptions_billing_sync",
        table_name="us_lacey_subscriptions",
    )
    op.drop_index(
        "uq_us_lacey_subscriptions_provider_subscription",
        table_name="us_lacey_subscriptions",
    )

    op.drop_constraint(
        "ck_us_lacey_subscriptions_billing_sync_status",
        "us_lacey_subscriptions",
        type_="check",
    )
    op.drop_constraint(
        "ck_us_lacey_subscriptions_billing_provider",
        "us_lacey_subscriptions",
        type_="check",
    )

    for column_name in (
        "provider_updated_at",
        "billing_last_error_code",
        "billing_last_synced_at",
        "billing_last_sync_attempt_at",
        "billing_sync_status",
        "provider_subscription_id",
        "provider_customer_id",
        "billing_provider",
    ):
        op.drop_column("us_lacey_subscriptions", column_name)
