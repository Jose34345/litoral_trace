"""Add capability-scoped U.S. Lacey platform-admin controls.

Revision ID: 044_platform_admin_control_plane
Revises: 043_us_lacey_engine2_shadow
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "044_platform_admin_control_plane"
down_revision: Union[str, Sequence[str], None] = "043_us_lacey_engine2_shadow"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"
FUNCTIONS = (
    "public.platform_admin_promote_existing_user(text, text)",
    "public.platform_admin_set_us_lacey_account_status(text, integer, text)",
    "public.platform_admin_set_us_lacey_operation_limit(text, integer, integer)",
    "public.platform_admin_revoke_user_sessions(text, integer)",
    "public.platform_admin_reset_pilot_account(text, integer)",
    "public.platform_admin_users(text)",
    "public.platform_admin_failed_jobs(text)",
)


def _set_platform_role() -> None:
    op.execute(f"GRANT {PLATFORM_ROLE} TO CURRENT_USER WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER")
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET ROLE {PLATFORM_ROLE}")


def _reset_platform_role() -> None:
    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    op.execute(f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER")


def upgrade() -> None:
    # Promotion is the one new users mutation.  The grant and FORCE-RLS policy
    # belong only to the non-login platform definer, never the runtime role.
    op.execute(f"GRANT UPDATE ON TABLE public.users TO {PLATFORM_ROLE}")
    op.execute("CREATE POLICY users_platform_update_044 ON public.users FOR UPDATE TO litoral_trace_platform_definer USING (true) WITH CHECK (true)")
    _set_platform_role()
    # Each function first validates a non-revoked superadmin persistent session.
    # The runtime receives EXECUTE only; it never gains cross-tenant table grants.
    op.execute("""
    CREATE FUNCTION public.platform_admin_promote_existing_user(actor_refresh_token_hash text, target_email text)
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
    END $$;
    """)
    op.execute("""
    CREATE FUNCTION public.platform_admin_set_us_lacey_account_status(actor_refresh_token_hash text, target_organization_id integer, requested_status text)
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
    END $$;
    """)
    op.execute("""
    CREATE FUNCTION public.platform_admin_set_us_lacey_operation_limit(actor_refresh_token_hash text, target_organization_id integer, requested_limit integer)
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
    END $$;
    """)
    op.execute("""
    CREATE FUNCTION public.platform_admin_revoke_user_sessions(actor_refresh_token_hash text, target_user_id integer)
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
    END $$;
    """)
    # Operations own all listed working records through tenant-scoped CASCADE FKs.
    # The explicit commercial guard makes paid Lemon accounts impossible to reset.
    op.execute("""
    CREATE FUNCTION public.platform_admin_reset_pilot_account(actor_refresh_token_hash text, target_organization_id integer)
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
    END $$;
    """)
    op.execute("""
    CREATE FUNCTION public.platform_admin_users(actor_refresh_token_hash text)
    RETURNS TABLE(user_id integer, organization_id integer, organization_name text, full_name text, email text, role text, is_active boolean, created_at timestamptz, last_login_at timestamptz)
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$ BEGIN
      PERFORM 1 FROM public._platform_superadmin_session_actor(actor_refresh_token_hash);
      RETURN QUERY SELECT u.id,u.organization_id,o.name::text,u.full_name::text,u.email::text,u.role::text,u.is_active,u.created_at,u.last_login_at FROM public.users u JOIN public.organizations o ON o.id=u.organization_id ORDER BY u.created_at DESC;
    END $$;
    """)
    op.execute("""
    CREATE FUNCTION public.platform_admin_failed_jobs(actor_refresh_token_hash text)
    RETURNS TABLE(organization_id integer, organization_name text, operation_id integer, job_status text, error_code text, error_message text, retry_count integer, created_at timestamptz)
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$ BEGIN
      PERFORM 1 FROM public._platform_superadmin_session_actor(actor_refresh_token_hash);
      RETURN QUERY SELECT j.organization_id,o.name::text,j.operation_id,j.status::text,j.last_error_code::text,left(coalesce(j.last_error_message,''),512)::text,j.attempt_count,j.created_at FROM public.us_lacey_processing_jobs j JOIN public.organizations o ON o.id=j.organization_id WHERE j.status='FAILED' ORDER BY j.created_at DESC LIMIT 100;
    END $$;
    """)
    for signature in FUNCTIONS:
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM {WORKER_ROLE}")
        op.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {RUNTIME_ROLE}")
    _reset_platform_role()


def downgrade() -> None:
    # Policies can be changed only by the table owner, not the definer role.
    op.execute("DROP POLICY IF EXISTS users_platform_update_044 ON public.users")
    op.execute(f"REVOKE UPDATE ON TABLE public.users FROM {PLATFORM_ROLE}")
    _set_platform_role()
    for signature in FUNCTIONS:
        op.execute(f"DROP FUNCTION IF EXISTS {signature}")
    _reset_platform_role()
