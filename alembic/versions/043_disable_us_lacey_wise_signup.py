"""Disable Wise for new U.S. Lacey self-service registrations.

Revision ID: 043_042_us_lacey_owner_admin
Revises: 042_us_lacey_owner_admin
Create Date: 2026-09-03 16:05:00.000000

The payment table keeps its historical WISE value so existing audit records stay
readable. This migration changes only the supported registration surface: new
accounts may start with MANUAL_BANK_TRANSFER or LEMON_SQUEEZY, never WISE.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "043_042_us_lacey_owner_admin"
down_revision: Union[str, Sequence[str], None] = "042_us_lacey_owner_admin"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"
REGISTER_SIGNATURE = (
    "public.us_lacey_self_register(text,text,text,text,text,text,integer,integer,text,text,text,text)"
)
PRE_043_SIGNATURE = (
    "public.us_lacey_self_register_pre_043(text,text,text,text,text,text,integer,integer,text,text,text,text)"
)


def _grant_temp_platform_set() -> None:
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )


def _revoke_temp_platform_set() -> None:
    op.execute(f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER")


def _enter_platform_role() -> None:
    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET ROLE {PLATFORM_ROLE}")


def _leave_platform_role() -> None:
    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()


def upgrade() -> None:
    """Put a fail-closed provider guard in front of the certified 041 registrar."""
    _enter_platform_role()

    # Preserve the already-certified 041 implementation behind a private name.
    # Renaming instead of rewriting it keeps Lemon activation behavior unchanged.
    op.execute(
        "ALTER FUNCTION public.us_lacey_self_register(text,text,text,text,text,text,integer,integer,text,text,text,text) "
        "RENAME TO us_lacey_self_register_pre_043"
    )
    op.execute(f"REVOKE ALL ON FUNCTION {PRE_043_SIGNATURE} FROM PUBLIC")
    op.execute(f"REVOKE ALL ON FUNCTION {PRE_043_SIGNATURE} FROM {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL ON FUNCTION {PRE_043_SIGNATURE} FROM {WORKER_ROLE}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {PRE_043_SIGNATURE} TO {PLATFORM_ROLE}")

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_self_register(
            requested_legal_name text,
            requested_business_type text,
            requested_admin_name text,
            requested_admin_email text,
            requested_password_hash text,
            requested_verification_token_hash text,
            requested_price_cents integer,
            requested_monthly_operation_limit integer,
            requested_payment_provider text,
            requested_terms_version text,
            requested_privacy_version text,
            requested_beta_version text
        )
        RETURNS TABLE (
            organization_id integer,
            user_id integer,
            payment_public_id uuid,
            payment_reference text,
            amount_cents integer,
            account_status text
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            normalized_provider text := upper(btrim(coalesce(requested_payment_provider, '')));
            registration record;
        BEGIN
            IF normalized_provider NOT IN ('MANUAL_BANK_TRANSFER','LEMON_SQUEEZY') THEN
                RAISE EXCEPTION 'invalid initial payment provider'
                    USING ERRCODE = '22023';
            END IF;

            SELECT * INTO registration
            FROM public.us_lacey_self_register_pre_043(
                requested_legal_name,
                requested_business_type,
                requested_admin_name,
                requested_admin_email,
                requested_password_hash,
                requested_verification_token_hash,
                requested_price_cents,
                requested_monthly_operation_limit,
                normalized_provider,
                requested_terms_version,
                requested_privacy_version,
                requested_beta_version
            );

            RETURN QUERY SELECT
                registration.organization_id::integer,
                registration.user_id::integer,
                registration.payment_public_id::uuid,
                registration.payment_reference::text,
                registration.amount_cents::integer,
                registration.account_status::text;
        END;
        $$;
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {REGISTER_SIGNATURE} FROM PUBLIC")
    op.execute(f"REVOKE ALL ON FUNCTION {REGISTER_SIGNATURE} FROM {WORKER_ROLE}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {REGISTER_SIGNATURE} TO {RUNTIME_ROLE}")

    _leave_platform_role()


def downgrade() -> None:
    """Restore the exact 041 registration facade when rolling back 043."""
    _enter_platform_role()
    op.execute(f"DROP FUNCTION IF EXISTS {REGISTER_SIGNATURE}")
    op.execute(
        "ALTER FUNCTION public.us_lacey_self_register_pre_043(text,text,text,text,text,text,integer,integer,text,text,text,text) "
        "RENAME TO us_lacey_self_register"
    )
    op.execute(f"REVOKE ALL ON FUNCTION {REGISTER_SIGNATURE} FROM PUBLIC")
    op.execute(f"REVOKE ALL ON FUNCTION {REGISTER_SIGNATURE} FROM {WORKER_ROLE}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {REGISTER_SIGNATURE} TO {RUNTIME_ROLE}")
    _leave_platform_role()
