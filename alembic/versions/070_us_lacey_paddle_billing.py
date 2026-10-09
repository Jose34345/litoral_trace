"""Add Paddle recurring billing for the U.S. Lacey customer portal.

Revision ID: 070_us_lacey_paddle_billing
Revises: 069_remove_postgis_geometry
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "070_us_lacey_paddle_billing"
down_revision = "069_remove_postgis_geometry"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"

REGISTER_SIGNATURE = (
    "public.us_lacey_self_register(text,text,text,text,text,text,integer,integer,text,text,text,text)"
)
REGISTER_LEGACY_SIGNATURE = (
    "public.us_lacey_self_register_legacy(text,text,text,text,text,text,integer,integer,text,text,text,text)"
)
APPLY_TRANSACTION_SIGNATURE = (
    "public.us_lacey_apply_paddle_transaction("
    "integer,uuid,text,text,text,text,text,integer,text,timestamptz,timestamptz,timestamptz)"
)
APPLY_SUBSCRIPTION_SIGNATURE = (
    "public.us_lacey_apply_paddle_subscription_event("
    "integer,uuid,text,text,text,text,text,timestamptz,timestamptz)"
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


def _replace_registration_function(*, allow_paddle: bool) -> None:
    providers = (
        "'MANUAL_BANK_TRANSFER','WISE','LEMON_SQUEEZY','PADDLE'"
        if allow_paddle
        else "'MANUAL_BANK_TRANSFER','WISE','LEMON_SQUEEZY'"
    )
    paddle_branch = """
            IF normalized_provider = 'PADDLE' THEN
                UPDATE public.us_lacey_payments AS payment
                SET provider = 'PADDLE', updated_at = now()
                WHERE payment.organization_id = registration.organization_id
                  AND payment.public_id = registration.payment_public_id
                  AND payment.status = 'PENDING';
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'registered Paddle payment not found'
                        USING ERRCODE = 'P0002';
                END IF;

                UPDATE public.us_lacey_subscriptions AS subscription
                SET billing_provider = 'PADDLE',
                    billing_sync_status = 'PENDING',
                    billing_last_error_code = NULL,
                    updated_at = now()
                WHERE subscription.organization_id = registration.organization_id;
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'registered Paddle subscription not found'
                        USING ERRCODE = 'P0002';
                END IF;
            END IF;
    """ if allow_paddle else ""

    paddle_case = (
        "WHEN normalized_provider IN ('LEMON_SQUEEZY','PADDLE') "
        "THEN 'MANUAL_BANK_TRANSFER'"
        if allow_paddle
        else "WHEN normalized_provider = 'LEMON_SQUEEZY' "
        "THEN 'MANUAL_BANK_TRANSFER'"
    )

    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.us_lacey_self_register(
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
            normalized_provider text :=
                upper(btrim(coalesce(requested_payment_provider, '')));
            registration record;
        BEGIN
            IF normalized_provider NOT IN ({providers}) THEN
                RAISE EXCEPTION 'invalid initial payment provider'
                    USING ERRCODE = '22023';
            END IF;

            SELECT * INTO registration
            FROM public.us_lacey_self_register_legacy(
                requested_legal_name,
                requested_business_type,
                requested_admin_name,
                requested_admin_email,
                requested_password_hash,
                requested_verification_token_hash,
                requested_price_cents,
                requested_monthly_operation_limit,
                CASE
                    {paddle_case}
                    ELSE normalized_provider
                END,
                requested_terms_version,
                requested_privacy_version,
                requested_beta_version
            );

            IF normalized_provider = 'LEMON_SQUEEZY' THEN
                UPDATE public.us_lacey_payments AS payment
                SET provider = 'LEMON_SQUEEZY', updated_at = now()
                WHERE payment.organization_id = registration.organization_id
                  AND payment.public_id = registration.payment_public_id
                  AND payment.status = 'PENDING';
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'registered Lemon payment not found'
                        USING ERRCODE = 'P0002';
                END IF;
            END IF;

            {paddle_branch}

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


def upgrade() -> None:
    op.drop_constraint(
        "ck_us_lacey_payments_provider",
        "us_lacey_payments",
        type_="check",
    )
    op.create_check_constraint(
        "ck_us_lacey_payments_provider",
        "us_lacey_payments",
        "provider IN ("
        "'MANUAL_BANK_TRANSFER','WISE','STRIPE','LEMON_SQUEEZY','PADDLE'"
        ")",
    )

    op.drop_constraint(
        "ck_us_lacey_subscriptions_billing_provider",
        "us_lacey_subscriptions",
        type_="check",
    )
    op.create_check_constraint(
        "ck_us_lacey_subscriptions_billing_provider",
        "us_lacey_subscriptions",
        "billing_provider IN ("
        "'NONE','MANUAL','LEMON_SQUEEZY','PADDLE','STRIPE'"
        ")",
    )

    op.add_column(
        "us_lacey_subscriptions",
        sa.Column("provider_last_transaction_id", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "us_lacey_subscriptions",
        sa.Column("provider_last_event_id", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "us_lacey_subscriptions",
        sa.Column("provider_last_event_sha256", sa.String(length=64), nullable=True),
    )
    op.create_index(
        "uq_us_lacey_subscriptions_provider_last_transaction",
        "us_lacey_subscriptions",
        ["billing_provider", "provider_last_transaction_id"],
        unique=True,
        postgresql_where=sa.text("provider_last_transaction_id IS NOT NULL"),
    )

    _enter_platform_role()
    _replace_registration_function(allow_paddle=True)

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_apply_paddle_transaction(
            target_organization_id integer,
            target_payment_public_id uuid,
            target_transaction_id text,
            target_event_id text,
            target_payload_sha256 text,
            target_customer_id text,
            target_subscription_id text,
            target_amount_cents integer,
            target_currency text,
            target_period_start timestamptz,
            target_period_end timestamptz,
            target_provider_updated_at timestamptz
        )
        RETURNS TABLE (
            payment_status text,
            subscription_status text,
            account_status text,
            idempotent boolean
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            normalized_transaction_id text :=
                btrim(coalesce(target_transaction_id, ''));
            normalized_event_id text := btrim(coalesce(target_event_id, ''));
            normalized_customer_id text := btrim(coalesce(target_customer_id, ''));
            normalized_subscription_id text :=
                btrim(coalesce(target_subscription_id, ''));
            payment_row record;
            subscription_row record;
            current_account_status text;
        BEGIN
            IF target_organization_id IS NULL
               OR target_organization_id <= 0
               OR target_payment_public_id IS NULL
               OR normalized_transaction_id !~ '^txn_[a-z0-9]{26}$'
               OR normalized_event_id !~ '^evt_[a-z0-9]{26}$'
               OR normalized_customer_id !~ '^ctm_[a-z0-9]{26}$'
               OR normalized_subscription_id !~ '^sub_[a-z0-9]{26}$'
               OR target_payload_sha256 IS NULL
               OR target_payload_sha256 !~ '^[0-9a-f]{64}$'
               OR target_amount_cents IS NULL
               OR target_amount_cents <= 0
               OR upper(btrim(coalesce(target_currency, ''))) <> 'USD'
               OR target_provider_updated_at IS NULL THEN
                RAISE EXCEPTION 'invalid Paddle transaction activation payload'
                    USING ERRCODE = '22023';
            END IF;

            SELECT p.id, p.subscription_id, p.status, p.amount_cents,
                   p.currency, p.customer_reference
            INTO payment_row
            FROM public.us_lacey_payments AS p
            WHERE p.organization_id = target_organization_id
              AND p.public_id = target_payment_public_id
              AND p.provider = 'PADDLE'
            FOR UPDATE;

            IF payment_row.id IS NULL THEN
                RAISE EXCEPTION 'Paddle payment not found'
                    USING ERRCODE = 'P0002';
            END IF;
            IF payment_row.amount_cents <> target_amount_cents
               OR payment_row.currency <> 'USD' THEN
                RAISE EXCEPTION 'Paddle payment amount mismatch'
                    USING ERRCODE = '22023';
            END IF;

            SELECT s.*
            INTO subscription_row
            FROM public.us_lacey_subscriptions AS s
            WHERE s.id = payment_row.subscription_id
              AND s.organization_id = target_organization_id
            FOR UPDATE;

            IF subscription_row.id IS NULL THEN
                RAISE EXCEPTION 'Paddle subscription record not found'
                    USING ERRCODE = 'P0002';
            END IF;
            IF subscription_row.billing_provider NOT IN ('NONE','PADDLE') THEN
                RAISE EXCEPTION 'subscription belongs to a different billing provider'
                    USING ERRCODE = '22023';
            END IF;
            IF subscription_row.provider_customer_id IS NOT NULL
               AND subscription_row.provider_customer_id <> normalized_customer_id THEN
                RAISE EXCEPTION 'Paddle customer mismatch'
                    USING ERRCODE = '22023';
            END IF;
            IF subscription_row.provider_subscription_id IS NOT NULL
               AND subscription_row.provider_subscription_id <>
                   normalized_subscription_id THEN
                RAISE EXCEPTION 'Paddle subscription mismatch'
                    USING ERRCODE = '22023';
            END IF;

            IF subscription_row.provider_last_event_id = normalized_event_id THEN
                IF subscription_row.provider_last_event_sha256 <>
                   target_payload_sha256 THEN
                    RAISE EXCEPTION 'Paddle event idempotency conflict'
                        USING ERRCODE = '23505';
                END IF;
                SELECT profile.account_status
                INTO current_account_status
                FROM public.us_lacey_organization_profiles AS profile
                WHERE profile.organization_id = target_organization_id;
                RETURN QUERY SELECT
                    payment_row.status::text,
                    subscription_row.status::text,
                    current_account_status,
                    true;
                RETURN;
            END IF;

            IF subscription_row.provider_last_transaction_id =
               normalized_transaction_id THEN
                SELECT profile.account_status
                INTO current_account_status
                FROM public.us_lacey_organization_profiles AS profile
                WHERE profile.organization_id = target_organization_id;
                RETURN QUERY SELECT
                    payment_row.status::text,
                    subscription_row.status::text,
                    current_account_status,
                    true;
                RETURN;
            END IF;

            IF subscription_row.provider_updated_at IS NOT NULL
               AND target_provider_updated_at <
                   subscription_row.provider_updated_at THEN
                SELECT profile.account_status
                INTO current_account_status
                FROM public.us_lacey_organization_profiles AS profile
                WHERE profile.organization_id = target_organization_id;
                RETURN QUERY SELECT
                    payment_row.status::text,
                    subscription_row.status::text,
                    current_account_status,
                    true;
                RETURN;
            END IF;

            IF payment_row.status = 'PENDING' THEN
                UPDATE public.us_lacey_payments AS payment
                SET status = 'VERIFIED',
                    customer_reference = normalized_transaction_id,
                    paid_at = coalesce(payment.paid_at, now()),
                    verified_at = now(),
                    updated_at = now()
                WHERE payment.id = payment_row.id
                  AND payment.organization_id = target_organization_id;
            ELSIF payment_row.status <> 'VERIFIED' THEN
                RAISE EXCEPTION 'Paddle payment cannot activate this account'
                    USING ERRCODE = '22023';
            END IF;

            UPDATE public.us_lacey_subscriptions AS subscription
            SET status = 'ACTIVE',
                started_at = coalesce(
                    subscription.started_at,
                    target_period_start,
                    now()
                ),
                renews_at = coalesce(
                    target_period_end,
                    subscription.renews_at
                ),
                used_operations = CASE
                    WHEN subscription.provider_last_transaction_id IS NULL
                        THEN subscription.used_operations
                    ELSE 0
                END,
                billing_provider = 'PADDLE',
                provider_customer_id = normalized_customer_id,
                provider_subscription_id = normalized_subscription_id,
                provider_last_transaction_id = normalized_transaction_id,
                provider_last_event_id = normalized_event_id,
                provider_last_event_sha256 = target_payload_sha256,
                billing_sync_status = 'IN_SYNC',
                billing_last_sync_attempt_at = now(),
                billing_last_synced_at = now(),
                billing_last_error_code = NULL,
                provider_updated_at = target_provider_updated_at,
                updated_at = now()
            WHERE subscription.id = payment_row.subscription_id
              AND subscription.organization_id = target_organization_id;

            UPDATE public.us_lacey_organization_profiles AS profile
            SET account_status = 'ACTIVE',
                updated_at = now()
            WHERE profile.organization_id = target_organization_id
              AND profile.account_status IN (
                  'PAYMENT_PENDING','PILOT','ACTIVE','SUSPENDED'
              );

            SELECT profile.account_status
            INTO current_account_status
            FROM public.us_lacey_organization_profiles AS profile
            WHERE profile.organization_id = target_organization_id;

            RETURN QUERY SELECT
                'VERIFIED'::text,
                'ACTIVE'::text,
                current_account_status,
                false;
        END;
        $$;
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {APPLY_TRANSACTION_SIGNATURE} FROM PUBLIC")
    op.execute(f"REVOKE ALL ON FUNCTION {APPLY_TRANSACTION_SIGNATURE} FROM {WORKER_ROLE}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {APPLY_TRANSACTION_SIGNATURE} TO {RUNTIME_ROLE}")

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_apply_paddle_subscription_event(
            target_organization_id integer,
            target_payment_public_id uuid,
            target_event_id text,
            target_payload_sha256 text,
            target_customer_id text,
            target_subscription_id text,
            target_provider_status text,
            target_next_billed_at timestamptz,
            target_provider_updated_at timestamptz
        )
        RETURNS TABLE (
            payment_status text,
            subscription_status text,
            account_status text,
            idempotent boolean
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            normalized_event_id text := btrim(coalesce(target_event_id, ''));
            normalized_customer_id text := btrim(coalesce(target_customer_id, ''));
            normalized_subscription_id text :=
                btrim(coalesce(target_subscription_id, ''));
            normalized_status text :=
                lower(btrim(coalesce(target_provider_status, '')));
            mapped_status text;
            payment_row record;
            subscription_row record;
            current_account_status text;
        BEGIN
            IF target_organization_id IS NULL
               OR target_organization_id <= 0
               OR target_payment_public_id IS NULL
               OR normalized_event_id !~ '^evt_[a-z0-9]{26}$'
               OR normalized_customer_id !~ '^ctm_[a-z0-9]{26}$'
               OR normalized_subscription_id !~ '^sub_[a-z0-9]{26}$'
               OR target_payload_sha256 IS NULL
               OR target_payload_sha256 !~ '^[0-9a-f]{64}$'
               OR normalized_status NOT IN (
                   'active','trialing','past_due','paused','canceled'
               )
               OR target_provider_updated_at IS NULL THEN
                RAISE EXCEPTION 'invalid Paddle subscription event payload'
                    USING ERRCODE = '22023';
            END IF;

            mapped_status := CASE
                WHEN normalized_status IN ('active','trialing') THEN 'ACTIVE'
                WHEN normalized_status IN ('past_due','paused') THEN 'PAST_DUE'
                ELSE 'CANCELED'
            END;

            SELECT p.id, p.subscription_id, p.status
            INTO payment_row
            FROM public.us_lacey_payments AS p
            WHERE p.organization_id = target_organization_id
              AND p.public_id = target_payment_public_id
              AND p.provider = 'PADDLE'
            FOR UPDATE;

            IF payment_row.id IS NULL THEN
                RAISE EXCEPTION 'Paddle payment not found'
                    USING ERRCODE = 'P0002';
            END IF;

            SELECT s.*
            INTO subscription_row
            FROM public.us_lacey_subscriptions AS s
            WHERE s.id = payment_row.subscription_id
              AND s.organization_id = target_organization_id
            FOR UPDATE;

            IF subscription_row.id IS NULL THEN
                RAISE EXCEPTION 'Paddle subscription record not found'
                    USING ERRCODE = 'P0002';
            END IF;
            IF subscription_row.billing_provider NOT IN ('NONE','PADDLE') THEN
                RAISE EXCEPTION 'subscription belongs to a different billing provider'
                    USING ERRCODE = '22023';
            END IF;
            IF subscription_row.provider_customer_id IS NOT NULL
               AND subscription_row.provider_customer_id <> normalized_customer_id THEN
                RAISE EXCEPTION 'Paddle customer mismatch'
                    USING ERRCODE = '22023';
            END IF;
            IF subscription_row.provider_subscription_id IS NOT NULL
               AND subscription_row.provider_subscription_id <>
                   normalized_subscription_id THEN
                RAISE EXCEPTION 'Paddle subscription mismatch'
                    USING ERRCODE = '22023';
            END IF;

            IF subscription_row.provider_last_event_id = normalized_event_id THEN
                IF subscription_row.provider_last_event_sha256 <>
                   target_payload_sha256 THEN
                    RAISE EXCEPTION 'Paddle event idempotency conflict'
                        USING ERRCODE = '23505';
                END IF;
                SELECT profile.account_status
                INTO current_account_status
                FROM public.us_lacey_organization_profiles AS profile
                WHERE profile.organization_id = target_organization_id;
                RETURN QUERY SELECT
                    payment_row.status::text,
                    subscription_row.status::text,
                    current_account_status,
                    true;
                RETURN;
            END IF;

            IF subscription_row.provider_updated_at IS NOT NULL
               AND target_provider_updated_at <
                   subscription_row.provider_updated_at THEN
                SELECT profile.account_status
                INTO current_account_status
                FROM public.us_lacey_organization_profiles AS profile
                WHERE profile.organization_id = target_organization_id;
                RETURN QUERY SELECT
                    payment_row.status::text,
                    subscription_row.status::text,
                    current_account_status,
                    true;
                RETURN;
            END IF;

            UPDATE public.us_lacey_subscriptions AS subscription
            SET billing_provider = 'PADDLE',
                provider_customer_id = normalized_customer_id,
                provider_subscription_id = normalized_subscription_id,
                status = CASE
                    WHEN payment_row.status = 'VERIFIED' THEN mapped_status
                    WHEN mapped_status IN ('PAST_DUE','CANCELED') THEN mapped_status
                    ELSE subscription.status
                END,
                renews_at = coalesce(
                    target_next_billed_at,
                    subscription.renews_at
                ),
                provider_last_event_id = normalized_event_id,
                provider_last_event_sha256 = target_payload_sha256,
                billing_sync_status = 'IN_SYNC',
                billing_last_sync_attempt_at = now(),
                billing_last_synced_at = now(),
                billing_last_error_code = NULL,
                provider_updated_at = target_provider_updated_at,
                updated_at = now()
            WHERE subscription.id = payment_row.subscription_id
              AND subscription.organization_id = target_organization_id;

            IF payment_row.status = 'VERIFIED' THEN
                UPDATE public.us_lacey_organization_profiles AS profile
                SET account_status = CASE
                        WHEN mapped_status = 'ACTIVE' THEN 'ACTIVE'
                        ELSE 'SUSPENDED'
                    END,
                    updated_at = now()
                WHERE profile.organization_id = target_organization_id;
            END IF;

            SELECT profile.account_status
            INTO current_account_status
            FROM public.us_lacey_organization_profiles AS profile
            WHERE profile.organization_id = target_organization_id;

            SELECT s.status
            INTO mapped_status
            FROM public.us_lacey_subscriptions AS s
            WHERE s.id = payment_row.subscription_id
              AND s.organization_id = target_organization_id;

            RETURN QUERY SELECT
                payment_row.status::text,
                mapped_status,
                current_account_status,
                false;
        END;
        $$;
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {APPLY_SUBSCRIPTION_SIGNATURE} FROM PUBLIC")
    op.execute(f"REVOKE ALL ON FUNCTION {APPLY_SUBSCRIPTION_SIGNATURE} FROM {WORKER_ROLE}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {APPLY_SUBSCRIPTION_SIGNATURE} TO {RUNTIME_ROLE}")
    _leave_platform_role()


def downgrade() -> None:
    _enter_platform_role()
    op.execute(f"DROP FUNCTION IF EXISTS {APPLY_SUBSCRIPTION_SIGNATURE}")
    op.execute(f"DROP FUNCTION IF EXISTS {APPLY_TRANSACTION_SIGNATURE}")
    _replace_registration_function(allow_paddle=False)
    _leave_platform_role()

    op.drop_index(
        "uq_us_lacey_subscriptions_provider_last_transaction",
        table_name="us_lacey_subscriptions",
    )
    op.drop_column("us_lacey_subscriptions", "provider_last_event_sha256")
    op.drop_column("us_lacey_subscriptions", "provider_last_event_id")
    op.drop_column("us_lacey_subscriptions", "provider_last_transaction_id")

    op.drop_constraint(
        "ck_us_lacey_subscriptions_billing_provider",
        "us_lacey_subscriptions",
        type_="check",
    )
    op.create_check_constraint(
        "ck_us_lacey_subscriptions_billing_provider",
        "us_lacey_subscriptions",
        "billing_provider IN ('NONE','MANUAL','LEMON_SQUEEZY','STRIPE')",
    )

    op.drop_constraint(
        "ck_us_lacey_payments_provider",
        "us_lacey_payments",
        type_="check",
    )
    op.create_check_constraint(
        "ck_us_lacey_payments_provider",
        "us_lacey_payments",
        "provider IN ('MANUAL_BANK_TRANSFER','WISE','STRIPE','LEMON_SQUEEZY')",
    )
