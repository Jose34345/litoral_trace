"""Add signed Lemon Squeezy payment activation for U.S. Lacey.

Revision ID: 041_us_lacey_lemon
Revises: 040_us_lacey_ppq505
Create Date: 2026-09-02 18:20:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "041_us_lacey_lemon"
down_revision: Union[str, Sequence[str], None] = "040_us_lacey_ppq505"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"
REGISTER_SIGNATURE = (
    "public.us_lacey_self_register(text,text,text,text,text,text,integer,integer,text,text,text,text)"
)
REGISTER_LEGACY_SIGNATURE = (
    "public.us_lacey_self_register_legacy(text,text,text,text,text,text,integer,integer,text,text,text,text)"
)
APPLY_SIGNATURE = (
    "public.us_lacey_apply_lemon_order(integer,uuid,text,text,text,integer,text,integer,integer,boolean)"
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
    op.drop_constraint(
        "ck_us_lacey_payments_provider", "us_lacey_payments", type_="check"
    )
    op.create_check_constraint(
        "ck_us_lacey_payments_provider",
        "us_lacey_payments",
        "provider IN ('MANUAL_BANK_TRANSFER','WISE','STRIPE','LEMON_SQUEEZY')",
    )

    op.create_table(
        "us_lacey_payment_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("payment_id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=40), nullable=False),
        sa.Column("provider_order_id", sa.String(length=128), nullable=False),
        sa.Column("event_name", sa.String(length=64), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("amount_cents", sa.Integer(), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("store_id", sa.Integer(), nullable=False),
        sa.Column("variant_id", sa.Integer(), nullable=False),
        sa.Column("test_mode", sa.Boolean(), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["payment_id", "organization_id"],
            ["us_lacey_payments.id", "us_lacey_payments.organization_id"],
            name="fk_us_lacey_payment_events_payment_tenant",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_us_lacey_payment_events"),
        sa.UniqueConstraint(
            "provider", "provider_order_id", name="uq_us_lacey_payment_events_provider_order"
        ),
        sa.CheckConstraint(
            "provider = 'LEMON_SQUEEZY'", name="ck_us_lacey_payment_events_provider"
        ),
        sa.CheckConstraint(
            "event_name = 'order_created'", name="ck_us_lacey_payment_events_name"
        ),
        sa.CheckConstraint(
            "payload_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_us_lacey_payment_events_payload_sha256",
        ),
        sa.CheckConstraint(
            "amount_cents > 0", name="ck_us_lacey_payment_events_amount_positive"
        ),
        sa.CheckConstraint(
            "currency = 'USD'", name="ck_us_lacey_payment_events_currency_usd"
        ),
        sa.CheckConstraint(
            "store_id > 0", name="ck_us_lacey_payment_events_store_positive"
        ),
        sa.CheckConstraint(
            "variant_id > 0", name="ck_us_lacey_payment_events_variant_positive"
        ),
    )
    op.create_index(
        "ix_us_lacey_payment_events_org_payment",
        "us_lacey_payment_events",
        ["organization_id", "payment_id"],
    )

    op.execute("ALTER TABLE public.us_lacey_payment_events ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE public.us_lacey_payment_events FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY us_lacey_payment_events_platform_all "
        "ON public.us_lacey_payment_events FOR ALL "
        f"TO {PLATFORM_ROLE} USING (true) WITH CHECK (true)"
    )
    op.execute("REVOKE ALL ON TABLE public.us_lacey_payment_events FROM PUBLIC")
    op.execute(f"REVOKE ALL ON TABLE public.us_lacey_payment_events FROM {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL ON TABLE public.us_lacey_payment_events FROM {WORKER_ROLE}")
    op.execute(
        f"GRANT SELECT, INSERT ON TABLE public.us_lacey_payment_events TO {PLATFORM_ROLE}"
    )
    op.execute("REVOKE ALL ON SEQUENCE public.us_lacey_payment_events_id_seq FROM PUBLIC")
    op.execute(
        f"REVOKE ALL ON SEQUENCE public.us_lacey_payment_events_id_seq FROM {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE ALL ON SEQUENCE public.us_lacey_payment_events_id_seq FROM {WORKER_ROLE}"
    )
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE public.us_lacey_payment_events_id_seq TO {PLATFORM_ROLE}"
    )

    # The original registration function remains the proven account-creation
    # implementation. Rename it behind a same-signature facade so Python callers
    # do not change and Lemon can be introduced without duplicating that contract.
    _enter_platform_role()
    op.execute(
        "ALTER FUNCTION public.us_lacey_self_register(text,text,text,text,text,text,integer,integer,text,text,text,text) "
        "RENAME TO us_lacey_self_register_legacy"
    )
    op.execute(f"REVOKE ALL ON FUNCTION {REGISTER_LEGACY_SIGNATURE} FROM PUBLIC")
    op.execute(f"REVOKE ALL ON FUNCTION {REGISTER_LEGACY_SIGNATURE} FROM {RUNTIME_ROLE}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {REGISTER_LEGACY_SIGNATURE} TO {PLATFORM_ROLE}")

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
                RAISE EXCEPTION 'invalid initial payment provider' USING ERRCODE = '22023';
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
                    WHEN normalized_provider = 'LEMON_SQUEEZY' THEN 'MANUAL_BANK_TRANSFER'
                    ELSE normalized_provider
                END,
                requested_terms_version,
                requested_privacy_version,
                requested_beta_version
            );

            IF normalized_provider = 'LEMON_SQUEEZY' THEN
                UPDATE public.us_lacey_payments
                SET provider = 'LEMON_SQUEEZY', updated_at = now()
                WHERE organization_id = registration.organization_id
                  AND public_id = registration.payment_public_id
                  AND status = 'PENDING';
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'registered Lemon payment not found' USING ERRCODE = 'P0002';
                END IF;
            END IF;

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
    op.execute(f"GRANT EXECUTE ON FUNCTION {REGISTER_SIGNATURE} TO {RUNTIME_ROLE}")

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_apply_lemon_order(
            target_organization_id integer,
            target_payment_public_id uuid,
            target_provider_order_id text,
            target_event_name text,
            target_payload_sha256 text,
            target_amount_cents integer,
            target_currency text,
            target_store_id integer,
            target_variant_id integer,
            target_test_mode boolean
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
            normalized_order_id text := btrim(coalesce(target_provider_order_id, ''));
            payment_row_id integer;
            payment_subscription_id integer;
            current_payment_status text;
            expected_amount integer;
            expected_currency text;
            existing_event_payment_id integer;
            existing_event_org_id integer;
            existing_event_hash text;
            current_subscription_status text;
            current_account_status text;
        BEGIN
            IF target_organization_id IS NULL OR target_organization_id <= 0
               OR target_payment_public_id IS NULL
               OR normalized_order_id = '' OR char_length(normalized_order_id) > 128
               OR coalesce(target_event_name, '') <> 'order_created'
               OR target_payload_sha256 IS NULL
               OR target_payload_sha256 !~ '^[0-9a-f]{64}$'
               OR target_amount_cents IS NULL OR target_amount_cents <= 0
               OR upper(btrim(coalesce(target_currency, ''))) <> 'USD'
               OR target_store_id IS NULL OR target_store_id <= 0
               OR target_variant_id IS NULL OR target_variant_id <= 0
               OR target_test_mode IS NULL THEN
                RAISE EXCEPTION 'invalid Lemon order activation payload' USING ERRCODE = '22023';
            END IF;

            SELECT p.id, p.subscription_id, p.status, p.amount_cents, p.currency
            INTO payment_row_id, payment_subscription_id, current_payment_status,
                 expected_amount, expected_currency
            FROM public.us_lacey_payments p
            WHERE p.organization_id = target_organization_id
              AND p.public_id = target_payment_public_id
              AND p.provider = 'LEMON_SQUEEZY'
            FOR UPDATE;

            IF payment_row_id IS NULL THEN
                RAISE EXCEPTION 'Lemon payment not found' USING ERRCODE = 'P0002';
            END IF;
            IF expected_amount <> target_amount_cents OR expected_currency <> 'USD' THEN
                RAISE EXCEPTION 'Lemon payment amount mismatch' USING ERRCODE = '22023';
            END IF;

            SELECT e.payment_id, e.organization_id, e.payload_sha256
            INTO existing_event_payment_id, existing_event_org_id, existing_event_hash
            FROM public.us_lacey_payment_events e
            WHERE e.provider = 'LEMON_SQUEEZY'
              AND e.provider_order_id = normalized_order_id;

            IF existing_event_payment_id IS NOT NULL THEN
                IF existing_event_payment_id <> payment_row_id
                   OR existing_event_org_id <> target_organization_id
                   OR existing_event_hash <> target_payload_sha256 THEN
                    RAISE EXCEPTION 'Lemon order idempotency conflict' USING ERRCODE = '23505';
                END IF;
                SELECT s.status INTO current_subscription_status
                FROM public.us_lacey_subscriptions s
                WHERE s.id = payment_subscription_id
                  AND s.organization_id = target_organization_id;
                SELECT p.account_status INTO current_account_status
                FROM public.us_lacey_organization_profiles p
                WHERE p.organization_id = target_organization_id;
                RETURN QUERY SELECT current_payment_status, current_subscription_status,
                                    current_account_status, true;
                RETURN;
            END IF;

            IF current_payment_status <> 'PENDING' THEN
                RAISE EXCEPTION 'Lemon payment is not pending' USING ERRCODE = '22023';
            END IF;

            INSERT INTO public.us_lacey_payment_events (
                organization_id, payment_id, provider, provider_order_id,
                event_name, payload_sha256, amount_cents, currency,
                store_id, variant_id, test_mode, processed_at
            ) VALUES (
                target_organization_id, payment_row_id, 'LEMON_SQUEEZY',
                normalized_order_id, target_event_name, target_payload_sha256,
                target_amount_cents, 'USD', target_store_id, target_variant_id,
                target_test_mode, now()
            );

            UPDATE public.us_lacey_payments
            SET status = 'VERIFIED', paid_at = coalesce(paid_at, now()),
                verified_at = now(), updated_at = now()
            WHERE id = payment_row_id
              AND organization_id = target_organization_id;

            UPDATE public.us_lacey_subscriptions
            SET status = 'ACTIVE', started_at = coalesce(started_at, now()),
                updated_at = now()
            WHERE id = payment_subscription_id
              AND organization_id = target_organization_id;

            UPDATE public.us_lacey_organization_profiles
            SET account_status = 'ACTIVE', updated_at = now()
            WHERE organization_id = target_organization_id
              AND account_status IN ('PAYMENT_PENDING','PILOT');

            SELECT s.status INTO current_subscription_status
            FROM public.us_lacey_subscriptions s
            WHERE s.id = payment_subscription_id
              AND s.organization_id = target_organization_id;
            SELECT p.account_status INTO current_account_status
            FROM public.us_lacey_organization_profiles p
            WHERE p.organization_id = target_organization_id;

            RETURN QUERY SELECT 'VERIFIED'::text, current_subscription_status,
                                current_account_status, false;
        END;
        $$;
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {APPLY_SIGNATURE} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {APPLY_SIGNATURE} TO {RUNTIME_ROLE}")
    _leave_platform_role()


def downgrade() -> None:
    _enter_platform_role()
    op.execute(f"DROP FUNCTION IF EXISTS {APPLY_SIGNATURE}")
    op.execute(f"DROP FUNCTION IF EXISTS {REGISTER_SIGNATURE}")
    op.execute(
        "ALTER FUNCTION public.us_lacey_self_register_legacy(text,text,text,text,text,text,integer,integer,text,text,text,text) "
        "RENAME TO us_lacey_self_register"
    )
    op.execute(f"REVOKE ALL ON FUNCTION {REGISTER_SIGNATURE} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {REGISTER_SIGNATURE} TO {RUNTIME_ROLE}")
    _leave_platform_role()

    op.drop_index(
        "ix_us_lacey_payment_events_org_payment", table_name="us_lacey_payment_events"
    )
    op.drop_table("us_lacey_payment_events")

    op.drop_constraint(
        "ck_us_lacey_payments_provider", "us_lacey_payments", type_="check"
    )
    op.create_check_constraint(
        "ck_us_lacey_payments_provider",
        "us_lacey_payments",
        "provider IN ('MANUAL_BANK_TRANSFER','WISE','STRIPE')",
    )
