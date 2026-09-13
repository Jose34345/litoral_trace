"""Add U.S. Lacey self-service billing, legal acceptance and durable processing jobs.

Revision ID: 035_us_lacey_self_service
Revises: 034_us_lacey_pilot_core
Create Date: 2026-08-30 15:30:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "035_us_lacey_self_service"
down_revision: Union[str, Sequence[str], None] = "034_us_lacey_pilot_core"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"

TENANT_TABLES = (
    "us_lacey_subscriptions",
    "us_lacey_payments",
    "us_lacey_terms_acceptances",
    "us_lacey_processing_jobs",
)


def _enable_tenant_rls(table_name: str) -> None:
    op.execute(f"ALTER TABLE public.{table_name} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table_name} FORCE ROW LEVEL SECURITY")
    for command in ("SELECT", "INSERT", "UPDATE", "DELETE"):
        policy = f"{table_name}_tenant_{command.lower()}"
        predicate = "organization_id = NULLIF(current_setting('app.current_organization_id', true), '')::integer"
        op.execute(f"DROP POLICY IF EXISTS {policy} ON public.{table_name}")
        if command == "INSERT":
            op.execute(
                f"CREATE POLICY {policy} ON public.{table_name} FOR INSERT TO {RUNTIME_ROLE} "
                f"WITH CHECK ({predicate})"
            )
        elif command == "UPDATE":
            op.execute(
                f"CREATE POLICY {policy} ON public.{table_name} FOR UPDATE TO {RUNTIME_ROLE} "
                f"USING ({predicate}) WITH CHECK ({predicate})"
            )
        else:
            op.execute(
                f"CREATE POLICY {policy} ON public.{table_name} FOR {command} TO {RUNTIME_ROLE} "
                f"USING ({predicate})"
            )

    # The dedicated non-login platform definer is the only cross-tenant control plane.
    op.execute(
        f"CREATE POLICY {table_name}_platform_all ON public.{table_name} "
        f"FOR ALL TO {PLATFORM_ROLE} USING (true) WITH CHECK (true)"
    )
    op.execute(f"REVOKE ALL ON TABLE public.{table_name} FROM PUBLIC")
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.{table_name} TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.{table_name} TO {PLATFORM_ROLE}"
    )


def _grant_sequences() -> None:
    for table_name in TENANT_TABLES + ("us_lacey_email_verifications",):
        sequence_name = f"public.{table_name}_id_seq"
        op.execute(f"GRANT USAGE, SELECT ON SEQUENCE {sequence_name} TO {PLATFORM_ROLE}")
        if table_name in TENANT_TABLES:
            op.execute(f"GRANT USAGE, SELECT ON SEQUENCE {sequence_name} TO {RUNTIME_ROLE}")


def _grant_platform_profile_access() -> None:
    op.execute(
        "CREATE POLICY us_lacey_org_profiles_platform_all "
        "ON public.us_lacey_organization_profiles FOR ALL "
        f"TO {PLATFORM_ROLE} USING (true) WITH CHECK (true)"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE ON TABLE public.us_lacey_organization_profiles "
        f"TO {PLATFORM_ROLE}"
    )
    op.execute(
        "GRANT USAGE, SELECT ON SEQUENCE public.us_lacey_organization_profiles_id_seq "
        f"TO {PLATFORM_ROLE}"
    )


def _grant_temp_platform_set() -> None:
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )


def _revoke_temp_platform_set() -> None:
    op.execute(f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER")


def _create_self_service_functions() -> None:
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
            normalized_email text := lower(btrim(coalesce(requested_admin_email, '')));
            normalized_name text := btrim(coalesce(requested_legal_name, ''));
            normalized_business_type text := upper(btrim(coalesce(requested_business_type, 'OTHER')));
            normalized_provider text := upper(btrim(coalesce(requested_payment_provider, 'MANUAL_BANK_TRANSFER')));
            new_org_id integer;
            new_user_id integer;
            new_subscription_id integer;
            new_payment_id integer;
            new_payment_public_id uuid := gen_random_uuid();
            new_payment_reference text := 'LT-US-' || upper(substr(replace(gen_random_uuid()::text, '-', ''), 1, 12));
        BEGIN
            IF normalized_name = '' OR char_length(normalized_name) > 255 THEN
                RAISE EXCEPTION 'invalid legal name' USING ERRCODE = '22023';
            END IF;
            IF normalized_email = '' OR char_length(normalized_email) > 255 OR position('@' in normalized_email) <= 1 THEN
                RAISE EXCEPTION 'invalid email' USING ERRCODE = '22023';
            END IF;
            IF normalized_business_type NOT IN ('IMPORTER','CUSTOMS_BROKER','OTHER') THEN
                RAISE EXCEPTION 'invalid business type' USING ERRCODE = '22023';
            END IF;
            IF normalized_provider NOT IN ('MANUAL_BANK_TRANSFER','WISE') THEN
                RAISE EXCEPTION 'invalid initial payment provider' USING ERRCODE = '22023';
            END IF;
            IF requested_password_hash IS NULL OR char_length(btrim(requested_password_hash)) < 40 THEN
                RAISE EXCEPTION 'invalid password hash' USING ERRCODE = '22023';
            END IF;
            IF requested_verification_token_hash IS NULL OR requested_verification_token_hash !~ '^[0-9a-f]{64}$' THEN
                RAISE EXCEPTION 'invalid verification token hash' USING ERRCODE = '22023';
            END IF;
            IF requested_price_cents IS NULL OR requested_price_cents <= 0 THEN
                RAISE EXCEPTION 'price must be positive' USING ERRCODE = '22023';
            END IF;
            IF requested_monthly_operation_limit IS NULL OR requested_monthly_operation_limit <= 0 THEN
                RAISE EXCEPTION 'operation limit must be positive' USING ERRCODE = '22023';
            END IF;
            IF btrim(coalesce(requested_terms_version, '')) = ''
               OR btrim(coalesce(requested_privacy_version, '')) = ''
               OR btrim(coalesce(requested_beta_version, '')) = '' THEN
                RAISE EXCEPTION 'legal document versions are required' USING ERRCODE = '22023';
            END IF;

            INSERT INTO public.organizations (
                name, slug, tax_id, tier, description, is_active, created_at, updated_at
            ) VALUES (
                normalized_name,
                'us-' || substr(replace(gen_random_uuid()::text, '-', ''), 1, 20),
                NULL,
                'private_beta',
                'U.S. Lacey self-service account',
                TRUE,
                now(), now()
            ) RETURNING id INTO new_org_id;

            INSERT INTO public.users (
                organization_id, email, username, password_hash, role, full_name,
                is_active, created_at, updated_at
            ) VALUES (
                new_org_id, normalized_email, normalized_email, requested_password_hash,
                'admin', NULLIF(btrim(coalesce(requested_admin_name, '')), ''),
                TRUE, now(), now()
            ) RETURNING id INTO new_user_id;

            INSERT INTO public.us_lacey_organization_profiles (
                organization_id, legal_name, country_code, business_type,
                admin_contact_name, admin_contact_email, billing_email,
                account_status, created_at, updated_at
            ) VALUES (
                new_org_id, normalized_name, 'US', normalized_business_type,
                NULLIF(btrim(coalesce(requested_admin_name, '')), ''),
                normalized_email, normalized_email, 'PENDING_EMAIL', now(), now()
            );

            INSERT INTO public.us_lacey_subscriptions (
                public_id, organization_id, plan_code, currency, price_cents,
                monthly_operation_limit, used_operations, status, created_at, updated_at
            ) VALUES (
                gen_random_uuid(), new_org_id, 'PRIVATE_BETA', 'USD', requested_price_cents,
                requested_monthly_operation_limit, 0, 'PENDING', now(), now()
            ) RETURNING id INTO new_subscription_id;

            INSERT INTO public.us_lacey_payments (
                public_id, organization_id, subscription_id, provider, amount_cents,
                currency, payment_reference, status, created_at, updated_at
            ) VALUES (
                new_payment_public_id, new_org_id, new_subscription_id, normalized_provider,
                requested_price_cents, 'USD', new_payment_reference, 'PENDING', now(), now()
            ) RETURNING id INTO new_payment_id;

            INSERT INTO public.us_lacey_terms_acceptances (
                organization_id, user_id, document_type, document_version, accepted_at
            ) VALUES
                (new_org_id, new_user_id, 'TERMS', btrim(requested_terms_version), now()),
                (new_org_id, new_user_id, 'PRIVACY', btrim(requested_privacy_version), now()),
                (new_org_id, new_user_id, 'PRIVATE_BETA', btrim(requested_beta_version), now());

            INSERT INTO public.us_lacey_email_verifications (
                organization_id, user_id, token_hash, expires_at, created_at
            ) VALUES (
                new_org_id, new_user_id, requested_verification_token_hash,
                now() + interval '24 hours', now()
            );

            RETURN QUERY SELECT
                new_org_id,
                new_user_id,
                new_payment_public_id,
                new_payment_reference,
                requested_price_cents,
                'PENDING_EMAIL'::text;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_verify_email(requested_token_hash text)
        RETURNS TABLE (organization_id integer, user_id integer, account_status text)
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            verification_id integer;
            target_org_id integer;
            target_user_id integer;
        BEGIN
            SELECT id, us_lacey_email_verifications.organization_id, us_lacey_email_verifications.user_id
            INTO verification_id, target_org_id, target_user_id
            FROM public.us_lacey_email_verifications
            WHERE token_hash = requested_token_hash
              AND used_at IS NULL
              AND expires_at > now()
            FOR UPDATE;

            IF verification_id IS NULL THEN
                RAISE EXCEPTION 'verification token invalid or expired' USING ERRCODE = '22023';
            END IF;

            UPDATE public.us_lacey_email_verifications
            SET used_at = now()
            WHERE id = verification_id;

            UPDATE public.us_lacey_organization_profiles
            SET account_status = 'PAYMENT_PENDING', updated_at = now()
            WHERE us_lacey_organization_profiles.organization_id = target_org_id
              AND account_status = 'PENDING_EMAIL';

            RETURN QUERY SELECT target_org_id, target_user_id, 'PAYMENT_PENDING'::text;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_verify_payment(
            actor_refresh_token_hash text,
            target_organization_id integer,
            target_payment_public_id uuid
        )
        RETURNS TABLE (payment_status text, subscription_status text, account_status text)
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            payment_subscription_id integer;
        BEGIN
            PERFORM 1 FROM public._platform_superadmin_session_actor(actor_refresh_token_hash);

            SELECT subscription_id INTO payment_subscription_id
            FROM public.us_lacey_payments
            WHERE organization_id = target_organization_id
              AND public_id = target_payment_public_id
              AND status = 'PENDING'
            FOR UPDATE;

            IF payment_subscription_id IS NULL THEN
                RAISE EXCEPTION 'pending payment not found' USING ERRCODE = 'P0002';
            END IF;

            UPDATE public.us_lacey_payments
            SET status = 'VERIFIED', paid_at = coalesce(paid_at, now()),
                verified_at = now(), updated_at = now()
            WHERE organization_id = target_organization_id
              AND public_id = target_payment_public_id;

            UPDATE public.us_lacey_subscriptions
            SET status = 'ACTIVE', started_at = coalesce(started_at, now()), updated_at = now()
            WHERE organization_id = target_organization_id
              AND id = payment_subscription_id;

            UPDATE public.us_lacey_organization_profiles
            SET account_status = 'ACTIVE', updated_at = now()
            WHERE organization_id = target_organization_id
              AND account_status IN ('PAYMENT_PENDING','PILOT');

            RETURN QUERY SELECT 'VERIFIED'::text, 'ACTIVE'::text, 'ACTIVE'::text;
        END;
        $$;
        """
    )

    signatures = (
        "public.us_lacey_self_register(text,text,text,text,text,text,integer,integer,text,text,text,text)",
        "public.us_lacey_verify_email(text)",
        "public.us_lacey_verify_payment(text,integer,uuid)",
    )
    for signature in signatures:
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")
        op.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {RUNTIME_ROLE}")

    # ALTER FUNCTION ... OWNER requires both SET ROLE capability and CREATE on
    # the containing schema for the target owner. Mirror the hardened handoff
    # already proven by migration 028, and revoke both temporary capabilities
    # before this transaction can complete.
    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    for signature in signatures:
        op.execute(f"ALTER FUNCTION {signature} OWNER TO {PLATFORM_ROLE}")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()


def upgrade() -> None:
    op.drop_constraint(
        "ck_us_lacey_org_profiles_status",
        "us_lacey_organization_profiles",
        type_="check",
    )
    op.create_check_constraint(
        "ck_us_lacey_org_profiles_status",
        "us_lacey_organization_profiles",
        "account_status IN ('PENDING_EMAIL','PAYMENT_PENDING','PILOT','ACTIVE','SUSPENDED')",
    )
    op.alter_column(
        "us_lacey_organization_profiles",
        "account_status",
        server_default=sa.text("'PENDING_EMAIL'"),
        existing_type=sa.String(length=24),
        existing_nullable=False,
    )

    op.create_table(
        "us_lacey_subscriptions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("plan_code", sa.String(64), nullable=False, server_default="PRIVATE_BETA"),
        sa.Column("currency", sa.String(3), nullable=False, server_default="USD"),
        sa.Column("price_cents", sa.Integer(), nullable=False),
        sa.Column("monthly_operation_limit", sa.Integer(), nullable=False),
        sa.Column("used_operations", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("status", sa.String(24), nullable=False, server_default="PENDING"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("renews_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("public_id", name="uq_us_lacey_subscriptions_public_id"),
        sa.UniqueConstraint("organization_id", name="uq_us_lacey_subscriptions_org"),
        sa.UniqueConstraint("id", "organization_id", name="uq_us_lacey_subscriptions_id_org"),
        sa.CheckConstraint("currency = 'USD'", name="ck_us_lacey_subscriptions_currency_usd"),
        sa.CheckConstraint("price_cents > 0", name="ck_us_lacey_subscriptions_price_positive"),
        sa.CheckConstraint("monthly_operation_limit > 0", name="ck_us_lacey_subscriptions_limit_positive"),
        sa.CheckConstraint("used_operations >= 0", name="ck_us_lacey_subscriptions_usage_nonnegative"),
        sa.CheckConstraint("status IN ('PENDING','ACTIVE','PAST_DUE','CANCELED')", name="ck_us_lacey_subscriptions_status"),
    )
    op.create_index("ix_us_lacey_subscriptions_org_status", "us_lacey_subscriptions", ["organization_id", "status"])

    op.create_table(
        "us_lacey_payments",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("subscription_id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(40), nullable=False, server_default="MANUAL_BANK_TRANSFER"),
        sa.Column("amount_cents", sa.Integer(), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False, server_default="USD"),
        sa.Column("payment_reference", sa.String(64), nullable=False),
        sa.Column("customer_reference", sa.String(255), nullable=True),
        sa.Column("status", sa.String(24), nullable=False, server_default="PENDING"),
        sa.Column("paid_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("verified_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["verified_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["subscription_id", "organization_id"],
            ["us_lacey_subscriptions.id", "us_lacey_subscriptions.organization_id"],
            name="fk_us_lacey_payments_subscription_tenant", ondelete="CASCADE",
        ),
        sa.UniqueConstraint("public_id", name="uq_us_lacey_payments_public_id"),
        sa.UniqueConstraint("payment_reference", name="uq_us_lacey_payments_reference"),
        sa.UniqueConstraint("id", "organization_id", name="uq_us_lacey_payments_id_org"),
        sa.CheckConstraint("amount_cents > 0", name="ck_us_lacey_payments_amount_positive"),
        sa.CheckConstraint("currency = 'USD'", name="ck_us_lacey_payments_currency_usd"),
        sa.CheckConstraint("provider IN ('MANUAL_BANK_TRANSFER','WISE','STRIPE')", name="ck_us_lacey_payments_provider"),
        sa.CheckConstraint("status IN ('PENDING','VERIFIED','REJECTED','REFUNDED')", name="ck_us_lacey_payments_status"),
    )
    op.create_index("ix_us_lacey_payments_org_status", "us_lacey_payments", ["organization_id", "status"])
    op.create_index("ix_us_lacey_payments_reference", "us_lacey_payments", ["payment_reference"])

    op.create_table(
        "us_lacey_terms_acceptances",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("document_type", sa.String(32), nullable=False),
        sa.Column("document_version", sa.String(64), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("organization_id", "user_id", "document_type", "document_version", name="uq_us_lacey_terms_acceptance_version"),
        sa.CheckConstraint("document_type IN ('TERMS','PRIVACY','PRIVATE_BETA')", name="ck_us_lacey_terms_acceptance_type"),
    )
    op.create_index("ix_us_lacey_terms_acceptances_org", "us_lacey_terms_acceptances", ["organization_id"])

    op.create_table(
        "us_lacey_processing_jobs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False),
        sa.Column("assurance_document_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="QUEUED"),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("locked_by", sa.String(255), nullable=True),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_code", sa.String(100), nullable=True),
        sa.Column("last_error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_us_lacey_processing_jobs_operation_tenant", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_us_lacey_processing_jobs_document_tenant", ondelete="CASCADE",
        ),
        sa.UniqueConstraint("public_id", name="uq_us_lacey_processing_jobs_public_id"),
        sa.UniqueConstraint("organization_id", "operation_id", "assurance_document_id", name="uq_us_lacey_processing_jobs_document_once"),
        sa.UniqueConstraint("id", "organization_id", name="uq_us_lacey_processing_jobs_id_org"),
        sa.CheckConstraint("status IN ('QUEUED','RUNNING','RETRY','COMPLETED','FAILED')", name="ck_us_lacey_processing_jobs_status"),
        sa.CheckConstraint("attempt_count >= 0", name="ck_us_lacey_processing_jobs_attempt_nonnegative"),
        sa.CheckConstraint("max_attempts > 0", name="ck_us_lacey_processing_jobs_max_attempts_positive"),
    )
    op.create_index("ix_us_lacey_processing_jobs_queue", "us_lacey_processing_jobs", ["status", "available_at", "created_at"])
    op.create_index("ix_us_lacey_processing_jobs_org_status", "us_lacey_processing_jobs", ["organization_id", "status"])

    # Verification tokens are never directly readable by the web runtime.
    op.create_table(
        "us_lacey_email_verifications",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("token_hash", sa.String(64), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("token_hash", name="uq_us_lacey_email_verifications_token"),
    )
    op.create_index("ix_us_lacey_email_verifications_expiry", "us_lacey_email_verifications", ["expires_at"])
    op.execute("REVOKE ALL ON TABLE public.us_lacey_email_verifications FROM PUBLIC")
    op.execute(f"REVOKE ALL ON TABLE public.us_lacey_email_verifications FROM {RUNTIME_ROLE}")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.us_lacey_email_verifications TO {PLATFORM_ROLE}")

    for table_name in TENANT_TABLES:
        _enable_tenant_rls(table_name)
    _grant_sequences()
    _grant_platform_profile_access()

    # Worker may inspect/claim queue metadata globally, but still cannot read customer
    # operations, Assurance documents or Vault bytes through this grant.
    op.execute(f"GRANT SELECT, UPDATE ON TABLE public.us_lacey_processing_jobs TO {WORKER_ROLE}")
    op.execute(
        "CREATE POLICY us_lacey_processing_jobs_worker_all ON public.us_lacey_processing_jobs "
        f"FOR ALL TO {WORKER_ROLE} USING (true) WITH CHECK (true)"
    )

    _create_self_service_functions()


def downgrade() -> None:
    signatures = (
        "public.us_lacey_verify_payment(text,integer,uuid)",
        "public.us_lacey_verify_email(text)",
        "public.us_lacey_self_register(text,text,text,text,text,text,integer,integer,text,text,text,text)",
    )
    _grant_temp_platform_set()
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")
    for signature in signatures:
        op.execute(f"DROP FUNCTION IF EXISTS {signature}")
    op.execute("RESET ROLE")
    _revoke_temp_platform_set()

    op.execute("DROP POLICY IF EXISTS us_lacey_org_profiles_platform_all ON public.us_lacey_organization_profiles")
    op.execute(
        f"REVOKE SELECT, INSERT, UPDATE ON TABLE public.us_lacey_organization_profiles FROM {PLATFORM_ROLE}"
    )
    op.execute(
        f"REVOKE USAGE, SELECT ON SEQUENCE public.us_lacey_organization_profiles_id_seq FROM {PLATFORM_ROLE}"
    )
    op.execute("DROP POLICY IF EXISTS us_lacey_processing_jobs_worker_all ON public.us_lacey_processing_jobs")
    for table_name in reversed(TENANT_TABLES):
        op.drop_table(table_name)
    op.drop_table("us_lacey_email_verifications")

    op.drop_constraint("ck_us_lacey_org_profiles_status", "us_lacey_organization_profiles", type_="check")
    op.create_check_constraint(
        "ck_us_lacey_org_profiles_status",
        "us_lacey_organization_profiles",
        "account_status IN ('PILOT','ACTIVE','SUSPENDED')",
    )
    op.alter_column(
        "us_lacey_organization_profiles",
        "account_status",
        server_default=sa.text("'PILOT'"),
        existing_type=sa.String(length=24),
        existing_nullable=False,
    )
