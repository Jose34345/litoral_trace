"""Add product-led five-shipment evaluation and four-hour raw retention.

Revision ID: 076_us_lacey_product_led_evaluation
Revises: 075_us_lacey_identity_and_product_bridge
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "076_us_lacey_product_led_evaluation"
down_revision = "075_us_lacey_identity_and_product_bridge"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = (
    "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
)

CLAIM_FUNCTION = "public.us_lacey_evaluation_claim(text,text)"
TOUCH_FUNCTION = "public.us_lacey_evaluation_touch(text,integer)"
COUNT_FUNCTION = "public.us_lacey_evaluation_mark_operation_success(integer,integer)"
PRE_SANDBOX_EVENT_FUNCTION = (
    "public.us_lacey_outreach_record_pre_sandbox_event(uuid,text,text,jsonb)"
)
RAW_CLAIM_FUNCTION = "public.us_lacey_evaluation_raw_purge_claim(text)"
RAW_COMPLETE_FUNCTION = (
    "public.us_lacey_evaluation_raw_purge_complete(bigint,text)"
)
RAW_RETRY_FUNCTION = (
    "public.us_lacey_evaluation_raw_purge_retry(bigint,text,text,integer)"
)
RAW_OVERDUE_FUNCTION = (
    "public.us_lacey_evaluation_raw_purge_overdue(integer)"
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


def _secure_tenant_read_table(table: str) -> None:
    predicate = f"organization_id = {TENANT_CONTEXT_SQL}"
    op.execute(f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY {table}_tenant_select ON public.{table} "
        f"FOR SELECT TO {RUNTIME_ROLE} USING ({predicate})"
    )
    op.execute(
        f"CREATE POLICY {table}_platform_all ON public.{table} "
        f"FOR ALL TO {PLATFORM_ROLE} USING (true) WITH CHECK (true)"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE public.{table} "
        "FROM PUBLIC, anon, authenticated"
    )
    op.execute(
        f"GRANT SELECT ON TABLE public.{table} TO {RUNTIME_ROLE}"
    )


def upgrade() -> None:
    op.create_table(
        "us_lacey_evaluations",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column(
            "status",
            sa.String(24),
            nullable=False,
            server_default="ANONYMOUS",
        ),
        sa.Column("work_email", sa.String(255), nullable=True),
        sa.Column(
            "operation_limit",
            sa.Integer(),
            nullable=False,
            server_default="5",
        ),
        sa.Column(
            "successful_operations_used",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "last_activity_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "inactive_expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "raw_retention_hours",
            sa.Integer(),
            nullable=False,
            server_default="4",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_us_lacey_evaluations_organization",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "organization_id",
            name="uq_us_lacey_evaluations_organization",
        ),
        sa.CheckConstraint(
            "status IN ('ANONYMOUS','ACTIVE','EXHAUSTED','EXPIRED')",
            name="ck_us_lacey_evaluations_status",
        ),
        sa.CheckConstraint(
            "operation_limit = 5",
            name="ck_us_lacey_evaluations_operation_limit",
        ),
        sa.CheckConstraint(
            "successful_operations_used >= 0 "
            "AND successful_operations_used <= operation_limit",
            name="ck_us_lacey_evaluations_usage",
        ),
        sa.CheckConstraint(
            "raw_retention_hours = 4",
            name="ck_us_lacey_evaluations_raw_retention",
        ),
        sa.CheckConstraint(
            "(status = 'ANONYMOUS' AND work_email IS NULL "
            "AND claimed_at IS NULL AND inactive_expires_at IS NULL) "
            "OR "
            "(status IN ('ACTIVE','EXHAUSTED') "
            "AND work_email IS NOT NULL AND claimed_at IS NOT NULL "
            "AND inactive_expires_at IS NOT NULL) "
            "OR status = 'EXPIRED'",
            name="ck_us_lacey_evaluations_claim_state",
        ),
    )
    op.create_index(
        "ix_us_lacey_evaluations_status_expiry",
        "us_lacey_evaluations",
        ["status", "inactive_expires_at"],
    )

    op.create_table(
        "us_lacey_evaluation_operations",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False),
        sa.Column(
            "counted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_us_lacey_evaluation_operations_operation_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "operation_id",
            name="uq_us_lacey_evaluation_operations_operation",
        ),
    )
    op.create_index(
        "ix_us_lacey_evaluation_operations_org_counted",
        "us_lacey_evaluation_operations",
        ["organization_id", "counted_at"],
    )

    op.create_table(
        "us_lacey_evaluation_raw_purge_jobs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("vault_document_id", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "state",
            sa.String(24),
            nullable=False,
            server_default="PENDING",
        ),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("locked_by", sa.String(255), nullable=True),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(2000), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["vault_document_id", "organization_id"],
            ["vault_documents.id", "vault_documents.organization_id"],
            name="fk_us_lacey_eval_raw_purge_vault_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "vault_document_id",
            name="uq_us_lacey_eval_raw_purge_document",
        ),
        sa.CheckConstraint(
            "state IN ('PENDING','RUNNING','RETRY','COMPLETED','FAILED')",
            name="ck_us_lacey_eval_raw_purge_state",
        ),
        sa.CheckConstraint(
            "attempt_count >= 0",
            name="ck_us_lacey_eval_raw_purge_attempt_count",
        ),
    )
    op.create_index(
        "ix_us_lacey_eval_raw_purge_claim",
        "us_lacey_evaluation_raw_purge_jobs",
        ["state", "available_at", "expires_at", "id"],
        postgresql_where=sa.text("state IN ('PENDING','RETRY')"),
    )

    _secure_tenant_read_table("us_lacey_evaluations")
    _secure_tenant_read_table("us_lacey_evaluation_operations")

    op.execute(
        "ALTER TABLE public.us_lacey_evaluation_raw_purge_jobs "
        "ENABLE ROW LEVEL SECURITY"
    )
    op.execute(
        "ALTER TABLE public.us_lacey_evaluation_raw_purge_jobs "
        "FORCE ROW LEVEL SECURITY"
    )
    op.execute(
        "CREATE POLICY us_lacey_evaluation_raw_purge_jobs_platform_all "
        "ON public.us_lacey_evaluation_raw_purge_jobs "
        f"FOR ALL TO {PLATFORM_ROLE} USING (true) WITH CHECK (true)"
    )
    op.execute(
        "REVOKE ALL PRIVILEGES ON TABLE "
        "public.us_lacey_evaluation_raw_purge_jobs "
        "FROM PUBLIC, anon, authenticated, litoral_trace_app, "
        "litoral_trace_worker_executor"
    )

    # EVALUATION is intentionally zero-cost and card-free.
    op.drop_constraint(
        "ck_us_lacey_subscriptions_price_positive",
        "us_lacey_subscriptions",
        type_="check",
    )
    op.create_check_constraint(
        "ck_us_lacey_subscriptions_price_positive",
        "us_lacey_subscriptions",
        "(plan_code IN ('SANDBOX','EVALUATION') AND price_cents = 0) "
        "OR (plan_code NOT IN ('SANDBOX','EVALUATION') AND price_cents > 0)",
    )

    # Extend first-party funnel vocabulary for product-led activation.
    op.drop_constraint(
        "ck_us_lacey_outreach_event_name",
        "us_lacey_outreach_events",
        type_="check",
    )
    op.create_check_constraint(
        "ck_us_lacey_outreach_event_name",
        "us_lacey_outreach_events",
        "event_name IN ("
        "'LINK_OPENED','SAMPLE_STARTED','SAMPLE_REUSE_REACHED',"
        "'SAMPLE_COMPLETED','SANDBOX_STARTED','OWN_SHIPMENT_STARTED',"
        "'OPERATION_CREATED','DOCUMENTS_UPLOADED','OWN_SHIPMENT_PROCESSED',"
        "'EVALUATION_CLAIMED','EVALUATION_OPERATION_2','EVIDENCE_REUSED',"
        "'REVIEW_REACHED','AUTO_RESOLVED_CONFIRMED','REVIEW_COMPLETED',"
        "'EXPORT_DOWNLOADED','EVALUATION_EXHAUSTED','UPGRADE_STARTED',"
        "'PQL_QUALIFIED'"
        ")",
    )

    # Backfill currently-live sandbox tenants and their already uploaded raw docs.
    op.execute(
        """
        INSERT INTO public.us_lacey_evaluations (
            organization_id,
            status,
            operation_limit,
            successful_operations_used,
            last_activity_at,
            raw_retention_hours,
            created_at,
            updated_at
        )
        SELECT
            org.id,
            'ANONYMOUS',
            5,
            least(coalesce(sub.used_operations, 0), 5),
            now(),
            4,
            now(),
            now()
        FROM public.organizations AS org
        JOIN public.us_lacey_subscriptions AS sub
          ON sub.organization_id = org.id
        WHERE org.is_sandbox = true
          AND sub.plan_code = 'SANDBOX'
        ON CONFLICT (organization_id) DO NOTHING
        """
    )
    op.execute(
        """
        INSERT INTO public.us_lacey_evaluation_raw_purge_jobs (
            organization_id,
            vault_document_id,
            expires_at,
            state,
            attempt_count,
            available_at,
            created_at,
            updated_at
        )
        SELECT
            vault.organization_id,
            vault.id,
            vault.created_at + interval '4 hours',
            'PENDING',
            0,
            vault.created_at + interval '4 hours',
            now(),
            now()
        FROM public.vault_documents AS vault
        JOIN public.us_lacey_evaluations AS evaluation
          ON evaluation.organization_id = vault.organization_id
        WHERE vault.status <> 'deleted'
        ON CONFLICT (organization_id, vault_document_id) DO NOTHING
        """
    )

    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE "
        "public.us_lacey_evaluations, "
        "public.us_lacey_evaluation_operations, "
        "public.us_lacey_evaluation_raw_purge_jobs "
        f"TO {PLATFORM_ROLE}"
    )
    op.execute(
        "REVOKE ALL PRIVILEGES ON SEQUENCE "
        "public.us_lacey_evaluations_id_seq, "
        "public.us_lacey_evaluation_operations_id_seq, "
        "public.us_lacey_evaluation_raw_purge_jobs_id_seq "
        "FROM PUBLIC, anon, authenticated, litoral_trace_app, "
        "litoral_trace_worker_executor"
    )
    op.execute(
        "GRANT USAGE, SELECT ON SEQUENCE "
        "public.us_lacey_evaluations_id_seq, "
        "public.us_lacey_evaluation_operations_id_seq, "
        "public.us_lacey_evaluation_raw_purge_jobs_id_seq "
        f"TO {PLATFORM_ROLE}"
    )
    op.execute(
        "GRANT SELECT (id, organization_id, email, username, full_name) "
        f"ON TABLE public.users TO {PLATFORM_ROLE}"
    )
    op.execute(
        "GRANT UPDATE (email, username, full_name, updated_at) "
        f"ON TABLE public.users TO {PLATFORM_ROLE}"
    )
    op.execute(
        "GRANT SELECT (organization_id, admin_contact_email, admin_contact_name) "
        f"ON TABLE public.us_lacey_organization_profiles TO {PLATFORM_ROLE}"
    )
    op.execute(
        "GRANT UPDATE (admin_contact_email, admin_contact_name, updated_at) "
        f"ON TABLE public.us_lacey_organization_profiles TO {PLATFORM_ROLE}"
    )
    op.execute(
        "GRANT SELECT, UPDATE ON TABLE public.us_lacey_subscriptions "
        f"TO {PLATFORM_ROLE}"
    )
    op.execute(
        "GRANT SELECT, UPDATE ON TABLE public.user_sessions "
        f"TO {PLATFORM_ROLE}"
    )
    op.execute(
        "GRANT SELECT, UPDATE ON TABLE public.organizations "
        f"TO {PLATFORM_ROLE}"
    )
    op.execute(
        "GRANT SELECT, UPDATE ON TABLE public.us_lacey_sandbox_purge_jobs "
        f"TO {PLATFORM_ROLE}"
    )
    op.execute(
        "GRANT SELECT, INSERT ON TABLE public.us_lacey_outreach_events "
        f"TO {PLATFORM_ROLE}"
    )
    op.execute(
        "GRANT SELECT, UPDATE ON TABLE public.us_lacey_outreach_sessions "
        f"TO {PLATFORM_ROLE}"
    )
    op.execute(
        "GRANT SELECT ON TABLE public.us_lacey_operations, "
        "public.vault_documents TO "
        f"{PLATFORM_ROLE}"
    )
    op.execute(
        "GRANT TRIGGER ON TABLE public.organizations, "
        "public.vault_documents, public.us_lacey_operations "
        f"TO {PLATFORM_ROLE}"
    )
    op.execute(
        "GRANT UPDATE (status, deleted_at, updated_at) "
        f"ON TABLE public.vault_documents TO {PLATFORM_ROLE}"
    )
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")

    # Every future zero-touch sandbox receives an evaluation state immediately.
    op.execute(
        """
        CREATE FUNCTION public.us_lacey_evaluation_from_sandbox()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        BEGIN
            IF NEW.is_sandbox IS TRUE THEN
                INSERT INTO public.us_lacey_evaluations (
                    organization_id,
                    status,
                    operation_limit,
                    successful_operations_used,
                    last_activity_at,
                    raw_retention_hours,
                    created_at,
                    updated_at
                ) VALUES (
                    NEW.id,
                    'ANONYMOUS',
                    5,
                    0,
                    now(),
                    4,
                    now(),
                    now()
                )
                ON CONFLICT (organization_id) DO NOTHING;
            END IF;
            RETURN NEW;
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_us_lacey_evaluation_from_sandbox
        AFTER INSERT ON public.organizations
        FOR EACH ROW
        WHEN (NEW.is_sandbox IS TRUE)
        EXECUTE FUNCTION public.us_lacey_evaluation_from_sandbox()
        """
    )

    # Raw object deletion is always scheduled four hours from its own upload time.
    op.execute(
        """
        CREATE FUNCTION public.us_lacey_evaluation_schedule_raw_purge()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM public.us_lacey_evaluations AS evaluation
                WHERE evaluation.organization_id = NEW.organization_id
            ) THEN
                INSERT INTO public.us_lacey_evaluation_raw_purge_jobs (
                    organization_id,
                    vault_document_id,
                    expires_at,
                    state,
                    attempt_count,
                    available_at,
                    created_at,
                    updated_at
                ) VALUES (
                    NEW.organization_id,
                    NEW.id,
                    NEW.created_at + interval '4 hours',
                    'PENDING',
                    0,
                    NEW.created_at + interval '4 hours',
                    now(),
                    now()
                )
                ON CONFLICT (organization_id, vault_document_id)
                DO NOTHING;
            END IF;
            RETURN NEW;
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_us_lacey_evaluation_schedule_raw_purge
        AFTER INSERT ON public.vault_documents
        FOR EACH ROW
        EXECUTE FUNCTION public.us_lacey_evaluation_schedule_raw_purge()
        """
    )


    op.execute(
        """
        CREATE FUNCTION public.us_lacey_evaluation_claim(
            requested_token_hash text,
            requested_work_email text
        )
        RETURNS TABLE(
            organization_id integer,
            user_id integer,
            evaluation_status text,
            successful_operations_used integer,
            operation_limit integer,
            inactive_expires_at timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            actor record;
            evaluation record;
            purge_job record;
            normalized_email text;
            email_domain text;
            new_expires_at timestamptz;
        BEGIN
            normalized_email := lower(btrim(coalesce(requested_work_email, '')));
            IF normalized_email = ''
               OR char_length(normalized_email) > 255
               OR normalized_email !~ '^[^[:space:]@]+@[^[:space:]@]+[.][^[:space:]@]+$' THEN
                RAISE EXCEPTION 'enter a valid work email'
                    USING ERRCODE = '22023';
            END IF;

            email_domain := split_part(normalized_email, '@', 2);
            IF email_domain = ANY (
                ARRAY[
                    'gmail.com','googlemail.com','yahoo.com','yahoo.co.uk',
                    'hotmail.com','outlook.com','live.com','msn.com',
                    'icloud.com','me.com','mac.com','aol.com',
                    'proton.me','protonmail.com','gmx.com','mail.com',
                    'yandex.com','zoho.com'
                ]::text[]
            ) THEN
                RAISE EXCEPTION 'use your work email to save this evaluation'
                    USING ERRCODE = '22023';
            END IF;

            SELECT
                session.user_id,
                session.organization_id
            INTO actor
            FROM public.user_sessions AS session
            JOIN public.users AS usr
              ON usr.id = session.user_id
             AND usr.organization_id = session.organization_id
            JOIN public.organizations AS org
              ON org.id = session.organization_id
            WHERE session.token_hash = requested_token_hash
              AND session.revoked_at IS NULL
              AND session.expires_at > now()
              AND usr.is_active = true
              AND org.is_active = true
            FOR UPDATE OF session, usr, org;

            IF NOT FOUND THEN
                RAISE EXCEPTION 'evaluation session is invalid or expired'
                    USING ERRCODE = '42501';
            END IF;

            SELECT
                e.status,
                e.successful_operations_used,
                e.operation_limit,
                e.claimed_at
            INTO evaluation
            FROM public.us_lacey_evaluations AS e
            WHERE e.organization_id = actor.organization_id
            FOR UPDATE;

            IF NOT FOUND THEN
                RAISE EXCEPTION 'evaluation is unavailable'
                    USING ERRCODE = '55000';
            END IF;

            IF evaluation.status <> 'ANONYMOUS' THEN
                RAISE EXCEPTION 'evaluation has already been claimed'
                    USING ERRCODE = '55000';
            END IF;

            IF evaluation.successful_operations_used < 1 THEN
                RAISE EXCEPTION 'process one shipment before saving the evaluation'
                    USING ERRCODE = '55000';
            END IF;

            IF NOT EXISTS (
                SELECT 1
                FROM public.organizations AS org
                WHERE org.id = actor.organization_id
                  AND org.is_sandbox = true
                  AND org.sandbox_expires_at > now()
            ) THEN
                RAISE EXCEPTION 'sandbox is no longer available'
                    USING ERRCODE = '55000';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM public.users AS other_user
                WHERE lower(other_user.email) = normalized_email
                  AND other_user.id <> actor.user_id
            ) THEN
                RAISE EXCEPTION 'an account already exists for this work email'
                    USING ERRCODE = '23505';
            END IF;

            SELECT job.id, job.state
            INTO purge_job
            FROM public.us_lacey_sandbox_purge_jobs AS job
            WHERE job.organization_id = actor.organization_id
            FOR UPDATE;

            IF NOT FOUND OR purge_job.state NOT IN ('PENDING','RETRY') THEN
                RAISE EXCEPTION 'sandbox retention can no longer be extended'
                    USING ERRCODE = '55000';
            END IF;

            new_expires_at := now() + interval '7 days';

            UPDATE public.users
            SET
                email = normalized_email,
                username = normalized_email,
                full_name = normalized_email,
                updated_at = now()
            WHERE id = actor.user_id
              AND organization_id = actor.organization_id;

            UPDATE public.us_lacey_organization_profiles
            SET
                admin_contact_email = normalized_email,
                admin_contact_name = normalized_email,
                updated_at = now()
            WHERE organization_id = actor.organization_id;

            UPDATE public.us_lacey_evaluations
            SET
                status = CASE
                    WHEN successful_operations_used >= operation_limit
                    THEN 'EXHAUSTED'
                    ELSE 'ACTIVE'
                END,
                work_email = normalized_email,
                claimed_at = now(),
                last_activity_at = now(),
                inactive_expires_at = new_expires_at,
                updated_at = now()
            WHERE organization_id = actor.organization_id;

            UPDATE public.us_lacey_subscriptions
            SET
                plan_code = 'EVALUATION',
                price_cents = 0,
                monthly_operation_limit = 5,
                used_operations = evaluation.successful_operations_used,
                status = 'ACTIVE',
                renews_at = new_expires_at,
                updated_at = now()
            WHERE organization_id = actor.organization_id;

            UPDATE public.organizations
            SET
                sandbox_expires_at = new_expires_at,
                support_debug_consent = false,
                debug_retention_until = NULL,
                updated_at = now()
            WHERE id = actor.organization_id;

            UPDATE public.user_sessions
            SET expires_at = new_expires_at, updated_at = now()
            WHERE token_hash = requested_token_hash
              AND organization_id = actor.organization_id
              AND user_id = actor.user_id
              AND revoked_at IS NULL;

            UPDATE public.us_lacey_sandbox_purge_jobs
            SET
                expires_at = new_expires_at,
                available_at = new_expires_at,
                state = 'PENDING',
                locked_by = NULL,
                locked_at = NULL,
                heartbeat_at = NULL,
                last_error = NULL,
                last_error_at = NULL,
                updated_at = now()
            WHERE id = purge_job.id
              AND organization_id = actor.organization_id
              AND state IN ('PENDING','RETRY');

            RETURN QUERY
            SELECT
                actor.organization_id,
                actor.user_id,
                e.status::text,
                e.successful_operations_used,
                e.operation_limit,
                e.inactive_expires_at
            FROM public.us_lacey_evaluations AS e
            WHERE e.organization_id = actor.organization_id;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_evaluation_touch(
            requested_token_hash text,
            requested_organization_id integer
        )
        RETURNS TABLE(
            evaluation_status text,
            inactive_expires_at timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            evaluation record;
            new_expires_at timestamptz;
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM public.user_sessions AS session
                WHERE session.token_hash = requested_token_hash
                  AND session.organization_id = requested_organization_id
                  AND session.revoked_at IS NULL
                  AND session.expires_at > now()
            ) THEN
                RAISE EXCEPTION 'evaluation session is invalid or expired'
                    USING ERRCODE = '42501';
            END IF;

            SELECT
                e.status,
                e.last_activity_at,
                e.inactive_expires_at
            INTO evaluation
            FROM public.us_lacey_evaluations AS e
            WHERE e.organization_id = requested_organization_id
            FOR UPDATE;

            IF NOT FOUND OR evaluation.status = 'ANONYMOUS' THEN
                RETURN;
            END IF;

            IF evaluation.status = 'EXPIRED'
               OR evaluation.inactive_expires_at IS NULL
               OR evaluation.inactive_expires_at <= now() THEN
                UPDATE public.us_lacey_evaluations
                SET status = 'EXPIRED', updated_at = now()
                WHERE organization_id = requested_organization_id;
                RAISE EXCEPTION 'evaluation has expired'
                    USING ERRCODE = '42501';
            END IF;

            IF evaluation.status NOT IN ('ACTIVE','EXHAUSTED') THEN
                RETURN;
            END IF;

            IF evaluation.last_activity_at > now() - interval '15 minutes' THEN
                RETURN QUERY
                SELECT
                    e.status::text,
                    e.inactive_expires_at
                FROM public.us_lacey_evaluations AS e
                WHERE e.organization_id = requested_organization_id;
                RETURN;
            END IF;

            new_expires_at := now() + interval '7 days';

            UPDATE public.us_lacey_evaluations
            SET
                last_activity_at = now(),
                inactive_expires_at = new_expires_at,
                updated_at = now()
            WHERE organization_id = requested_organization_id;

            UPDATE public.organizations
            SET
                sandbox_expires_at = new_expires_at,
                updated_at = now()
            WHERE id = requested_organization_id
              AND is_sandbox = true;

            UPDATE public.us_lacey_subscriptions
            SET renews_at = new_expires_at, updated_at = now()
            WHERE organization_id = requested_organization_id
              AND plan_code = 'EVALUATION';

            UPDATE public.user_sessions
            SET expires_at = new_expires_at, updated_at = now()
            WHERE token_hash = requested_token_hash
              AND organization_id = requested_organization_id
              AND revoked_at IS NULL;

            UPDATE public.us_lacey_sandbox_purge_jobs
            SET
                expires_at = new_expires_at,
                available_at = new_expires_at,
                updated_at = now()
            WHERE organization_id = requested_organization_id
              AND state IN ('PENDING','RETRY');

            RETURN QUERY
            SELECT
                e.status::text,
                e.inactive_expires_at
            FROM public.us_lacey_evaluations AS e
            WHERE e.organization_id = requested_organization_id;
        END;
        $$;
        """
    )


    op.execute(
        """
        CREATE FUNCTION public.us_lacey_evaluation_mark_operation_success(
            requested_organization_id integer,
            requested_operation_id integer
        )
        RETURNS TABLE(
            counted boolean,
            successful_operations_used integer,
            evaluation_status text
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            evaluation record;
            current_status text;
            inserted_count integer;
            new_count integer;
            new_status text;
            attribution_session uuid;
        BEGIN
            SELECT
                e.status,
                e.operation_limit,
                e.successful_operations_used
            INTO evaluation
            FROM public.us_lacey_evaluations AS e
            WHERE e.organization_id = requested_organization_id
            FOR UPDATE;

            IF NOT FOUND THEN
                RETURN;
            END IF;

            SELECT upper(coalesce(op.status, ''))
            INTO current_status
            FROM public.us_lacey_operations AS op
            WHERE op.organization_id = requested_organization_id
              AND op.id = requested_operation_id;

            IF NOT FOUND
               OR current_status NOT IN (
                    'READY_FOR_REVIEW',
                    'REVIEW_REQUIRED',
                    'COMPLETED'
               ) THEN
                RETURN QUERY
                SELECT
                    false,
                    evaluation.successful_operations_used,
                    evaluation.status::text;
                RETURN;
            END IF;

            IF EXISTS (
                SELECT 1
                FROM public.us_lacey_evaluation_operations AS counted_op
                WHERE counted_op.organization_id = requested_organization_id
                  AND counted_op.operation_id = requested_operation_id
            ) THEN
                RETURN QUERY
                SELECT
                    false,
                    evaluation.successful_operations_used,
                    evaluation.status::text;
                RETURN;
            END IF;

            IF evaluation.successful_operations_used >= evaluation.operation_limit THEN
                RETURN QUERY
                SELECT
                    false,
                    evaluation.successful_operations_used,
                    evaluation.status::text;
                RETURN;
            END IF;

            INSERT INTO public.us_lacey_evaluation_operations (
                organization_id,
                operation_id,
                counted_at
            ) VALUES (
                requested_organization_id,
                requested_operation_id,
                now()
            )
            ON CONFLICT (organization_id, operation_id) DO NOTHING;

            GET DIAGNOSTICS inserted_count = ROW_COUNT;
            IF inserted_count <> 1 THEN
                RETURN QUERY
                SELECT
                    false,
                    evaluation.successful_operations_used,
                    evaluation.status::text;
                RETURN;
            END IF;

            SELECT count(*)::integer
            INTO new_count
            FROM public.us_lacey_evaluation_operations AS counted_op
            WHERE counted_op.organization_id = requested_organization_id;

            new_count := least(new_count, evaluation.operation_limit);
            new_status := CASE
                WHEN new_count >= evaluation.operation_limit
                     AND evaluation.status IN ('ACTIVE','EXHAUSTED')
                THEN 'EXHAUSTED'
                ELSE evaluation.status
            END;

            UPDATE public.us_lacey_evaluations
            SET
                successful_operations_used = new_count,
                status = new_status,
                last_activity_at = CASE
                    WHEN status IN ('ACTIVE','EXHAUSTED') THEN now()
                    ELSE last_activity_at
                END,
                updated_at = now()
            WHERE organization_id = requested_organization_id;

            UPDATE public.us_lacey_subscriptions
            SET
                used_operations = new_count,
                updated_at = now()
            WHERE organization_id = requested_organization_id
              AND plan_code IN ('SANDBOX','EVALUATION');

            SELECT org.sandbox_attribution_session_id
            INTO attribution_session
            FROM public.organizations AS org
            WHERE org.id = requested_organization_id;

            IF attribution_session IS NOT NULL THEN
                INSERT INTO public.us_lacey_outreach_events (
                    session_id,
                    event_name,
                    event_key,
                    event_metadata,
                    occurred_at
                ) VALUES (
                    attribution_session,
                    'OWN_SHIPMENT_PROCESSED',
                    requested_operation_id::text,
                    jsonb_build_object(
                        'successful_operations_used', new_count,
                        'operation_limit', evaluation.operation_limit
                    ),
                    now()
                )
                ON CONFLICT (
                    session_id,
                    event_name,
                    event_key
                ) DO NOTHING;

                IF new_count = 2 THEN
                    INSERT INTO public.us_lacey_outreach_events (
                        session_id,
                        event_name,
                        event_key,
                        event_metadata,
                        occurred_at
                    ) VALUES (
                        attribution_session,
                        'EVALUATION_OPERATION_2',
                        requested_operation_id::text,
                        jsonb_build_object(
                            'successful_operations_used', new_count
                        ),
                        now()
                    )
                    ON CONFLICT (
                        session_id,
                        event_name,
                        event_key
                    ) DO NOTHING;

                    INSERT INTO public.us_lacey_outreach_events (
                        session_id,
                        event_name,
                        event_key,
                        event_metadata,
                        occurred_at
                    ) VALUES (
                        attribution_session,
                        'PQL_QUALIFIED',
                        'second-successful-operation',
                        jsonb_build_object(
                            'reason', 'second_successful_operation',
                            'successful_operations_used', new_count
                        ),
                        now()
                    )
                    ON CONFLICT (
                        session_id,
                        event_name,
                        event_key
                    ) DO NOTHING;
                END IF;

                IF new_count = evaluation.operation_limit THEN
                    INSERT INTO public.us_lacey_outreach_events (
                        session_id,
                        event_name,
                        event_key,
                        event_metadata,
                        occurred_at
                    ) VALUES (
                        attribution_session,
                        'EVALUATION_EXHAUSTED',
                        'five-successful-operations',
                        jsonb_build_object(
                            'successful_operations_used', new_count,
                            'operation_limit', evaluation.operation_limit
                        ),
                        now()
                    )
                    ON CONFLICT (
                        session_id,
                        event_name,
                        event_key
                    ) DO NOTHING;
                END IF;

                UPDATE public.us_lacey_outreach_sessions
                SET last_event_at = now()
                WHERE id = attribution_session;
            END IF;

            RETURN QUERY
            SELECT true, new_count, new_status;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_evaluation_operation_success_trigger()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        BEGIN
            IF NEW.status IN ('READY_FOR_REVIEW','REVIEW_REQUIRED','COMPLETED')
               AND OLD.status IS DISTINCT FROM NEW.status THEN
                PERFORM *
                FROM public.us_lacey_evaluation_mark_operation_success(
                    NEW.organization_id,
                    NEW.id
                );
            END IF;
            RETURN NEW;
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_us_lacey_evaluation_operation_success
        AFTER UPDATE OF status ON public.us_lacey_operations
        FOR EACH ROW
        WHEN (
            NEW.status IN ('READY_FOR_REVIEW','REVIEW_REQUIRED','COMPLETED')
            AND OLD.status IS DISTINCT FROM NEW.status
        )
        EXECUTE FUNCTION public.us_lacey_evaluation_operation_success_trigger()
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_outreach_record_pre_sandbox_event(
            requested_attribution_session_id uuid,
            requested_event_name text,
            requested_event_key text,
            requested_event_metadata jsonb
        )
        RETURNS boolean
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            normalized_event text;
            normalized_key text;
            normalized_metadata jsonb;
        BEGIN
            normalized_event := upper(btrim(coalesce(requested_event_name, '')));
            normalized_key := left(coalesce(requested_event_key, ''), 128);
            normalized_metadata := coalesce(requested_event_metadata, '{}'::jsonb);

            IF normalized_event NOT IN (
                'SAMPLE_STARTED',
                'SAMPLE_REUSE_REACHED',
                'SAMPLE_COMPLETED'
            ) THEN
                RAISE EXCEPTION 'invalid pre-sandbox outreach event'
                    USING ERRCODE = '22023';
            END IF;

            IF pg_column_size(normalized_metadata) > 4096 THEN
                RAISE EXCEPTION 'outreach event metadata is too large'
                    USING ERRCODE = '22023';
            END IF;

            IF NOT EXISTS (
                SELECT 1
                FROM public.us_lacey_outreach_sessions AS session
                WHERE session.id = requested_attribution_session_id
            ) THEN
                RETURN false;
            END IF;

            INSERT INTO public.us_lacey_outreach_events (
                session_id,
                event_name,
                event_key,
                event_metadata,
                occurred_at
            ) VALUES (
                requested_attribution_session_id,
                normalized_event,
                normalized_key,
                normalized_metadata,
                now()
            )
            ON CONFLICT (
                session_id,
                event_name,
                event_key
            ) DO NOTHING;

            UPDATE public.us_lacey_outreach_sessions
            SET last_event_at = now()
            WHERE id = requested_attribution_session_id;

            RETURN true;
        END;
        $$;
        """
    )


    op.execute(
        """
        CREATE FUNCTION public.us_lacey_evaluation_raw_purge_claim(
            requested_worker_id text
        )
        RETURNS TABLE(
            job_id bigint,
            organization_id integer,
            vault_document_id integer,
            storage_bucket text,
            object_key text,
            storage_version_id text,
            attempt_count integer
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        BEGIN
            IF btrim(coalesce(requested_worker_id, '')) = ''
               OR char_length(requested_worker_id) > 255 THEN
                RAISE EXCEPTION 'invalid evaluation raw cleanup worker'
                    USING ERRCODE = '22023';
            END IF;

            /* Recover a cleanup process that stopped while owning a job. */
            UPDATE public.us_lacey_evaluation_raw_purge_jobs
            SET
                state = 'RETRY',
                available_at = now(),
                locked_by = NULL,
                locked_at = NULL,
                heartbeat_at = NULL,
                last_error = 'Cleanup worker stopped before completing raw purge.',
                updated_at = now()
            WHERE state = 'RUNNING'
              AND coalesce(heartbeat_at, locked_at, updated_at)
                  < now() - interval '15 minutes';

            /* Jobs whose raw object has already been removed are idempotently closed. */
            UPDATE public.us_lacey_evaluation_raw_purge_jobs AS job
            SET
                state = 'COMPLETED',
                completed_at = coalesce(job.completed_at, now()),
                locked_by = NULL,
                locked_at = NULL,
                heartbeat_at = NULL,
                updated_at = now()
            FROM public.vault_documents AS vault
            WHERE vault.id = job.vault_document_id
              AND vault.organization_id = job.organization_id
              AND vault.status = 'deleted'
              AND job.state IN ('PENDING','RETRY');

            RETURN QUERY
            WITH candidate AS (
                SELECT job.id
                FROM public.us_lacey_evaluation_raw_purge_jobs AS job
                JOIN public.us_lacey_evaluations AS evaluation
                  ON evaluation.organization_id = job.organization_id
                JOIN public.vault_documents AS vault
                  ON vault.id = job.vault_document_id
                 AND vault.organization_id = job.organization_id
                WHERE job.state IN ('PENDING','RETRY')
                  AND job.available_at <= now()
                  AND job.expires_at <= now()
                  AND evaluation.status IN ('ANONYMOUS','ACTIVE','EXHAUSTED')
                  AND vault.status <> 'deleted'
                ORDER BY job.expires_at ASC, job.id ASC
                FOR UPDATE OF job SKIP LOCKED
                LIMIT 1
            ),
            claimed AS (
                UPDATE public.us_lacey_evaluation_raw_purge_jobs AS job
                SET
                    state = 'RUNNING',
                    attempt_count = job.attempt_count + 1,
                    locked_by = requested_worker_id,
                    locked_at = now(),
                    heartbeat_at = now(),
                    last_error = NULL,
                    updated_at = now()
                FROM candidate
                WHERE job.id = candidate.id
                RETURNING
                    job.id,
                    job.organization_id,
                    job.vault_document_id,
                    job.attempt_count
            )
            SELECT
                claimed.id,
                claimed.organization_id,
                claimed.vault_document_id,
                vault.storage_bucket::text,
                vault.object_key::text,
                vault.storage_version_id::text,
                claimed.attempt_count
            FROM claimed
            JOIN public.vault_documents AS vault
              ON vault.id = claimed.vault_document_id
             AND vault.organization_id = claimed.organization_id;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_evaluation_raw_purge_complete(
            requested_job_id bigint,
            requested_worker_id text
        )
        RETURNS boolean
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            target_job record;
        BEGIN
            SELECT
                job.organization_id,
                job.vault_document_id
            INTO target_job
            FROM public.us_lacey_evaluation_raw_purge_jobs AS job
            WHERE job.id = requested_job_id
              AND job.state = 'RUNNING'
              AND job.locked_by = requested_worker_id
            FOR UPDATE;

            IF NOT FOUND THEN
                RAISE EXCEPTION 'evaluation raw purge job ownership was lost'
                    USING ERRCODE = '42501';
            END IF;

            UPDATE public.vault_documents
            SET
                status = 'deleted',
                deleted_at = coalesce(deleted_at, now()),
                updated_at = now()
            WHERE id = target_job.vault_document_id
              AND organization_id = target_job.organization_id;

            UPDATE public.us_lacey_evaluation_raw_purge_jobs
            SET
                state = 'COMPLETED',
                completed_at = now(),
                locked_by = NULL,
                locked_at = NULL,
                heartbeat_at = NULL,
                last_error = NULL,
                updated_at = now()
            WHERE id = requested_job_id
              AND state = 'RUNNING'
              AND locked_by = requested_worker_id;

            RETURN true;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_evaluation_raw_purge_retry(
            requested_job_id bigint,
            requested_worker_id text,
            requested_error text,
            requested_delay_seconds integer
        )
        RETURNS text
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            target_job record;
            next_state text;
        BEGIN
            IF requested_delay_seconds < 1
               OR requested_delay_seconds > 86400 THEN
                RAISE EXCEPTION 'invalid raw purge retry delay'
                    USING ERRCODE = '22023';
            END IF;

            SELECT job.attempt_count
            INTO target_job
            FROM public.us_lacey_evaluation_raw_purge_jobs AS job
            WHERE job.id = requested_job_id
              AND job.state = 'RUNNING'
              AND job.locked_by = requested_worker_id
            FOR UPDATE;

            IF NOT FOUND THEN
                RAISE EXCEPTION 'evaluation raw purge job ownership was lost'
                    USING ERRCODE = '42501';
            END IF;

            next_state := CASE
                WHEN target_job.attempt_count >= 5 THEN 'FAILED'
                ELSE 'RETRY'
            END;

            UPDATE public.us_lacey_evaluation_raw_purge_jobs
            SET
                state = next_state,
                available_at = CASE
                    WHEN next_state = 'RETRY'
                    THEN now() + make_interval(secs => requested_delay_seconds)
                    ELSE available_at
                END,
                locked_by = NULL,
                locked_at = NULL,
                heartbeat_at = NULL,
                last_error = left(
                    coalesce(
                        nullif(btrim(requested_error), ''),
                        'Evaluation raw document cleanup failed.'
                    ),
                    2000
                ),
                updated_at = now()
            WHERE id = requested_job_id
              AND state = 'RUNNING'
              AND locked_by = requested_worker_id;

            RETURN next_state;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_evaluation_raw_purge_overdue(
            requested_grace_seconds integer
        )
        RETURNS boolean
        LANGUAGE sql
        STABLE
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
            SELECT EXISTS (
                SELECT 1
                FROM public.us_lacey_evaluation_raw_purge_jobs AS job
                JOIN public.us_lacey_evaluations AS evaluation
                  ON evaluation.organization_id = job.organization_id
                WHERE evaluation.status IN ('ANONYMOUS','ACTIVE','EXHAUSTED')
                  AND job.state NOT IN ('COMPLETED')
                  AND job.expires_at
                      <= now() - make_interval(secs => requested_grace_seconds)
            )
        $$;
        """
    )

    for signature in (
        CLAIM_FUNCTION,
        TOUCH_FUNCTION,
        COUNT_FUNCTION,
        PRE_SANDBOX_EVENT_FUNCTION,
        RAW_CLAIM_FUNCTION,
        RAW_COMPLETE_FUNCTION,
        RAW_RETRY_FUNCTION,
        RAW_OVERDUE_FUNCTION,
        "public.us_lacey_evaluation_from_sandbox()",
        "public.us_lacey_evaluation_schedule_raw_purge()",
        "public.us_lacey_evaluation_operation_success_trigger()",
    ):
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")

    for signature in (
        CLAIM_FUNCTION,
        TOUCH_FUNCTION,
        PRE_SANDBOX_EVENT_FUNCTION,
    ):
        op.execute(
            f"GRANT EXECUTE ON FUNCTION {signature} TO {RUNTIME_ROLE}"
        )

    for signature in (
        COUNT_FUNCTION,
        RAW_CLAIM_FUNCTION,
        RAW_COMPLETE_FUNCTION,
        RAW_RETRY_FUNCTION,
        RAW_OVERDUE_FUNCTION,
    ):
        op.execute(
            f"GRANT EXECUTE ON FUNCTION {signature} TO {WORKER_ROLE}"
        )

    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()


def downgrade() -> None:
    _grant_temp_platform_set()
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")

    op.execute(
        "DROP TRIGGER IF EXISTS trg_us_lacey_evaluation_operation_success "
        "ON public.us_lacey_operations"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_us_lacey_evaluation_schedule_raw_purge "
        "ON public.vault_documents"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_us_lacey_evaluation_from_sandbox "
        "ON public.organizations"
    )

    for signature in (
        RAW_OVERDUE_FUNCTION,
        RAW_RETRY_FUNCTION,
        RAW_COMPLETE_FUNCTION,
        RAW_CLAIM_FUNCTION,
        PRE_SANDBOX_EVENT_FUNCTION,
        COUNT_FUNCTION,
        TOUCH_FUNCTION,
        CLAIM_FUNCTION,
        "public.us_lacey_evaluation_operation_success_trigger()",
        "public.us_lacey_evaluation_schedule_raw_purge()",
        "public.us_lacey_evaluation_from_sandbox()",
    ):
        op.execute(f"DROP FUNCTION IF EXISTS {signature}")

    op.execute("RESET ROLE")
    _revoke_temp_platform_set()

    op.drop_constraint(
        "ck_us_lacey_outreach_event_name",
        "us_lacey_outreach_events",
        type_="check",
    )
    op.create_check_constraint(
        "ck_us_lacey_outreach_event_name",
        "us_lacey_outreach_events",
        "event_name IN ("
        "'LINK_OPENED','SANDBOX_STARTED','OPERATION_CREATED',"
        "'DOCUMENTS_UPLOADED','REVIEW_REACHED','AUTO_RESOLVED_CONFIRMED',"
        "'REVIEW_COMPLETED','EXPORT_DOWNLOADED'"
        ")",
    )

    op.drop_constraint(
        "ck_us_lacey_subscriptions_price_positive",
        "us_lacey_subscriptions",
        type_="check",
    )
    op.create_check_constraint(
        "ck_us_lacey_subscriptions_price_positive",
        "us_lacey_subscriptions",
        "(plan_code = 'SANDBOX' AND price_cents = 0) "
        "OR (plan_code <> 'SANDBOX' AND price_cents > 0)",
    )

    op.drop_index(
        "ix_us_lacey_eval_raw_purge_claim",
        table_name="us_lacey_evaluation_raw_purge_jobs",
    )
    op.drop_table("us_lacey_evaluation_raw_purge_jobs")
    op.drop_index(
        "ix_us_lacey_evaluation_operations_org_counted",
        table_name="us_lacey_evaluation_operations",
    )
    op.drop_table("us_lacey_evaluation_operations")
    op.drop_index(
        "ix_us_lacey_evaluations_status_expiry",
        table_name="us_lacey_evaluations",
    )
    op.drop_table("us_lacey_evaluations")
