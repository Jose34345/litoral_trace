"""Add anonymous U.S. Lacey Learning Plane telemetry.

Revision ID: 057_us_lacey_learning_plane
Revises: 056_us_lacey_sandbox_growth_attribution
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "057_us_lacey_learning_plane"
down_revision = "056_us_lacey_sandbox_growth_attribution"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_ROLE = "litoral_trace_worker_executor"
PRIVACY_TRIGGER_FUNCTION = "public._us_lacey_enforce_telemetry_privacy_opt_in()"
CONSENT_FUNCTION = "public.us_lacey_sandbox_set_learning_consent(text,integer,boolean)"
ADMIN_METRICS_FUNCTION = "public.platform_admin_learning_plane_metrics(text)"


def upgrade() -> None:
    op.add_column(
        "us_lacey_sandbox_purge_jobs",
        sa.Column("learning_opt_in", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_table(
        "us_lacey_telemetry_runs",
        sa.Column("run_id", sa.Uuid(), primary_key=True),
        sa.Column("origin", sa.String(16), nullable=False),
        sa.Column("learning_opt_in", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("document_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("document_type_counts", sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
        sa.Column("processing_total_ms", sa.BigInteger(), nullable=True),
        sa.Column("reviewed_field_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("confirmed_field_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("corrected_field_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rejected_field_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unreviewed_field_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("human_correction_rate", sa.Numeric(6, 5), nullable=True),
        sa.Column("mean_prediction_confidence", sa.Numeric(6, 5), nullable=True),
        sa.Column("engine_version", sa.String(64), nullable=True),
        sa.Column("telemetry_schema_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("finalized_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("origin IN ('SANDBOX','PRODUCTION')", name="ck_us_lacey_telemetry_runs_origin"),
        sa.CheckConstraint("document_count >= 0", name="ck_us_lacey_telemetry_runs_document_count"),
        sa.CheckConstraint("processing_total_ms IS NULL OR processing_total_ms >= 0", name="ck_us_lacey_telemetry_runs_processing_ms"),
        sa.CheckConstraint(
            "reviewed_field_count >= 0 AND confirmed_field_count >= 0 AND corrected_field_count >= 0 "
            "AND rejected_field_count >= 0 AND unreviewed_field_count >= 0",
            name="ck_us_lacey_telemetry_runs_counts_nonnegative",
        ),
        sa.CheckConstraint(
            "reviewed_field_count = confirmed_field_count + corrected_field_count + rejected_field_count",
            name="ck_us_lacey_telemetry_runs_reviewed_consistency",
        ),
        sa.CheckConstraint(
            "human_correction_rate IS NULL OR (human_correction_rate >= 0 AND human_correction_rate <= 1)",
            name="ck_us_lacey_telemetry_runs_correction_rate",
        ),
        sa.CheckConstraint(
            "mean_prediction_confidence IS NULL OR "
            "(mean_prediction_confidence >= 0 AND mean_prediction_confidence <= 1)",
            name="ck_us_lacey_telemetry_runs_confidence",
        ),
    )
    op.create_index(
        "ix_us_lacey_telemetry_runs_origin_created",
        "us_lacey_telemetry_runs",
        ["origin", "created_at"],
    )
    op.create_table(
        "us_lacey_telemetry_field_actions",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "telemetry_run_id",
            sa.Uuid(),
            sa.ForeignKey("us_lacey_telemetry_runs.run_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("field_instance_id", sa.Uuid(), nullable=False),
        sa.Column("field_name", sa.String(128), nullable=False),
        sa.Column("document_type", sa.String(64), nullable=True),
        sa.Column("prediction_confidence", sa.Numeric(6, 5), nullable=True),
        sa.Column("action_taken", sa.String(16), nullable=False),
        sa.Column("deidentified_fragment", sa.Text(), nullable=True),
        sa.Column("target_value", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("telemetry_run_id", "field_instance_id", name="uq_us_lacey_telemetry_field_instance"),
        sa.CheckConstraint(
            "action_taken IN ('CONFIRMED','CORRECTED','REJECTED','UNREVIEWED')",
            name="ck_us_lacey_telemetry_field_action",
        ),
        sa.CheckConstraint(
            "prediction_confidence IS NULL OR (prediction_confidence >= 0 AND prediction_confidence <= 1)",
            name="ck_us_lacey_telemetry_field_confidence",
        ),
        sa.CheckConstraint(
            "action_taken = 'CORRECTED' OR (deidentified_fragment IS NULL AND target_value IS NULL)",
            name="ck_us_lacey_telemetry_corrected_payload",
        ),
    )
    op.create_index("ix_us_lacey_telemetry_field_run", "us_lacey_telemetry_field_actions", ["telemetry_run_id"])
    op.create_index(
        "ix_us_lacey_telemetry_field_document_action",
        "us_lacey_telemetry_field_actions",
        ["document_type", "action_taken"],
    )
    op.create_index(
        "ix_us_lacey_telemetry_field_name_action",
        "us_lacey_telemetry_field_actions",
        ["field_name", "action_taken"],
    )

    for table_name in ("us_lacey_telemetry_runs", "us_lacey_telemetry_field_actions"):
        op.execute(f"REVOKE ALL ON TABLE public.{table_name} FROM PUBLIC")
        op.execute(f"REVOKE ALL ON TABLE public.{table_name} FROM {RUNTIME_ROLE}")
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON TABLE public.us_lacey_telemetry_runs TO {WORKER_ROLE}")
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON TABLE public.us_lacey_telemetry_field_actions TO {WORKER_ROLE}")
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE public.us_lacey_telemetry_field_actions_id_seq TO {WORKER_ROLE}"
    )

    op.execute(
        """
        CREATE FUNCTION public._us_lacey_enforce_telemetry_privacy_opt_in()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE parent_learning_opt_in boolean;
        BEGIN
            SELECT telemetry_run.learning_opt_in
            INTO parent_learning_opt_in
            FROM public.us_lacey_telemetry_runs AS telemetry_run
            WHERE telemetry_run.run_id = NEW.telemetry_run_id;
            IF NOT FOUND THEN
                RETURN NEW;
            END IF;
            IF parent_learning_opt_in IS DISTINCT FROM TRUE
               OR NEW.action_taken <> 'CORRECTED'
               OR NEW.field_name NOT IN (
                    'species','genus','quantity','unit','country_of_harvest','hts_code'
               ) THEN
                NEW.deidentified_fragment := NULL;
                NEW.target_value := NULL;
            END IF;
            RETURN NEW;
        END;
        $$;
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {PRIVACY_TRIGGER_FUNCTION} FROM PUBLIC")
    op.execute(f"REVOKE ALL ON FUNCTION {PRIVACY_TRIGGER_FUNCTION} FROM {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL ON FUNCTION {PRIVACY_TRIGGER_FUNCTION} FROM {WORKER_ROLE}")
    op.execute(
        """
        CREATE TRIGGER trg_enforce_privacy_opt_in
        BEFORE INSERT OR UPDATE ON public.us_lacey_telemetry_field_actions
        FOR EACH ROW EXECUTE FUNCTION public._us_lacey_enforce_telemetry_privacy_opt_in()
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_sandbox_set_learning_consent(
            requested_token_hash text,
            requested_organization_id integer,
            requested_learning_opt_in boolean
        )
        RETURNS boolean
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE updated_jobs integer;
        BEGIN
            IF requested_token_hash IS NULL
               OR requested_token_hash !~ '^[0-9a-f]{64}$'
               OR requested_organization_id IS NULL
               OR requested_organization_id <= 0 THEN
                RAISE EXCEPTION 'invalid sandbox learning consent request'
                    USING ERRCODE = '22023';
            END IF;
            IF NOT EXISTS (
                SELECT 1
                FROM public.user_sessions AS session
                JOIN public.organizations AS organization
                  ON organization.id = session.organization_id
                WHERE session.token_hash = requested_token_hash
                  AND session.organization_id = requested_organization_id
                  AND session.revoked_at IS NULL
                  AND session.expires_at > now()
                  AND organization.is_sandbox = true
                  AND organization.sandbox_expires_at > now()
            ) THEN
                RAISE EXCEPTION 'sandbox session is not eligible'
                    USING ERRCODE = '42501';
            END IF;
            UPDATE public.us_lacey_sandbox_purge_jobs AS job
            SET learning_opt_in = coalesce(requested_learning_opt_in, false),
                updated_at = now()
            WHERE job.organization_id = requested_organization_id
              AND job.state = 'PENDING';
            GET DIAGNOSTICS updated_jobs = ROW_COUNT;
            IF updated_jobs <> 1 THEN
                RAISE EXCEPTION 'sandbox purge job is unavailable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN true;
        END;
        $$;
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {CONSENT_FUNCTION} FROM PUBLIC")
    op.execute(f"REVOKE ALL ON FUNCTION {CONSENT_FUNCTION} FROM {WORKER_ROLE}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {CONSENT_FUNCTION} TO {RUNTIME_ROLE}")

    op.execute(
        """
        CREATE FUNCTION public.platform_admin_learning_plane_metrics(actor_refresh_token_hash text)
        RETURNS TABLE(
            telemetry_run_count bigint,
            sandbox_run_count bigint,
            global_hcr numeric,
            sandbox_avg_processing_ms numeric
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        BEGIN
            PERFORM 1 FROM public._platform_superadmin_session_actor(actor_refresh_token_hash);
            RETURN QUERY
            SELECT
                count(*)::bigint,
                count(*) FILTER (WHERE telemetry.origin = 'SANDBOX')::bigint,
                round(
                    sum(telemetry.corrected_field_count)::numeric
                    / NULLIF(sum(telemetry.reviewed_field_count), 0),
                    5
                ),
                round(
                    avg(telemetry.processing_total_ms::numeric)
                    FILTER (
                        WHERE telemetry.origin = 'SANDBOX'
                          AND telemetry.processing_total_ms IS NOT NULL
                    ),
                    2
                )
            FROM public.us_lacey_telemetry_runs AS telemetry;
        END;
        $$;
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {ADMIN_METRICS_FUNCTION} FROM PUBLIC")
    op.execute(f"REVOKE ALL ON FUNCTION {ADMIN_METRICS_FUNCTION} FROM {WORKER_ROLE}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {ADMIN_METRICS_FUNCTION} TO {RUNTIME_ROLE}")


def downgrade() -> None:
    op.execute(f"REVOKE EXECUTE ON FUNCTION {ADMIN_METRICS_FUNCTION} FROM {RUNTIME_ROLE}")
    op.execute(f"DROP FUNCTION IF EXISTS {ADMIN_METRICS_FUNCTION}")
    op.execute(f"REVOKE EXECUTE ON FUNCTION {CONSENT_FUNCTION} FROM {RUNTIME_ROLE}")
    op.execute(f"DROP FUNCTION IF EXISTS {CONSENT_FUNCTION}")
    op.execute(
        "DROP TRIGGER IF EXISTS trg_enforce_privacy_opt_in "
        "ON public.us_lacey_telemetry_field_actions"
    )
    op.execute(f"DROP FUNCTION IF EXISTS {PRIVACY_TRIGGER_FUNCTION}")
    op.drop_index("ix_us_lacey_telemetry_field_name_action", table_name="us_lacey_telemetry_field_actions")
    op.drop_index("ix_us_lacey_telemetry_field_document_action", table_name="us_lacey_telemetry_field_actions")
    op.drop_index("ix_us_lacey_telemetry_field_run", table_name="us_lacey_telemetry_field_actions")
    op.drop_table("us_lacey_telemetry_field_actions")
    op.drop_index("ix_us_lacey_telemetry_runs_origin_created", table_name="us_lacey_telemetry_runs")
    op.drop_table("us_lacey_telemetry_runs")
    op.drop_column("us_lacey_sandbox_purge_jobs", "learning_opt_in")
