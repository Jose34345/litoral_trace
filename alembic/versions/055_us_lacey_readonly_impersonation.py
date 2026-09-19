"""Add zero-trust read-only U.S. Lacey admin impersonation.

Revision ID: 055_us_lacey_readonly_impersonation
Revises: 054_us_lacey_control_plane_audit_billing
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "055_us_lacey_readonly_impersonation"
down_revision: Union[str, Sequence[str], None] = (
    "054_us_lacey_control_plane_audit_billing"
)
branch_labels = None
depends_on = None


RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"
IMPERSONATION_ROLE = "litoral_trace_impersonation_reader"

TENANT_CONTEXT_SQL = (
    "current_setting('app.current_organization_id', true)::integer"
)


IMPERSONATION_TABLES = (
    "us_lacey_operations",
    "us_lacey_operation_documents",
    "us_lacey_operation_fields",
    "us_lacey_field_candidates",
    "us_lacey_ppq_shipments",
    "us_lacey_ppq_plant_lines",
    "us_lacey_plant_declarations",
    "us_lacey_processing_jobs",
    "assurance_documents",
    "document_extraction_runs",
    "extracted_document_fields",
    "reconciliation_issues",
    "us_lacey_engine_document_runs",
    "us_lacey_engine_shipment_runs",
    "us_lacey_evidence_snapshots",
    "us_lacey_evidence_snapshot_documents",
    "us_lacey_source_set_revisions",
    "us_lacey_source_set_members",
    "document_text_spans",
    "document_text_translations",
    "semantic_evidence_nodes",
    "semantic_snapshot_nodes",
    "semantic_evidence_edges",
    "us_lacey_product_intelligence_snapshots",
    "us_lacey_regulatory_assessment_snapshots",
)


VAULT_SAFE_COLUMNS = (
    "id",
    "public_id",
    "organization_id",
    "original_filename",
    "content_type",
    "size_bytes",
    "sha256",
    "document_type",
    "status",
    "last_error_code",
    "deleted_at",
    "created_at",
    "updated_at",
)

VAULT_FORBIDDEN_COLUMNS = (
    "created_by_user_id",
    "object_key",
    "storage_backend",
    "storage_bucket",
    "storage_etag",
    "storage_version_id",
    "idempotency_key",
    "last_error_message",
)


SENSITIVE_TABLES = (
    "organizations",
    "users",
    "user_sessions",
    "us_lacey_organization_profiles",
    "us_lacey_subscriptions",
    "us_lacey_payments",
    "us_lacey_payment_events",
    "us_lacey_terms_acceptances",
    "us_lacey_email_verifications",
    "audit_logs",
    "us_lacey_admin_audit_logs",
    "us_lacey_admin_impersonation_sessions",
    "us_lacey_sandbox_purge_jobs",
)


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
REVOKE_SESSIONS_FUNCTION = (
    "public.platform_admin_revoke_user_sessions(text,integer)"
)
RESET_FUNCTION = "public.platform_admin_reset_pilot_account(text,integer)"
ADMIN_USERS_FUNCTION = "public.platform_admin_users(text)"
FAILED_JOBS_FUNCTION = "public.platform_admin_failed_jobs(text)"
ACCOUNT_OVERVIEW_FUNCTION = "public.platform_us_lacey_account_overview(text)"


DENIED_CONTROL_PLANE_FUNCTIONS = (
    AUDIT_FUNCTION,
    PROMOTE_FUNCTION,
    STATUS_FUNCTION,
    LIMIT_FUNCTION,
    REVOKE_SESSIONS_FUNCTION,
    RESET_FUNCTION,
    ADMIN_USERS_FUNCTION,
    FAILED_JOBS_FUNCTION,
    ACCOUNT_OVERVIEW_FUNCTION,
)


START_FUNCTION = (
    "public.platform_admin_start_readonly_impersonation("
    "text,integer,text,text)"
)
RESOLVE_FUNCTION = (
    "public.platform_admin_resolve_readonly_impersonation(text,text)"
)
END_FUNCTION = (
    "public.platform_admin_end_readonly_impersonation(text,text)"
)

IMPERSONATION_CONTROL_FUNCTIONS = (
    START_FUNCTION,
    RESOLVE_FUNCTION,
    END_FUNCTION,
)


def _grant_temp_platform_set() -> None:
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE "
        "GRANTED BY CURRENT_USER"
    )


def _revoke_temp_platform_set() -> None:
    op.execute(
        f"REVOKE {PLATFORM_ROLE} "
        "FROM CURRENT_USER GRANTED BY CURRENT_USER"
    )


def _enter_platform_role() -> None:
    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET ROLE {PLATFORM_ROLE}")


def _leave_platform_role() -> None:
    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()


def _create_and_harden_reader_role() -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_roles
                WHERE rolname = '{IMPERSONATION_ROLE}'
            ) THEN
                CREATE ROLE {IMPERSONATION_ROLE}
                    NOLOGIN
                    NOINHERIT
                    NOBYPASSRLS
                    NOSUPERUSER
                    NOCREATEDB
                    NOCREATEROLE
                    NOREPLICATION;
            ELSE
                ALTER ROLE {IMPERSONATION_ROLE}
                    NOLOGIN
                    NOINHERIT
                    NOBYPASSRLS
                    NOSUPERUSER
                    NOCREATEDB
                    NOCREATEROLE
                    NOREPLICATION;
            END IF;
        END
        $$;
        """
    )

    op.execute(
        f"""
        DO $$
        DECLARE
            membership record;
        BEGIN
            FOR membership IN
                SELECT parent.rolname AS parent_role
                FROM pg_auth_members AS membership_map
                JOIN pg_roles AS parent
                  ON parent.oid = membership_map.roleid
                JOIN pg_roles AS child
                  ON child.oid = membership_map.member
                WHERE child.rolname = '{IMPERSONATION_ROLE}'
            LOOP
                EXECUTE format(
                    'REVOKE %I FROM %I',
                    membership.parent_role,
                    '{IMPERSONATION_ROLE}'
                );
            END LOOP;
        END
        $$;
        """
    )

    op.execute(
        f"""
        REVOKE ALL PRIVILEGES
        ON ALL TABLES IN SCHEMA public
        FROM {IMPERSONATION_ROLE}
        """
    )
    op.execute(
        f"""
        REVOKE ALL PRIVILEGES
        ON ALL SEQUENCES IN SCHEMA public
        FROM {IMPERSONATION_ROLE}
        """
    )
    op.execute(
        f"""
        REVOKE ALL PRIVILEGES
        ON ALL FUNCTIONS IN SCHEMA public
        FROM {IMPERSONATION_ROLE}
        """
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON SCHEMA public FROM {IMPERSONATION_ROLE}"
    )
    op.execute(
        f"GRANT USAGE ON SCHEMA public TO {IMPERSONATION_ROLE}"
    )


def _create_impersonation_reader_policies() -> None:
    for table in IMPERSONATION_TABLES:
        op.execute(
            f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY"
        )
        op.execute(
            f"ALTER TABLE public.{table} FORCE ROW LEVEL SECURITY"
        )

        # PostgreSQL requires at least one applicable permissive policy.
        # The paired permissive + restrictive predicates are identical, so
        # effective visibility remains fail-closed and tenant-bound.
        op.execute(
            f"""
            CREATE POLICY {table}_impersonation_allow
            ON public.{table}
            AS PERMISSIVE
            FOR SELECT
            TO {IMPERSONATION_ROLE}
            USING (
                organization_id = {TENANT_CONTEXT_SQL}
            )
            """
        )
        op.execute(
            f"""
            CREATE POLICY {table}_impersonation_read
            ON public.{table}
            AS RESTRICTIVE
            FOR SELECT
            TO {IMPERSONATION_ROLE}
            USING (
                organization_id = {TENANT_CONTEXT_SQL}
            )
            """
        )

        op.execute(
            f"GRANT SELECT ON TABLE public.{table} TO {IMPERSONATION_ROLE}"
        )


def _create_vault_metadata_policy() -> None:
    op.execute(
        "ALTER TABLE public.vault_documents ENABLE ROW LEVEL SECURITY"
    )
    op.execute(
        "ALTER TABLE public.vault_documents FORCE ROW LEVEL SECURITY"
    )

    op.execute(
        f"""
        CREATE POLICY vault_documents_impersonation_allow
        ON public.vault_documents
        AS PERMISSIVE
        FOR SELECT
        TO {IMPERSONATION_ROLE}
        USING (
            organization_id = {TENANT_CONTEXT_SQL}
        )
        """
    )
    op.execute(
        f"""
        CREATE POLICY vault_documents_impersonation_read
        ON public.vault_documents
        AS RESTRICTIVE
        FOR SELECT
        TO {IMPERSONATION_ROLE}
        USING (
            organization_id = {TENANT_CONTEXT_SQL}
        )
        """
    )

    safe_columns = ", ".join(VAULT_SAFE_COLUMNS)
    op.execute(
        f"""
        GRANT SELECT ({safe_columns})
        ON TABLE public.vault_documents
        TO {IMPERSONATION_ROLE}
        """
    )


def _create_impersonation_sessions_table() -> None:
    op.create_table(
        "us_lacey_admin_impersonation_sessions",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("admin_user_id", sa.Integer(), nullable=False),
        sa.Column("admin_organization_id", sa.Integer(), nullable=False),
        sa.Column("target_organization_id", sa.Integer(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column(
            "expires_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "revoked_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "token_hash",
            name="uq_us_lacey_impersonation_token_hash",
        ),
        sa.CheckConstraint(
            "token_hash ~ '^[0-9a-f]{64}$'",
            name="ck_us_lacey_impersonation_token_sha256",
        ),
        sa.CheckConstraint(
            "char_length(btrim(reason)) BETWEEN 10 AND 1000",
            name="ck_us_lacey_impersonation_reason_length",
        ),
        sa.CheckConstraint(
            "expires_at = created_at + interval '15 minutes'",
            name="ck_us_lacey_impersonation_hard_ttl",
        ),
        sa.CheckConstraint(
            "revoked_at IS NULL OR revoked_at >= created_at",
            name="ck_us_lacey_impersonation_revoked_after_creation",
        ),
    )

    op.create_index(
        "ix_us_lacey_impersonation_admin_created",
        "us_lacey_admin_impersonation_sessions",
        ["admin_user_id", "created_at"],
    )
    op.create_index(
        "ix_us_lacey_impersonation_target_created",
        "us_lacey_admin_impersonation_sessions",
        ["target_organization_id", "created_at"],
    )
    op.create_index(
        "ix_us_lacey_impersonation_expiry_revoked",
        "us_lacey_admin_impersonation_sessions",
        ["expires_at", "revoked_at"],
    )

    op.execute(
        """
        ALTER TABLE public.us_lacey_admin_impersonation_sessions
        ENABLE ROW LEVEL SECURITY
        """
    )
    op.execute(
        """
        ALTER TABLE public.us_lacey_admin_impersonation_sessions
        FORCE ROW LEVEL SECURITY
        """
    )

    op.execute(
        f"""
        CREATE POLICY us_lacey_impersonation_sessions_platform_select
        ON public.us_lacey_admin_impersonation_sessions
        FOR SELECT
        TO {PLATFORM_ROLE}
        USING (true)
        """
    )
    op.execute(
        f"""
        CREATE POLICY us_lacey_impersonation_sessions_platform_insert
        ON public.us_lacey_admin_impersonation_sessions
        FOR INSERT
        TO {PLATFORM_ROLE}
        WITH CHECK (true)
        """
    )
    op.execute(
        f"""
        CREATE POLICY us_lacey_impersonation_sessions_platform_update
        ON public.us_lacey_admin_impersonation_sessions
        FOR UPDATE
        TO {PLATFORM_ROLE}
        USING (true)
        WITH CHECK (true)
        """
    )

    for role in (
        "PUBLIC",
        RUNTIME_ROLE,
        WORKER_ROLE,
        IMPERSONATION_ROLE,
        PLATFORM_ROLE,
    ):
        op.execute(
            "REVOKE ALL PRIVILEGES "
            "ON TABLE public.us_lacey_admin_impersonation_sessions "
            f"FROM {role}"
        )

    op.execute(
        f"""
        GRANT SELECT, INSERT
        ON TABLE public.us_lacey_admin_impersonation_sessions
        TO {PLATFORM_ROLE}
        """
    )
    op.execute(
        f"""
        GRANT UPDATE (revoked_at)
        ON TABLE public.us_lacey_admin_impersonation_sessions
        TO {PLATFORM_ROLE}
        """
    )

    for role in (
        "PUBLIC",
        RUNTIME_ROLE,
        WORKER_ROLE,
        IMPERSONATION_ROLE,
        PLATFORM_ROLE,
    ):
        op.execute(
            "REVOKE ALL PRIVILEGES "
            "ON SEQUENCE "
            "public.us_lacey_admin_impersonation_sessions_id_seq "
            f"FROM {role}"
        )

    op.execute(
        f"""
        GRANT USAGE, SELECT
        ON SEQUENCE
        public.us_lacey_admin_impersonation_sessions_id_seq
        TO {PLATFORM_ROLE}
        """
    )


def _create_control_plane_functions() -> None:
    op.execute(
        """
        CREATE FUNCTION public.platform_admin_start_readonly_impersonation(
            actor_refresh_token_hash text,
            requested_target_organization_id integer,
            requested_reason text,
            requested_token_hash text
        )
        RETURNS TABLE(
            session_id bigint,
            expires_at timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            actor record;
            issued_at timestamptz;
            normalized_reason text;
            normalized_token_hash text;
            created_session_id bigint;
        BEGIN
            SELECT *
            INTO actor
            FROM public._platform_superadmin_session_actor(
                actor_refresh_token_hash
            );

            IF requested_target_organization_id IS NULL
               OR requested_target_organization_id <= 0 THEN
                RAISE EXCEPTION 'invalid target organization'
                    USING ERRCODE = '22023';
            END IF;

            IF NOT EXISTS (
                SELECT 1
                FROM public.us_lacey_organization_profiles AS profile
                WHERE profile.organization_id =
                      requested_target_organization_id
            ) THEN
                RAISE EXCEPTION 'U.S. Lacey account not found'
                    USING ERRCODE = '22023';
            END IF;

            normalized_reason :=
                btrim(coalesce(requested_reason, ''));

            IF char_length(normalized_reason) < 10
               OR char_length(normalized_reason) > 1000 THEN
                RAISE EXCEPTION
                    'impersonation reason must be 10-1000 characters'
                    USING ERRCODE = '22023';
            END IF;

            normalized_token_hash :=
                lower(btrim(coalesce(requested_token_hash, '')));

            IF normalized_token_hash !~ '^[0-9a-f]{64}$' THEN
                RAISE EXCEPTION 'invalid impersonation token hash'
                    USING ERRCODE = '22023';
            END IF;

            issued_at := statement_timestamp();

            INSERT INTO public.us_lacey_admin_impersonation_sessions (
                token_hash,
                admin_user_id,
                admin_organization_id,
                target_organization_id,
                reason,
                expires_at,
                created_at
            )
            VALUES (
                normalized_token_hash,
                actor.actor_user_id,
                actor.actor_organization_id,
                requested_target_organization_id,
                normalized_reason,
                issued_at + interval '15 minutes',
                issued_at
            )
            RETURNING id
            INTO created_session_id;

            PERFORM public._us_lacey_admin_audit(
                actor.actor_user_id,
                actor.actor_organization_id,
                'READONLY_IMPERSONATION_STARTED',
                requested_target_organization_id,
                NULL,
                '{}'::jsonb,
                jsonb_build_object(
                    'session_id',
                    created_session_id,
                    'expires_at',
                    issued_at + interval '15 minutes'
                ),
                NULL,
                jsonb_build_object(
                    'reason',
                    normalized_reason
                )
            );

            RETURN QUERY
            SELECT
                created_session_id,
                issued_at + interval '15 minutes';
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.platform_admin_resolve_readonly_impersonation(
            actor_refresh_token_hash text,
            requested_token_hash text
        )
        RETURNS TABLE(
            session_id bigint,
            target_organization_id integer
        )
        LANGUAGE plpgsql
        STABLE
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            actor record;
            target record;
            normalized_token_hash text;
        BEGIN
            SELECT *
            INTO actor
            FROM public._platform_superadmin_session_actor(
                actor_refresh_token_hash
            );

            normalized_token_hash :=
                lower(btrim(coalesce(requested_token_hash, '')));

            IF normalized_token_hash !~ '^[0-9a-f]{64}$' THEN
                RAISE EXCEPTION 'invalid impersonation token hash'
                    USING ERRCODE = '22023';
            END IF;

            SELECT
                session.id AS resolved_session_id,
                session.target_organization_id AS resolved_target_org
            INTO target
            FROM public.us_lacey_admin_impersonation_sessions AS session
            WHERE session.token_hash = normalized_token_hash
              AND session.admin_user_id = actor.actor_user_id
              AND session.admin_organization_id =
                  actor.actor_organization_id
              AND session.revoked_at IS NULL
              AND session.expires_at > statement_timestamp()
            LIMIT 1;

            IF target.resolved_session_id IS NULL THEN
                RAISE EXCEPTION
                    'impersonation session is invalid or expired'
                    USING ERRCODE = '42501';
            END IF;

            RETURN QUERY
            SELECT
                target.resolved_session_id::bigint,
                target.resolved_target_org::integer;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.platform_admin_end_readonly_impersonation(
            actor_refresh_token_hash text,
            requested_token_hash text
        )
        RETURNS void
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            actor record;
            target record;
            normalized_token_hash text;
            ended_at timestamptz;
        BEGIN
            SELECT *
            INTO actor
            FROM public._platform_superadmin_session_actor(
                actor_refresh_token_hash
            );

            normalized_token_hash :=
                lower(btrim(coalesce(requested_token_hash, '')));

            IF normalized_token_hash !~ '^[0-9a-f]{64}$' THEN
                RAISE EXCEPTION 'invalid impersonation token hash'
                    USING ERRCODE = '22023';
            END IF;

            SELECT
                session.id AS resolved_session_id,
                session.target_organization_id AS resolved_target_org,
                session.expires_at AS resolved_expires_at,
                session.revoked_at AS resolved_revoked_at
            INTO target
            FROM public.us_lacey_admin_impersonation_sessions AS session
            WHERE session.token_hash = normalized_token_hash
              AND session.admin_user_id = actor.actor_user_id
              AND session.admin_organization_id =
                  actor.actor_organization_id
            FOR UPDATE;

            IF target.resolved_session_id IS NULL THEN
                RAISE EXCEPTION
                    'impersonation session not found'
                    USING ERRCODE = '42501';
            END IF;

            IF target.resolved_revoked_at IS NOT NULL THEN
                RETURN;
            END IF;

            ended_at := statement_timestamp();

            UPDATE public.us_lacey_admin_impersonation_sessions
            SET revoked_at = ended_at
            WHERE id = target.resolved_session_id;

            PERFORM public._us_lacey_admin_audit(
                actor.actor_user_id,
                actor.actor_organization_id,
                'READONLY_IMPERSONATION_ENDED',
                target.resolved_target_org,
                NULL,
                jsonb_build_object(
                    'session_id',
                    target.resolved_session_id,
                    'revoked_at',
                    NULL,
                    'expires_at',
                    target.resolved_expires_at
                ),
                jsonb_build_object(
                    'session_id',
                    target.resolved_session_id,
                    'revoked_at',
                    ended_at,
                    'expires_at',
                    target.resolved_expires_at
                ),
                NULL,
                '{}'::jsonb
            );
        END;
        $$;
        """
    )


def _lock_down_control_plane_functions() -> None:
    for signature in DENIED_CONTROL_PLANE_FUNCTIONS:
        op.execute(
            f"REVOKE EXECUTE ON FUNCTION {signature} "
            f"FROM {IMPERSONATION_ROLE}"
        )

    for signature in IMPERSONATION_CONTROL_FUNCTIONS:
        for role in (
            "PUBLIC",
            IMPERSONATION_ROLE,
            WORKER_ROLE,
            PLATFORM_ROLE,
        ):
            op.execute(
                f"REVOKE EXECUTE ON FUNCTION {signature} FROM {role}"
            )
        op.execute(
            f"GRANT EXECUTE ON FUNCTION {signature} TO {RUNTIME_ROLE}"
        )


def _assert_reader_is_locked_down() -> None:
    allowed_tables_array = ", ".join(
        f"'{table}'" for table in IMPERSONATION_TABLES
    )
    sensitive_tables_array = ", ".join(
        f"'{table}'" for table in SENSITIVE_TABLES
    )
    denied_functions_array = ", ".join(
        f"'{signature}'" for signature in DENIED_CONTROL_PLANE_FUNCTIONS
    )
    forbidden_vault_columns_array = ", ".join(
        f"'{column}'" for column in VAULT_FORBIDDEN_COLUMNS
    )

    op.execute(
        f"""
        DO $$
        DECLARE
            role_record record;
            object_name text;
        BEGIN
            SELECT
                rolcanlogin,
                rolinherit,
                rolsuper,
                rolcreaterole,
                rolcreatedb,
                rolreplication,
                rolbypassrls
            INTO role_record
            FROM pg_roles
            WHERE rolname = '{IMPERSONATION_ROLE}';

            IF role_record IS NULL THEN
                RAISE EXCEPTION
                    'impersonation reader role is missing';
            END IF;

            IF role_record.rolcanlogin
               OR role_record.rolinherit
               OR role_record.rolsuper
               OR role_record.rolcreaterole
               OR role_record.rolcreatedb
               OR role_record.rolreplication
               OR role_record.rolbypassrls THEN
                RAISE EXCEPTION
                    'impersonation reader has unsafe role attributes';
            END IF;

            IF has_schema_privilege(
                '{IMPERSONATION_ROLE}',
                'public',
                'CREATE'
            ) THEN
                RAISE EXCEPTION
                    'impersonation reader unexpectedly has CREATE '
                    'on schema public';
            END IF;

            FOREACH object_name IN ARRAY ARRAY[
                {allowed_tables_array}
            ]
            LOOP
                IF NOT has_table_privilege(
                    '{IMPERSONATION_ROLE}',
                    'public.' || object_name,
                    'SELECT'
                ) THEN
                    RAISE EXCEPTION
                        'impersonation reader is missing SELECT on %',
                        object_name;
                END IF;

                IF has_table_privilege(
                    '{IMPERSONATION_ROLE}',
                    'public.' || object_name,
                    'INSERT'
                )
                OR has_table_privilege(
                    '{IMPERSONATION_ROLE}',
                    'public.' || object_name,
                    'UPDATE'
                )
                OR has_table_privilege(
                    '{IMPERSONATION_ROLE}',
                    'public.' || object_name,
                    'DELETE'
                )
                OR has_table_privilege(
                    '{IMPERSONATION_ROLE}',
                    'public.' || object_name,
                    'TRUNCATE'
                )
                OR has_table_privilege(
                    '{IMPERSONATION_ROLE}',
                    'public.' || object_name,
                    'REFERENCES'
                )
                OR has_table_privilege(
                    '{IMPERSONATION_ROLE}',
                    'public.' || object_name,
                    'TRIGGER'
                ) THEN
                    RAISE EXCEPTION
                        'impersonation reader has write privilege on %',
                        object_name;
                END IF;
            END LOOP;

            FOREACH object_name IN ARRAY ARRAY[
                {sensitive_tables_array}
            ]
            LOOP
                IF has_table_privilege(
                    '{IMPERSONATION_ROLE}',
                    'public.' || object_name,
                    'SELECT'
                ) THEN
                    RAISE EXCEPTION
                        'impersonation reader can read forbidden table %',
                        object_name;
                END IF;
            END LOOP;

            FOREACH object_name IN ARRAY ARRAY[
                {forbidden_vault_columns_array}
            ]
            LOOP
                IF has_column_privilege(
                    '{IMPERSONATION_ROLE}',
                    'public.vault_documents',
                    object_name,
                    'SELECT'
                ) THEN
                    RAISE EXCEPTION
                        'impersonation reader can read forbidden '
                        'vault column %',
                        object_name;
                END IF;
            END LOOP;

            FOREACH object_name IN ARRAY ARRAY[
                {denied_functions_array}
            ]
            LOOP
                IF has_function_privilege(
                    '{IMPERSONATION_ROLE}',
                    object_name,
                    'EXECUTE'
                ) THEN
                    RAISE EXCEPTION
                        'impersonation reader can execute forbidden '
                        'control-plane function %',
                        object_name;
                END IF;
            END LOOP;
        END
        $$;
        """
    )


def upgrade() -> None:
    _create_and_harden_reader_role()
    _create_impersonation_reader_policies()
    _create_vault_metadata_policy()
    _create_impersonation_sessions_table()

    _enter_platform_role()
    _create_control_plane_functions()
    _lock_down_control_plane_functions()
    _leave_platform_role()

    _assert_reader_is_locked_down()


def _revoke_reader_memberships_for_drop() -> None:
    op.execute(
        f"""
        DO $$
        DECLARE
            membership record;
        BEGIN
            FOR membership IN
                SELECT parent.rolname AS parent_role
                FROM pg_auth_members AS membership_map
                JOIN pg_roles AS parent
                  ON parent.oid = membership_map.roleid
                JOIN pg_roles AS child
                  ON child.oid = membership_map.member
                WHERE child.rolname = '{IMPERSONATION_ROLE}'
            LOOP
                EXECUTE format(
                    'REVOKE %I FROM %I',
                    membership.parent_role,
                    '{IMPERSONATION_ROLE}'
                );
            END LOOP;
        END
        $$;
        """
    )

    op.execute(
        f"""
        DO $$
        DECLARE
            membership record;
        BEGIN
            FOR membership IN
                SELECT member_role.rolname AS member_role
                FROM pg_auth_members AS membership_map
                JOIN pg_roles AS granted_role
                  ON granted_role.oid = membership_map.roleid
                JOIN pg_roles AS member_role
                  ON member_role.oid = membership_map.member
                WHERE granted_role.rolname = '{IMPERSONATION_ROLE}'
            LOOP
                EXECUTE format(
                    'REVOKE %I FROM %I',
                    '{IMPERSONATION_ROLE}',
                    membership.member_role
                );
            END LOOP;
        END
        $$;
        """
    )


def downgrade() -> None:
    _enter_platform_role()
    for signature in reversed(IMPERSONATION_CONTROL_FUNCTIONS):
        op.execute(f"DROP FUNCTION IF EXISTS {signature}")
    _leave_platform_role()

    op.execute(
        """
        DROP POLICY IF EXISTS
            us_lacey_impersonation_sessions_platform_update
        ON public.us_lacey_admin_impersonation_sessions
        """
    )
    op.execute(
        """
        DROP POLICY IF EXISTS
            us_lacey_impersonation_sessions_platform_insert
        ON public.us_lacey_admin_impersonation_sessions
        """
    )
    op.execute(
        """
        DROP POLICY IF EXISTS
            us_lacey_impersonation_sessions_platform_select
        ON public.us_lacey_admin_impersonation_sessions
        """
    )

    op.drop_index(
        "ix_us_lacey_impersonation_expiry_revoked",
        table_name="us_lacey_admin_impersonation_sessions",
    )
    op.drop_index(
        "ix_us_lacey_impersonation_target_created",
        table_name="us_lacey_admin_impersonation_sessions",
    )
    op.drop_index(
        "ix_us_lacey_impersonation_admin_created",
        table_name="us_lacey_admin_impersonation_sessions",
    )
    op.drop_table("us_lacey_admin_impersonation_sessions")

    safe_columns = ", ".join(VAULT_SAFE_COLUMNS)
    op.execute(
        f"""
        REVOKE SELECT ({safe_columns})
        ON TABLE public.vault_documents
        FROM {IMPERSONATION_ROLE}
        """
    )
    op.execute(
        """
        DROP POLICY IF EXISTS vault_documents_impersonation_read
        ON public.vault_documents
        """
    )
    op.execute(
        """
        DROP POLICY IF EXISTS vault_documents_impersonation_allow
        ON public.vault_documents
        """
    )

    for table in reversed(IMPERSONATION_TABLES):
        op.execute(
            f"REVOKE SELECT ON TABLE public.{table} "
            f"FROM {IMPERSONATION_ROLE}"
        )
        op.execute(
            f"DROP POLICY IF EXISTS {table}_impersonation_read "
            f"ON public.{table}"
        )
        op.execute(
            f"DROP POLICY IF EXISTS {table}_impersonation_allow "
            f"ON public.{table}"
        )

    op.execute(
        f"""
        REVOKE ALL PRIVILEGES
        ON ALL TABLES IN SCHEMA public
        FROM {IMPERSONATION_ROLE}
        """
    )
    op.execute(
        f"""
        REVOKE ALL PRIVILEGES
        ON ALL SEQUENCES IN SCHEMA public
        FROM {IMPERSONATION_ROLE}
        """
    )
    op.execute(
        f"""
        REVOKE ALL PRIVILEGES
        ON ALL FUNCTIONS IN SCHEMA public
        FROM {IMPERSONATION_ROLE}
        """
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON SCHEMA public FROM {IMPERSONATION_ROLE}"
    )

    _revoke_reader_memberships_for_drop()
    op.execute(f"DROP ROLE IF EXISTS {IMPERSONATION_ROLE}")
