"""Add first-party outreach attribution and sandbox funnel telemetry.

Revision ID: 059_us_lacey_outreach_attribution
Revises: 058_us_lacey_exception_first_states
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "059_us_lacey_outreach_attribution"
down_revision = "058_us_lacey_exception_first_states"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"

OPEN_FUNCTION = "public.us_lacey_outreach_open(text)"
BIND_FUNCTION = "public.us_lacey_outreach_bind_sandbox(text,integer,uuid)"
EVENT_FUNCTION = "public.us_lacey_outreach_record_event(text,integer,text,text,jsonb)"
CREATE_LINK_FUNCTION = (
    "public.platform_admin_create_outreach_link(text,text,text,text,text)"
)
FUNNEL_FUNCTION = "public.platform_admin_outreach_funnel(text,integer)"


def _grant_temp_platform_set() -> None:
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )


def _revoke_temp_platform_set() -> None:
    op.execute(
        f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER"
    )


def upgrade() -> None:
    op.create_table(
        "us_lacey_outreach_links",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("slug", sa.String(96), nullable=False),
        sa.Column("prospect_label", sa.String(160), nullable=False),
        sa.Column("campaign_code", sa.String(96), nullable=False),
        sa.Column("source", sa.String(48), nullable=False, server_default="direct_outreach"),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("click_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_clicked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("slug", name="uq_us_lacey_outreach_links_slug"),
        sa.CheckConstraint("click_count >= 0", name="ck_us_lacey_outreach_links_click_count"),
    )
    op.create_index(
        "ix_us_lacey_outreach_links_campaign",
        "us_lacey_outreach_links",
        ["campaign_code", "created_at"],
    )

    op.create_table(
        "us_lacey_outreach_sessions",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("outreach_link_id", sa.BigInteger(), nullable=False),
        sa.Column("prospect_label", sa.String(160), nullable=False),
        sa.Column("campaign_code", sa.String(96), nullable=False),
        sa.Column("source", sa.String(48), nullable=False),
        sa.Column("sandbox_organization_id", sa.Integer(), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("sandbox_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_event_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ["outreach_link_id"],
            ["us_lacey_outreach_links.id"],
            name="fk_us_lacey_outreach_sessions_link",
            ondelete="RESTRICT",
        ),
    )
    op.create_index(
        "ix_us_lacey_outreach_sessions_link_seen",
        "us_lacey_outreach_sessions",
        ["outreach_link_id", "first_seen_at"],
    )
    op.create_index(
        "ix_us_lacey_outreach_sessions_sandbox_org",
        "us_lacey_outreach_sessions",
        ["sandbox_organization_id"],
        postgresql_where=sa.text("sandbox_organization_id IS NOT NULL"),
    )

    op.create_table(
        "us_lacey_outreach_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("event_name", sa.String(40), nullable=False),
        sa.Column("event_key", sa.String(128), nullable=False, server_default=""),
        sa.Column("event_metadata", sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ["session_id"],
            ["us_lacey_outreach_sessions.id"],
            name="fk_us_lacey_outreach_events_session",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "session_id",
            "event_name",
            "event_key",
            name="uq_us_lacey_outreach_event_idempotency",
        ),
        sa.CheckConstraint(
            "event_name IN ("
            "'LINK_OPENED','SANDBOX_STARTED','OPERATION_CREATED',"
            "'DOCUMENTS_UPLOADED','REVIEW_REACHED','AUTO_RESOLVED_CONFIRMED',"
            "'REVIEW_COMPLETED','EXPORT_DOWNLOADED'"
            ")",
            name="ck_us_lacey_outreach_event_name",
        ),
    )
    op.create_index(
        "ix_us_lacey_outreach_events_session_time",
        "us_lacey_outreach_events",
        ["session_id", "occurred_at"],
    )
    op.create_index(
        "ix_us_lacey_outreach_events_name_time",
        "us_lacey_outreach_events",
        ["event_name", "occurred_at"],
    )

    op.add_column(
        "organizations",
        sa.Column("sandbox_attribution_session_id", sa.Uuid(), nullable=True),
    )
    op.create_foreign_key(
        "fk_organizations_sandbox_attribution_session",
        "organizations",
        "us_lacey_outreach_sessions",
        ["sandbox_attribution_session_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_organizations_sandbox_attribution_session",
        "organizations",
        ["sandbox_attribution_session_id"],
        unique=False,
        postgresql_where=sa.text("sandbox_attribution_session_id IS NOT NULL"),
    )

    for table_name in (
        "us_lacey_outreach_links",
        "us_lacey_outreach_sessions",
        "us_lacey_outreach_events",
    ):
        op.execute(f"REVOKE ALL ON TABLE public.{table_name} FROM PUBLIC")
        op.execute(f"REVOKE ALL ON TABLE public.{table_name} FROM {RUNTIME_ROLE}")
        op.execute(f"REVOKE ALL ON TABLE public.{table_name} FROM {WORKER_ROLE}")

    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    for table_name in (
        "us_lacey_outreach_links",
        "us_lacey_outreach_sessions",
        "us_lacey_outreach_events",
    ):
        op.execute(
            f"GRANT SELECT, INSERT, UPDATE ON TABLE public.{table_name} "
            f"TO {PLATFORM_ROLE}"
        )
    for sequence_name in (
        "us_lacey_outreach_links_id_seq",
        "us_lacey_outreach_events_id_seq",
    ):
        op.execute(
            f"GRANT USAGE, SELECT ON SEQUENCE public.{sequence_name} "
            f"TO {PLATFORM_ROLE}"
        )
    op.execute(f"GRANT SELECT, UPDATE ON TABLE public.organizations TO {PLATFORM_ROLE}")
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_outreach_open(requested_slug text)
        RETURNS TABLE(
            attribution_session_id uuid,
            prospect_label text,
            campaign_code text,
            source text
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            link_row record;
            new_session_id uuid := gen_random_uuid();
        BEGIN
            IF requested_slug IS NULL
               OR requested_slug !~ '^[a-z0-9][a-z0-9-]{2,95}$' THEN
                RAISE EXCEPTION 'invalid outreach link'
                    USING ERRCODE = '22023';
            END IF;

            UPDATE public.us_lacey_outreach_links AS link
            SET
                click_count = link.click_count + 1,
                last_clicked_at = now()
            WHERE link.slug = requested_slug
              AND link.active = true
              AND (link.expires_at IS NULL OR link.expires_at > now())
            RETURNING
                link.id,
                link.prospect_label,
                link.campaign_code,
                link.source
            INTO link_row;

            IF NOT FOUND THEN
                RAISE EXCEPTION 'outreach link unavailable'
                    USING ERRCODE = '22023';
            END IF;

            INSERT INTO public.us_lacey_outreach_sessions(
                id,
                outreach_link_id,
                prospect_label,
                campaign_code,
                source,
                first_seen_at,
                last_event_at
            ) VALUES (
                new_session_id,
                link_row.id,
                link_row.prospect_label,
                link_row.campaign_code,
                link_row.source,
                now(),
                now()
            );

            INSERT INTO public.us_lacey_outreach_events(
                session_id,
                event_name,
                event_key,
                event_metadata,
                occurred_at
            ) VALUES (
                new_session_id,
                'LINK_OPENED',
                '',
                '{}'::jsonb,
                now()
            );

            RETURN QUERY
            SELECT
                new_session_id,
                link_row.prospect_label::text,
                link_row.campaign_code::text,
                link_row.source::text;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_outreach_bind_sandbox(
            requested_token_hash text,
            requested_organization_id integer,
            requested_attribution_session_id uuid
        )
        RETURNS boolean
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            resolved_organization_id integer;
            sandbox_flag boolean;
            updated_rows integer;
        BEGIN
            IF requested_token_hash IS NULL
               OR requested_token_hash !~ '^[0-9a-f]{64}$'
               OR requested_organization_id IS NULL
               OR requested_organization_id <= 0
               OR requested_attribution_session_id IS NULL THEN
                RAISE EXCEPTION 'invalid outreach binding'
                    USING ERRCODE = '22023';
            END IF;

            SELECT portal_session.organization_id
            INTO resolved_organization_id
            FROM public.us_lacey_portal_session_lookup(
                requested_token_hash
            ) AS portal_session;

            IF resolved_organization_id IS NULL
               OR resolved_organization_id <> requested_organization_id THEN
                RAISE EXCEPTION 'sandbox session mismatch'
                    USING ERRCODE = '42501';
            END IF;

            SELECT org.is_sandbox
            INTO sandbox_flag
            FROM public.organizations AS org
            WHERE org.id = requested_organization_id
            FOR UPDATE;

            IF sandbox_flag IS DISTINCT FROM true THEN
                RAISE EXCEPTION 'outreach binding requires sandbox tenant'
                    USING ERRCODE = '55000';
            END IF;

            UPDATE public.us_lacey_outreach_sessions AS session
            SET
                sandbox_organization_id = requested_organization_id,
                sandbox_started_at = COALESCE(session.sandbox_started_at, now()),
                last_event_at = now()
            WHERE session.id = requested_attribution_session_id
              AND (
                    session.sandbox_organization_id IS NULL
                    OR session.sandbox_organization_id = requested_organization_id
              );

            GET DIAGNOSTICS updated_rows = ROW_COUNT;
            IF updated_rows <> 1 THEN
                RAISE EXCEPTION 'outreach session unavailable'
                    USING ERRCODE = '55000';
            END IF;

            UPDATE public.organizations AS org
            SET sandbox_attribution_session_id = requested_attribution_session_id
            WHERE org.id = requested_organization_id
              AND (
                    org.sandbox_attribution_session_id IS NULL
                    OR org.sandbox_attribution_session_id =
                        requested_attribution_session_id
              );

            GET DIAGNOSTICS updated_rows = ROW_COUNT;
            IF updated_rows <> 1 THEN
                RAISE EXCEPTION 'sandbox attribution already bound'
                    USING ERRCODE = '55000';
            END IF;

            INSERT INTO public.us_lacey_outreach_events(
                session_id,
                event_name,
                event_key,
                event_metadata,
                occurred_at
            ) VALUES (
                requested_attribution_session_id,
                'SANDBOX_STARTED',
                '',
                '{}'::jsonb,
                now()
            )
            ON CONFLICT (session_id, event_name, event_key) DO NOTHING;

            RETURN true;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_outreach_record_event(
            requested_token_hash text,
            requested_organization_id integer,
            requested_event_name text,
            requested_event_key text,
            requested_metadata jsonb
        )
        RETURNS boolean
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            resolved_organization_id integer;
            attribution_id uuid;
            normalized_event text := upper(btrim(coalesce(requested_event_name, '')));
            normalized_key text := left(btrim(coalesce(requested_event_key, '')), 128);
        BEGIN
            IF normalized_event NOT IN (
                'OPERATION_CREATED',
                'DOCUMENTS_UPLOADED',
                'REVIEW_REACHED',
                'AUTO_RESOLVED_CONFIRMED',
                'REVIEW_COMPLETED',
                'EXPORT_DOWNLOADED'
            ) THEN
                RAISE EXCEPTION 'invalid outreach event'
                    USING ERRCODE = '22023';
            END IF;

            SELECT portal_session.organization_id
            INTO resolved_organization_id
            FROM public.us_lacey_portal_session_lookup(
                requested_token_hash
            ) AS portal_session;

            IF resolved_organization_id IS NULL
               OR resolved_organization_id <> requested_organization_id THEN
                RAISE EXCEPTION 'outreach event session mismatch'
                    USING ERRCODE = '42501';
            END IF;

            SELECT org.sandbox_attribution_session_id
            INTO attribution_id
            FROM public.organizations AS org
            WHERE org.id = requested_organization_id;

            IF attribution_id IS NULL THEN
                RETURN false;
            END IF;

            INSERT INTO public.us_lacey_outreach_events(
                session_id,
                event_name,
                event_key,
                event_metadata,
                occurred_at
            ) VALUES (
                attribution_id,
                normalized_event,
                normalized_key,
                coalesce(requested_metadata, '{}'::jsonb),
                now()
            )
            ON CONFLICT (session_id, event_name, event_key) DO NOTHING;

            UPDATE public.us_lacey_outreach_sessions AS session
            SET last_event_at = greatest(session.last_event_at, now())
            WHERE session.id = attribution_id;

            RETURN true;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.platform_admin_create_outreach_link(
            actor_refresh_token_hash text,
            requested_slug text,
            requested_prospect_label text,
            requested_campaign_code text,
            requested_source text
        )
        RETURNS TABLE(
            outreach_link_id bigint,
            slug text,
            prospect_label text,
            campaign_code text,
            source text,
            active boolean,
            created_at timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            actor record;
            new_link record;
        BEGIN
            SELECT * INTO actor
            FROM public._platform_superadmin_session_actor(
                actor_refresh_token_hash
            );

            IF requested_slug IS NULL
               OR requested_slug !~ '^[a-z0-9][a-z0-9-]{2,95}$'
               OR btrim(coalesce(requested_prospect_label, '')) = ''
               OR btrim(coalesce(requested_campaign_code, '')) = ''
               OR btrim(coalesce(requested_source, '')) = '' THEN
                RAISE EXCEPTION 'invalid outreach link request'
                    USING ERRCODE = '22023';
            END IF;

            INSERT INTO public.us_lacey_outreach_links(
                slug,
                prospect_label,
                campaign_code,
                source,
                active,
                click_count,
                created_at
            ) VALUES (
                requested_slug,
                left(btrim(requested_prospect_label), 160),
                left(btrim(requested_campaign_code), 96),
                left(lower(btrim(requested_source)), 48),
                true,
                0,
                now()
            )
            RETURNING * INTO new_link;

            PERFORM public._platform_insert_audit_log(
                actor.actor_user_id,
                NULL,
                'superadmin',
                actor.actor_organization_id,
                NULL,
                'OUTREACH_LINK_CREATED',
                'us_lacey_outreach_link',
                new_link.id,
                jsonb_build_object(
                    'slug', new_link.slug,
                    'prospect_label', new_link.prospect_label,
                    'campaign_code', new_link.campaign_code,
                    'source', new_link.source
                )
            );

            RETURN QUERY
            SELECT
                new_link.id,
                new_link.slug::text,
                new_link.prospect_label::text,
                new_link.campaign_code::text,
                new_link.source::text,
                new_link.active,
                new_link.created_at;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.platform_admin_outreach_funnel(
            actor_refresh_token_hash text,
            requested_limit integer
        )
        RETURNS TABLE(
            outreach_link_id bigint,
            slug text,
            prospect_label text,
            campaign_code text,
            source text,
            active boolean,
            created_at timestamptz,
            click_count integer,
            attributed_sessions bigint,
            sandbox_started bigint,
            operations_created bigint,
            document_uploads bigint,
            review_reached bigint,
            auto_resolved_confirmed bigint,
            review_completed bigint,
            exports_downloaded bigint,
            last_event_at timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        BEGIN
            PERFORM 1
            FROM public._platform_superadmin_session_actor(
                actor_refresh_token_hash
            );

            RETURN QUERY
            WITH session_rollup AS (
                SELECT
                    session.outreach_link_id,
                    count(*)::bigint AS attributed_sessions,
                    count(*) FILTER (
                        WHERE session.sandbox_started_at IS NOT NULL
                    )::bigint AS sandbox_started,
                    max(session.last_event_at) AS last_event_at
                FROM public.us_lacey_outreach_sessions AS session
                GROUP BY session.outreach_link_id
            ),
            event_rollup AS (
                SELECT
                    session.outreach_link_id,
                    count(*) FILTER (
                        WHERE event.event_name = 'OPERATION_CREATED'
                    )::bigint AS operations_created,
                    count(*) FILTER (
                        WHERE event.event_name = 'DOCUMENTS_UPLOADED'
                    )::bigint AS document_uploads,
                    count(*) FILTER (
                        WHERE event.event_name = 'REVIEW_REACHED'
                    )::bigint AS review_reached,
                    count(*) FILTER (
                        WHERE event.event_name = 'AUTO_RESOLVED_CONFIRMED'
                    )::bigint AS auto_resolved_confirmed,
                    count(*) FILTER (
                        WHERE event.event_name = 'REVIEW_COMPLETED'
                    )::bigint AS review_completed,
                    count(*) FILTER (
                        WHERE event.event_name = 'EXPORT_DOWNLOADED'
                    )::bigint AS exports_downloaded
                FROM public.us_lacey_outreach_sessions AS session
                JOIN public.us_lacey_outreach_events AS event
                  ON event.session_id = session.id
                GROUP BY session.outreach_link_id
            )
            SELECT
                link.id,
                link.slug::text,
                link.prospect_label::text,
                link.campaign_code::text,
                link.source::text,
                link.active,
                link.created_at,
                link.click_count,
                coalesce(session_rollup.attributed_sessions, 0),
                coalesce(session_rollup.sandbox_started, 0),
                coalesce(event_rollup.operations_created, 0),
                coalesce(event_rollup.document_uploads, 0),
                coalesce(event_rollup.review_reached, 0),
                coalesce(event_rollup.auto_resolved_confirmed, 0),
                coalesce(event_rollup.review_completed, 0),
                coalesce(event_rollup.exports_downloaded, 0),
                session_rollup.last_event_at
            FROM public.us_lacey_outreach_links AS link
            LEFT JOIN session_rollup
              ON session_rollup.outreach_link_id = link.id
            LEFT JOIN event_rollup
              ON event_rollup.outreach_link_id = link.id
            ORDER BY link.created_at DESC
            LIMIT least(greatest(coalesce(requested_limit, 50), 1), 200);
        END;
        $$;
        """
    )

    op.execute("RESET ROLE")

    for signature in (
        OPEN_FUNCTION,
        BIND_FUNCTION,
        EVENT_FUNCTION,
        CREATE_LINK_FUNCTION,
        FUNNEL_FUNCTION,
    ):
        op.execute(f"ALTER FUNCTION {signature} OWNER TO {PLATFORM_ROLE}")
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM {WORKER_ROLE}")

    for signature in (OPEN_FUNCTION, BIND_FUNCTION, EVENT_FUNCTION):
        op.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {RUNTIME_ROLE}")
    for signature in (CREATE_LINK_FUNCTION, FUNNEL_FUNCTION):
        op.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {RUNTIME_ROLE}")

    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()


def downgrade() -> None:
    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")
    for signature in (
        FUNNEL_FUNCTION,
        CREATE_LINK_FUNCTION,
        EVENT_FUNCTION,
        BIND_FUNCTION,
        OPEN_FUNCTION,
    ):
        op.execute(f"DROP FUNCTION IF EXISTS {signature}")
    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()

    op.drop_index(
        "ix_organizations_sandbox_attribution_session",
        table_name="organizations",
    )
    op.drop_constraint(
        "fk_organizations_sandbox_attribution_session",
        "organizations",
        type_="foreignkey",
    )
    op.drop_column("organizations", "sandbox_attribution_session_id")

    op.drop_index(
        "ix_us_lacey_outreach_events_name_time",
        table_name="us_lacey_outreach_events",
    )
    op.drop_index(
        "ix_us_lacey_outreach_events_session_time",
        table_name="us_lacey_outreach_events",
    )
    op.drop_table("us_lacey_outreach_events")

    op.drop_index(
        "ix_us_lacey_outreach_sessions_sandbox_org",
        table_name="us_lacey_outreach_sessions",
    )
    op.drop_index(
        "ix_us_lacey_outreach_sessions_link_seen",
        table_name="us_lacey_outreach_sessions",
    )
    op.drop_table("us_lacey_outreach_sessions")

    op.drop_index(
        "ix_us_lacey_outreach_links_campaign",
        table_name="us_lacey_outreach_links",
    )
    op.drop_table("us_lacey_outreach_links")
