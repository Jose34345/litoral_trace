"""Classify outreach scans, human visits, and sandbox engagement.

Revision ID: 077_us_lacey_outreach_human_signals
Revises: 076_us_lacey_product_led_evaluation
"""
from __future__ import annotations

from alembic import op


revision = "077_us_lacey_outreach_human_signals"
down_revision = "076_us_lacey_product_led_evaluation"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"

OPEN_FUNCTION = "public.us_lacey_outreach_open(text)"
BIND_FUNCTION = "public.us_lacey_outreach_bind_sandbox(text,integer,uuid)"
PRE_SANDBOX_EVENT_FUNCTION = (
    "public.us_lacey_outreach_record_pre_sandbox_event(uuid,text,text,jsonb)"
)
ENGAGEMENT_FUNCTION = "public.platform_admin_outreach_engagement(text,integer)"


def _grant_temp_platform_set() -> None:
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )


def _revoke_temp_platform_set() -> None:
    op.execute(
        f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER"
    )


def _create_event_constraint(*, include_human_signals: bool) -> None:
    events = [
        "LINK_OPENED",
        "SAMPLE_STARTED",
        "SAMPLE_REUSE_REACHED",
        "SAMPLE_COMPLETED",
        "SANDBOX_STARTED",
        "OWN_SHIPMENT_STARTED",
        "OPERATION_CREATED",
        "DOCUMENTS_UPLOADED",
        "OWN_SHIPMENT_PROCESSED",
        "EVALUATION_CLAIMED",
        "EVALUATION_OPERATION_2",
        "EVIDENCE_REUSED",
        "REVIEW_REACHED",
        "AUTO_RESOLVED_CONFIRMED",
        "REVIEW_COMPLETED",
        "EXPORT_DOWNLOADED",
        "EVALUATION_EXHAUSTED",
        "UPGRADE_STARTED",
        "PQL_QUALIFIED",
    ]
    if include_human_signals:
        events.extend(["LINK_SCANNED", "HUMAN_VISIT", "SANDBOX_ENGAGED"])
    allowed = ",".join(f"'{event}'" for event in events)
    op.create_check_constraint(
        "ck_us_lacey_outreach_event_name",
        "us_lacey_outreach_events",
        f"event_name IN ({allowed})",
    )


def _replace_open_function(*, scan_event: str) -> None:
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.us_lacey_outreach_open(requested_slug text)
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
               OR requested_slug !~ '^[a-z0-9][a-z0-9-]{{2,95}}$' THEN
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
                '{scan_event}',
                '',
                '{{}}'::jsonb,
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


def _replace_pre_sandbox_event_function(*, allow_human_visit: bool) -> None:
    allowed = (
        "'SAMPLE_STARTED','SAMPLE_REUSE_REACHED','SAMPLE_COMPLETED','HUMAN_VISIT'"
        if allow_human_visit
        else "'SAMPLE_STARTED','SAMPLE_REUSE_REACHED','SAMPLE_COMPLETED'"
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.us_lacey_outreach_record_pre_sandbox_event(
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
            normalized_metadata := coalesce(requested_event_metadata, '{{}}'::jsonb);

            IF normalized_event NOT IN ({allowed}) THEN
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

            INSERT INTO public.us_lacey_outreach_events(
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
            ON CONFLICT (session_id, event_name, event_key) DO NOTHING;

            UPDATE public.us_lacey_outreach_sessions
            SET last_event_at = now()
            WHERE id = requested_attribution_session_id;

            RETURN true;
        END;
        $$;
        """
    )


def _replace_bind_function(*, include_engagement_event: bool) -> None:
    engagement_sql = """
            INSERT INTO public.us_lacey_outreach_events(
                session_id,
                event_name,
                event_key,
                event_metadata,
                occurred_at
            ) VALUES (
                requested_attribution_session_id,
                'SANDBOX_ENGAGED',
                '',
                '{}'::jsonb,
                now()
            )
            ON CONFLICT (session_id, event_name, event_key) DO NOTHING;
    """ if include_engagement_event else ""

    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.us_lacey_outreach_bind_sandbox(
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
               OR requested_token_hash !~ '^[0-9a-f]{{64}}$'
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
                '{{}}'::jsonb,
                now()
            )
            ON CONFLICT (session_id, event_name, event_key) DO NOTHING;

            {engagement_sql}

            RETURN true;
        END;
        $$;
        """
    )


def upgrade() -> None:
    op.drop_constraint(
        "ck_us_lacey_outreach_event_name",
        "us_lacey_outreach_events",
        type_="check",
    )
    _create_event_constraint(include_human_signals=True)

    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(
        "GRANT SELECT ON TABLE public.us_lacey_outreach_links, "
        "public.us_lacey_outreach_sessions, public.us_lacey_outreach_events "
        f"TO {PLATFORM_ROLE}"
    )
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")

    _replace_open_function(scan_event="LINK_SCANNED")
    _replace_pre_sandbox_event_function(allow_human_visit=True)
    _replace_bind_function(include_engagement_event=True)

    op.execute(
        """
        CREATE FUNCTION public.platform_admin_outreach_engagement(
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
            raw_hits integer,
            attributed_sessions bigint,
            link_scans bigint,
            human_visits bigint,
            product_engaged bigint,
            sandbox_engaged bigint,
            operations_created bigint,
            document_uploads bigint,
            review_reached bigint,
            review_completed bigint,
            exports_downloaded bigint,
            last_event_at timestamptz,
            engagement_state text
        )
        LANGUAGE plpgsql
        STABLE
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
                    max(session.last_event_at) AS last_event_at
                FROM public.us_lacey_outreach_sessions AS session
                GROUP BY session.outreach_link_id
            ),
            event_rollup AS (
                SELECT
                    session.outreach_link_id,
                    count(DISTINCT event.session_id) FILTER (
                        WHERE event.event_name IN ('LINK_SCANNED','LINK_OPENED')
                    )::bigint AS link_scans,
                    count(DISTINCT event.session_id) FILTER (
                        WHERE event.event_name = 'HUMAN_VISIT'
                    )::bigint AS human_visits,
                    count(DISTINCT event.session_id) FILTER (
                        WHERE event.event_name IN (
                            'SAMPLE_STARTED',
                            'SAMPLE_REUSE_REACHED',
                            'SAMPLE_COMPLETED',
                            'SANDBOX_STARTED',
                            'SANDBOX_ENGAGED',
                            'OPERATION_CREATED',
                            'DOCUMENTS_UPLOADED',
                            'REVIEW_REACHED',
                            'REVIEW_COMPLETED',
                            'EXPORT_DOWNLOADED'
                        )
                    )::bigint AS product_engaged,
                    count(DISTINCT event.session_id) FILTER (
                        WHERE event.event_name IN (
                            'SANDBOX_STARTED','SANDBOX_ENGAGED'
                        )
                    )::bigint AS sandbox_engaged,
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
                coalesce(event_rollup.link_scans, 0),
                coalesce(event_rollup.human_visits, 0),
                coalesce(event_rollup.product_engaged, 0),
                coalesce(event_rollup.sandbox_engaged, 0),
                coalesce(event_rollup.operations_created, 0),
                coalesce(event_rollup.document_uploads, 0),
                coalesce(event_rollup.review_reached, 0),
                coalesce(event_rollup.review_completed, 0),
                coalesce(event_rollup.exports_downloaded, 0),
                session_rollup.last_event_at,
                CASE
                    WHEN coalesce(event_rollup.product_engaged, 0) > 0
                        THEN 'PRODUCT_ENGAGED'
                    WHEN coalesce(event_rollup.human_visits, 0) > 0
                        THEN 'LIKELY_HUMAN'
                    WHEN link.click_count > 0
                        THEN 'LIKELY_AUTOMATED'
                    ELSE 'NO_ACTIVITY'
                END::text
            FROM public.us_lacey_outreach_links AS link
            LEFT JOIN session_rollup
              ON session_rollup.outreach_link_id = link.id
            LEFT JOIN event_rollup
              ON event_rollup.outreach_link_id = link.id
            ORDER BY
                CASE
                    WHEN coalesce(event_rollup.product_engaged, 0) > 0 THEN 0
                    WHEN coalesce(event_rollup.human_visits, 0) > 0 THEN 1
                    WHEN link.click_count > 0 THEN 2
                    ELSE 3
                END,
                coalesce(session_rollup.last_event_at, link.created_at) DESC
            LIMIT least(greatest(coalesce(requested_limit, 50), 1), 200);
        END;
        $$;
        """
    )

    for signature in (
        OPEN_FUNCTION,
        BIND_FUNCTION,
        PRE_SANDBOX_EVENT_FUNCTION,
        ENGAGEMENT_FUNCTION,
    ):
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM {WORKER_ROLE}")
        op.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {RUNTIME_ROLE}")

    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()


def downgrade() -> None:
    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")

    op.execute(f"DROP FUNCTION IF EXISTS {ENGAGEMENT_FUNCTION}")
    _replace_open_function(scan_event="LINK_OPENED")
    _replace_pre_sandbox_event_function(allow_human_visit=False)
    _replace_bind_function(include_engagement_event=False)

    for signature in (
        OPEN_FUNCTION,
        BIND_FUNCTION,
        PRE_SANDBOX_EVENT_FUNCTION,
    ):
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM {WORKER_ROLE}")
        op.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {RUNTIME_ROLE}")

    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()

    op.execute(
        "DELETE FROM public.us_lacey_outreach_events "
        "WHERE event_name IN ('HUMAN_VISIT','SANDBOX_ENGAGED')"
    )
    op.execute(
        """
        DELETE FROM public.us_lacey_outreach_events AS scanned
        WHERE scanned.event_name = 'LINK_SCANNED'
          AND EXISTS (
              SELECT 1
              FROM public.us_lacey_outreach_events AS legacy
              WHERE legacy.session_id = scanned.session_id
                AND legacy.event_name = 'LINK_OPENED'
                AND legacy.event_key = scanned.event_key
          )
        """
    )
    op.execute(
        "UPDATE public.us_lacey_outreach_events "
        "SET event_name = 'LINK_OPENED' "
        "WHERE event_name = 'LINK_SCANNED'"
    )

    op.drop_constraint(
        "ck_us_lacey_outreach_event_name",
        "us_lacey_outreach_events",
        type_="check",
    )
    _create_event_constraint(include_human_signals=False)
