"""Expose privacy-bounded Pilot Watch control-plane projections.

Revision ID: 065_us_lacey_pilot_watch
Revises: 064_us_lacey_pilot_watchdog
"""
from __future__ import annotations

from alembic import op


revision = "065_us_lacey_pilot_watch"
down_revision = "064_us_lacey_pilot_watchdog"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"

METRICS_FUNCTION = "public.platform_admin_pilot_watch_metrics(text)"
FEED_FUNCTION = "public.platform_admin_pilot_watch_feed(text,integer)"


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
    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(
        f"GRANT SELECT (id, public_id, organization_id, status, updated_at) "
        f"ON TABLE public.us_lacey_operations TO {PLATFORM_ROLE}"
    )
    op.execute(
        f"GRANT SELECT (id, is_active, is_sandbox, sandbox_expires_at) "
        f"ON TABLE public.organizations TO {PLATFORM_ROLE}"
    )
    op.execute(
        f"GRANT SELECT (id, outreach_link_id, last_event_at) "
        f"ON TABLE public.us_lacey_outreach_sessions TO {PLATFORM_ROLE}"
    )
    op.execute(
        f"GRANT SELECT (id, slug, prospect_label) "
        f"ON TABLE public.us_lacey_outreach_links TO {PLATFORM_ROLE}"
    )
    op.execute(
        f"GRANT SELECT (session_id, event_name, occurred_at) "
        f"ON TABLE public.us_lacey_outreach_events TO {PLATFORM_ROLE}"
    )
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")

    op.execute(
        """
        CREATE FUNCTION public.platform_admin_pilot_watch_metrics(
            actor_refresh_token_hash text
        )
        RETURNS TABLE(
            active_sandboxes bigint,
            tests_today bigint,
            open_p0 bigint,
            open_p1 bigint,
            exports_completed bigint
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
            SELECT
                (
                    SELECT count(*)::bigint
                    FROM public.organizations AS org
                    WHERE org.is_active IS TRUE
                      AND org.is_sandbox IS TRUE
                      AND (
                          org.sandbox_expires_at IS NULL
                          OR org.sandbox_expires_at > now()
                      )
                ),
                (
                    SELECT count(*)::bigint
                    FROM public.us_lacey_pilot_quality_snapshots AS snapshot
                    WHERE snapshot.trigger IN ('INITIAL_PROCESS', 'REPROCESS')
                      AND snapshot.created_at >= date_trunc('day', now())
                ),
                (
                    SELECT count(*)::bigint
                    FROM public.us_lacey_pilot_incidents AS incident
                    WHERE incident.status <> 'CLOSED'
                      AND incident.severity = 'P0'
                ),
                (
                    SELECT count(*)::bigint
                    FROM public.us_lacey_pilot_incidents AS incident
                    WHERE incident.status <> 'CLOSED'
                      AND incident.severity = 'P1'
                ),
                (
                    SELECT count(*)::bigint
                    FROM public.us_lacey_outreach_events AS event
                    WHERE event.event_name = 'EXPORT_DOWNLOADED'
                      AND event.occurred_at >= date_trunc('day', now())
                );
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.platform_admin_pilot_watch_feed(
            actor_refresh_token_hash text,
            requested_limit integer
        )
        RETURNS TABLE(
            prospect_label text,
            prospect_slug text,
            operation_public_id uuid,
            last_event_at timestamptz,
            document_count integer,
            commercial_line_count integer,
            canonical_line_count integer,
            auto_resolved_count integer,
            action_required_count integer,
            operation_status text,
            incident_severity text,
            incident_public_id uuid
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
            WITH latest_snapshot AS (
                SELECT DISTINCT ON (
                    snapshot.organization_id,
                    snapshot.operation_id
                )
                    snapshot.organization_id,
                    snapshot.operation_id,
                    snapshot.attribution_session_id,
                    snapshot.document_count,
                    snapshot.commercial_line_count,
                    snapshot.canonical_line_count,
                    snapshot.auto_resolved_count,
                    snapshot.action_required_count,
                    snapshot.created_at
                FROM public.us_lacey_pilot_quality_snapshots AS snapshot
                ORDER BY
                    snapshot.organization_id,
                    snapshot.operation_id,
                    snapshot.created_at DESC,
                    snapshot.id DESC
            )
            SELECT
                link.prospect_label::text,
                coalesce(
                    link.slug::text,
                    'sandbox-' || left(operation.public_id::text, 12)
                ) AS prospect_slug,
                operation.public_id,
                greatest(
                    latest.created_at,
                    operation.updated_at,
                    outreach_session.last_event_at
                ) AS last_event_at,
                latest.document_count,
                latest.commercial_line_count,
                latest.canonical_line_count,
                latest.auto_resolved_count,
                latest.action_required_count,
                operation.status::text,
                open_incident.severity::text,
                open_incident.incident_public_id
            FROM latest_snapshot AS latest
            JOIN public.us_lacey_operations AS operation
              ON operation.id = latest.operation_id
             AND operation.organization_id = latest.organization_id
            LEFT JOIN public.us_lacey_outreach_sessions AS outreach_session
              ON outreach_session.id = latest.attribution_session_id
            LEFT JOIN public.us_lacey_outreach_links AS link
              ON link.id = outreach_session.outreach_link_id
            LEFT JOIN LATERAL (
                SELECT
                    incident.severity,
                    incident.incident_public_id
                FROM public.us_lacey_pilot_incidents AS incident
                WHERE incident.organization_id = latest.organization_id
                  AND incident.operation_id = latest.operation_id
                  AND incident.status <> 'CLOSED'
                ORDER BY
                    CASE incident.severity WHEN 'P0' THEN 0 ELSE 1 END,
                    incident.created_at DESC,
                    incident.id DESC
                LIMIT 1
            ) AS open_incident ON true
            ORDER BY last_event_at DESC, operation.public_id
            LIMIT least(greatest(coalesce(requested_limit, 50), 1), 200);
        END;
        $$;
        """
    )

    for signature in (METRICS_FUNCTION, FEED_FUNCTION):
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
    op.execute(f"DROP FUNCTION IF EXISTS {FEED_FUNCTION}")
    op.execute(f"DROP FUNCTION IF EXISTS {METRICS_FUNCTION}")
    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()
