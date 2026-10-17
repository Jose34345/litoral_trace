from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from litoral_trace.us_lacey.db import reset_us_lacey_engine_state
from litoral_trace.us_lacey.pilot_watchdog import run_lacey_pilot_watchdog
from litoral_trace.us_lacey.worker_db import reset_us_lacey_worker_engine_state


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_POSTGRES_TEST_DATABASE_URL")
    or not os.environ.get("US_LACEY_DATABASE_URL")
    or not os.environ.get("US_LACEY_WORKER_DATABASE_URL"),
    reason="requires isolated U.S. PostgreSQL runtime and worker credentials",
)


def _root_engine():
    return create_engine(
        os.environ["US_LACEY_POSTGRES_TEST_DATABASE_URL"],
        pool_pre_ping=True,
        hide_parameters=True,
    )


def test_watchdog_scans_only_attributed_stalls_and_deduplicates_incident():
    reset_us_lacey_engine_state()
    reset_us_lacey_worker_engine_state()
    root = _root_engine()
    suffix = uuid4().hex[:12]
    now = datetime.now(timezone.utc)
    session_id = uuid4()
    organization_id: int | None = None
    operation_id: int | None = None
    link_id: int | None = None

    try:
        with root.begin() as connection:
            link_id = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.us_lacey_outreach_links(
                            slug,
                            prospect_label,
                            campaign_code,
                            source,
                            active,
                            click_count
                        ) VALUES (
                            :slug,
                            'Pilot Watchdog Gate',
                            'pilot-watchdog-gate',
                            'direct_outreach',
                            true,
                            0
                        )
                        RETURNING id
                        """
                    ),
                    {"slug": f"watchdog-{suffix}"},
                ).scalar_one()
            )
            connection.execute(
                text(
                    """
                    INSERT INTO public.us_lacey_outreach_sessions(
                        id,
                        outreach_link_id,
                        prospect_label,
                        campaign_code,
                        source,
                        first_seen_at,
                        last_event_at
                    ) VALUES (
                        :session_id,
                        :link_id,
                        'Pilot Watchdog Gate',
                        'pilot-watchdog-gate',
                        'direct_outreach',
                        now(),
                        now()
                    )
                    """
                ),
                {"session_id": session_id, "link_id": link_id},
            )
            organization_id = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.organizations(
                            name,
                            slug,
                            tier,
                            is_active,
                            is_sandbox,
                            sandbox_attribution_session_id
                        ) VALUES (
                            :name,
                            :slug,
                            'pro',
                            true,
                            false,
                            :session_id
                        )
                        RETURNING id
                        """
                    ),
                    {
                        "name": f"Pilot Watchdog {suffix}",
                        "slug": f"pilot-watchdog-{suffix}",
                        "session_id": session_id,
                    },
                ).scalar_one()
            )
            connection.execute(
                text(
                    """
                    UPDATE public.us_lacey_outreach_sessions
                    SET sandbox_organization_id = :organization_id
                    WHERE id = :session_id
                    """
                ),
                {
                    "organization_id": organization_id,
                    "session_id": session_id,
                },
            )
            operation_id = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.us_lacey_operations(
                            public_id,
                            organization_id,
                            client_reference,
                            status,
                            document_count,
                            merchandise_line_count,
                            created_at,
                            updated_at
                        ) VALUES (
                            :public_id,
                            :organization_id,
                            :client_reference,
                            'PROCESSING',
                            3,
                            0,
                            :created_at,
                            :updated_at
                        )
                        RETURNING id
                        """
                    ),
                    {
                        "public_id": uuid4(),
                        "organization_id": organization_id,
                        "client_reference": f"WATCHDOG-{suffix}",
                        "created_at": now - timedelta(minutes=20),
                        "updated_at": now - timedelta(minutes=11),
                    },
                ).scalar_one()
            )

        first = run_lacey_pilot_watchdog(now=now)
        second = run_lacey_pilot_watchdog(now=now + timedelta(seconds=61))

        assert first.failure_count == 0
        assert second.failure_count == 0
        assert first.scanned_count >= 1
        assert second.scanned_count >= 1

        with root.connect() as connection:
            snapshots = connection.execute(
                text(
                    """
                    SELECT trigger, attribution_session_id
                    FROM public.us_lacey_pilot_quality_snapshots
                    WHERE organization_id = :organization_id
                      AND operation_id = :operation_id
                    ORDER BY id
                    """
                ),
                {
                    "organization_id": organization_id,
                    "operation_id": operation_id,
                },
            ).mappings().all()
            incidents = connection.execute(
                text(
                    """
                    SELECT
                        detector_code,
                        severity,
                        status,
                        fingerprint,
                        diagnostic_manifest
                    FROM public.us_lacey_pilot_incidents
                    WHERE organization_id = :organization_id
                      AND operation_id = :operation_id
                    """
                ),
                {
                    "organization_id": organization_id,
                    "operation_id": operation_id,
                },
            ).mappings().all()
            privileges = connection.execute(
                text(
                    """
                    SELECT
                        has_function_privilege(
                            'litoral_trace_app',
                            'public.us_lacey_pilot_watchdog_candidates(timestamp with time zone)',
                            'EXECUTE'
                        ) AS runtime_execute,
                        has_function_privilege(
                            'litoral_trace_worker_executor',
                            'public.us_lacey_pilot_watchdog_candidates(timestamp with time zone)',
                            'EXECUTE'
                        ) AS worker_execute
                    """
                )
            ).mappings().one()

        assert len(snapshots) == 2
        assert all(row["trigger"] == "WATCHDOG" for row in snapshots)
        assert all(row["attribution_session_id"] == session_id for row in snapshots)

        assert len(incidents) == 1
        incident = incidents[0]
        assert incident["detector_code"] == "PROCESSING_STALLED"
        assert incident["severity"] == "P0"
        assert incident["status"] == "OPEN"
        assert len(incident["fingerprint"]) == 64
        assert incident["diagnostic_manifest"]["trigger"] == "WATCHDOG"
        assert incident["diagnostic_manifest"]["detector"] == {
            "code": "PROCESSING_STALLED",
            "severity": "P0",
        }
        assert dict(privileges) == {
            "runtime_execute": False,
            "worker_execute": True,
        }
    finally:
        if operation_id is not None or organization_id is not None or link_id is not None:
            with root.begin() as connection:
                if operation_id is not None:
                    connection.execute(
                        text(
                            "DELETE FROM public.us_lacey_pilot_incidents "
                            "WHERE operation_id = :operation_id"
                        ),
                        {"operation_id": operation_id},
                    )
                    connection.execute(
                        text(
                            "DELETE FROM public.us_lacey_pilot_quality_snapshots "
                            "WHERE operation_id = :operation_id"
                        ),
                        {"operation_id": operation_id},
                    )
                    connection.execute(
                        text(
                            "DELETE FROM public.us_lacey_operations "
                            "WHERE id = :operation_id"
                        ),
                        {"operation_id": operation_id},
                    )
                if organization_id is not None:
                    connection.execute(
                        text(
                            "UPDATE public.organizations "
                            "SET sandbox_attribution_session_id = NULL "
                            "WHERE id = :organization_id"
                        ),
                        {"organization_id": organization_id},
                    )
                    connection.execute(
                        text(
                            "UPDATE public.us_lacey_outreach_sessions "
                            "SET sandbox_organization_id = NULL "
                            "WHERE id = :session_id"
                        ),
                        {"session_id": session_id},
                    )
                connection.execute(
                    text(
                        "DELETE FROM public.us_lacey_outreach_sessions "
                        "WHERE id = :session_id"
                    ),
                    {"session_id": session_id},
                )
                if link_id is not None:
                    connection.execute(
                        text(
                            "DELETE FROM public.us_lacey_outreach_links "
                            "WHERE id = :link_id"
                        ),
                        {"link_id": link_id},
                    )
                if organization_id is not None:
                    connection.execute(
                        text(
                            "DELETE FROM public.organizations "
                            "WHERE id = :organization_id"
                        ),
                        {"organization_id": organization_id},
                    )
        root.dispose()
        reset_us_lacey_worker_engine_state()
        reset_us_lacey_engine_state()
