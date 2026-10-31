"""Passive 60-second watchdog for attributed U.S. Lacey pilot operations."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
from uuid import UUID

from sqlalchemy import text

from litoral_trace.us_lacey.pilot_reliability import capture_stalled_pilot_quality
from litoral_trace.us_lacey.worker_db import get_us_lacey_worker_db_session


LOGGER = logging.getLogger(__name__)
DEFAULT_STALE_AFTER_SECONDS = 10 * 60


@dataclass(frozen=True, slots=True)
class PilotWatchdogResult:
    scanned_count: int
    snapshot_count: int
    incident_count: int
    failure_count: int


def _watchdog_candidates(*, stale_before: datetime) -> tuple[dict, ...]:
    session = get_us_lacey_worker_db_session()
    try:
        rows = session.execute(
            text(
                """
                SELECT
                    organization_id,
                    operation_id,
                    operation_public_id,
                    attribution_session_id,
                    operation_updated_at
                FROM public.us_lacey_pilot_watchdog_candidates(:stale_before)
                """
            ),
            {"stale_before": stale_before},
        ).mappings().all()
        return tuple(dict(row) for row in rows)
    finally:
        session.close()


def run_lacey_pilot_watchdog(
    *,
    stale_after_seconds: int = DEFAULT_STALE_AFTER_SECONDS,
    now: datetime | None = None,
) -> PilotWatchdogResult:
    """Capture and flag attributed PROCESSING operations older than ten minutes.

    Every candidate is isolated: one malformed operation or observability bug is
    logged and skipped without preventing checks for the remaining pilots.
    """
    threshold_seconds = max(60, int(stale_after_seconds))
    observed_at = now or datetime.now(timezone.utc)
    stale_before = observed_at - timedelta(seconds=threshold_seconds)
    candidates = _watchdog_candidates(stale_before=stale_before)

    snapshot_count = 0
    incident_count = 0
    failure_count = 0

    for candidate in candidates:
        try:
            capture = capture_stalled_pilot_quality(
                organization_id=int(candidate["organization_id"]),
                operation_id=int(candidate["operation_id"]),
                attribution_session_id=UUID(
                    str(candidate["attribution_session_id"])
                ),
            )
            snapshot_count += 1
            incident_count += len(capture.incidents)
        except Exception:
            failure_count += 1
            LOGGER.exception(
                "Lacey pilot watchdog observation failed",
                extra={
                    "organization_id": candidate.get("organization_id"),
                    "operation_id": candidate.get("operation_id"),
                    "operation_public_id": str(
                        candidate.get("operation_public_id") or ""
                    ),
                },
            )

    return PilotWatchdogResult(
        scanned_count=len(candidates),
        snapshot_count=snapshot_count,
        incident_count=incident_count,
        failure_count=failure_count,
    )
