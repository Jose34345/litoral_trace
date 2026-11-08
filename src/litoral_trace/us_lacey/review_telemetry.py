"""Non-authoritative UX telemetry for U.S. Lacey human review."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


MAX_REVIEW_SECONDS = 7 * 24 * 60 * 60
MAX_TRACKED_FIELDS = 200


@dataclass(frozen=True, slots=True)
class ReviewTelemetry:
    started_at: str | None
    elapsed_seconds: int | None
    modified_field_ids: tuple[int, ...]


def _safe_started_at(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None

    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc).isoformat()


def _safe_elapsed(value: int | str | None) -> int | None:
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return None

    if seconds < 0 or seconds > MAX_REVIEW_SECONDS:
        return None
    return seconds


def _safe_field_ids(value: str | None) -> tuple[int, ...]:
    result: list[int] = []
    seen: set[int] = set()

    for raw in str(value or "").split(","):
        try:
            field_id = int(raw.strip())
        except (TypeError, ValueError):
            continue

        if field_id <= 0 or field_id in seen:
            continue

        seen.add(field_id)
        result.append(field_id)
        if len(result) >= MAX_TRACKED_FIELDS:
            break

    return tuple(result)


def parse_review_telemetry(
    *,
    started_at: str | None,
    elapsed_seconds: int | str | None,
    modified_field_ids: str | None,
) -> ReviewTelemetry:
    """Normalize analytics input without ever participating in review authority."""
    return ReviewTelemetry(
        started_at=_safe_started_at(started_at),
        elapsed_seconds=_safe_elapsed(elapsed_seconds),
        modified_field_ids=_safe_field_ids(modified_field_ids),
    )
