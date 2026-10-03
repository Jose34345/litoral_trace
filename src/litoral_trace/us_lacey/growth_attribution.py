"""First-party commercial attribution for the zero-touch U.S. Lacey sandbox.

The growth plane stores only campaign/funnel metadata. It never persists uploaded
documents, extracted values, email addresses, IP addresses, or customer field
content. Attribution failures are intentionally fail-open for the product flow.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import logging
import re
from uuid import UUID

from sqlalchemy import text

from litoral_trace.us_lacey.db import get_us_lacey_db_session


LOGGER = logging.getLogger(__name__)

OUTREACH_ATTRIBUTION_COOKIE = "lt_lacey_outreach"
OUTREACH_ATTRIBUTION_COOKIE_MAX_AGE = 7 * 24 * 60 * 60
_OUTREACH_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{2,95}$")
_ALLOWED_EVENTS = frozenset(
    {
        "OPERATION_CREATED",
        "DOCUMENTS_UPLOADED",
        "REVIEW_REACHED",
        "AUTO_RESOLVED_CONFIRMED",
        "REVIEW_COMPLETED",
        "EXPORT_DOWNLOADED",
    }
)


class UsLaceyOutreachError(RuntimeError):
    """Safe attribution error; callers must not expose database details."""


@dataclass(frozen=True, slots=True)
class OutreachAttributionSession:
    attribution_session_id: UUID
    prospect_label: str
    campaign_code: str
    source: str


def _session_token_hash(raw_token: str) -> str:
    return hashlib.sha256(str(raw_token).encode("utf-8")).hexdigest()


def normalize_outreach_slug(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _OUTREACH_SLUG_RE.fullmatch(normalized):
        raise UsLaceyOutreachError("Outreach link is unavailable.")
    return normalized


def open_outreach_link(slug: str) -> OutreachAttributionSession:
    """Create one anonymous attribution session from a public outreach slug."""

    normalized = normalize_outreach_slug(slug)
    db = get_us_lacey_db_session()
    try:
        row = db.execute(
            text(
                """
                SELECT *
                FROM public.us_lacey_outreach_open(:slug)
                """
            ),
            {"slug": normalized},
        ).mappings().one()
        db.commit()
        return OutreachAttributionSession(
            attribution_session_id=UUID(str(row["attribution_session_id"])),
            prospect_label=str(row["prospect_label"]),
            campaign_code=str(row["campaign_code"]),
            source=str(row["source"]),
        )
    except Exception as exc:
        db.rollback()
        raise UsLaceyOutreachError("Outreach link is unavailable.") from exc
    finally:
        db.close()


def bind_outreach_to_sandbox(
    *,
    attribution_session_id: UUID | str,
    session_token: str,
    organization_id: int,
) -> bool:
    """Bind a pre-sandbox outreach click to the newly created sandbox tenant."""

    try:
        attribution_id = UUID(str(attribution_session_id))
    except (TypeError, ValueError) as exc:
        raise UsLaceyOutreachError("Outreach session is invalid.") from exc

    db = get_us_lacey_db_session()
    try:
        result = db.execute(
            text(
                """
                SELECT public.us_lacey_outreach_bind_sandbox(
                    :token_hash,
                    :organization_id,
                    :attribution_session_id
                )
                """
            ),
            {
                "token_hash": _session_token_hash(session_token),
                "organization_id": int(organization_id),
                "attribution_session_id": attribution_id,
            },
        ).scalar_one()
        db.commit()
        return bool(result)
    except Exception as exc:
        db.rollback()
        raise UsLaceyOutreachError("Unable to bind outreach attribution.") from exc
    finally:
        db.close()


def record_outreach_event(
    *,
    session_token: str,
    organization_id: int,
    event_name: str,
    event_key: str = "",
    metadata: dict[str, object] | None = None,
) -> bool:
    """Record one idempotent, non-content funnel event for an attributed sandbox."""

    normalized_event = str(event_name or "").strip().upper()
    if normalized_event not in _ALLOWED_EVENTS:
        raise UsLaceyOutreachError("Outreach event is invalid.")

    safe_metadata = metadata or {}
    serialized = json.dumps(safe_metadata, separators=(",", ":"), sort_keys=True)
    if len(serialized) > 4096:
        raise UsLaceyOutreachError("Outreach event metadata is too large.")

    db = get_us_lacey_db_session()
    try:
        result = db.execute(
            text(
                """
                SELECT public.us_lacey_outreach_record_event(
                    :token_hash,
                    :organization_id,
                    :event_name,
                    :event_key,
                    CAST(:event_metadata AS jsonb)
                )
                """
            ),
            {
                "token_hash": _session_token_hash(session_token),
                "organization_id": int(organization_id),
                "event_name": normalized_event,
                "event_key": str(event_key or "")[:128],
                "event_metadata": serialized,
            },
        ).scalar_one()
        db.commit()
        return bool(result)
    except Exception as exc:
        db.rollback()
        raise UsLaceyOutreachError("Unable to record outreach event.") from exc
    finally:
        db.close()


def safe_record_outreach_event(**kwargs: object) -> bool:
    """Best-effort wrapper: growth telemetry can never break customer workflow."""

    try:
        return record_outreach_event(**kwargs)
    except Exception:
        LOGGER.exception(
            "us_lacey_outreach_event_failed",
            extra={
                "organization_id": kwargs.get("organization_id"),
                "event_name": kwargs.get("event_name"),
            },
        )
        return False
