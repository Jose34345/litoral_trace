"""Jinja-backed views for the organization-wide U.S. Lacey audit log."""
from __future__ import annotations

from litoral_trace.web.templates import templates


def render_audit_log_list(
    *,
    request,
    identity,
    entitlement,
    events,
    filters,
    event_type_options,
    date_range_options,
    pagination=None,
) -> str:
    return templates.get_template("us_lacey/audit_log_list.html").render(
        request=request,
        identity=identity,
        entitlement=entitlement,
        events=events,
        filters=filters,
        event_type_options=event_type_options,
        date_range_options=date_range_options,
        pagination=pagination or {
            "page": 1,
            "page_size": max(1, len(events)),
            "total_results": len(events),
            "total_pages": 1,
            "start": 0 if not events else 1,
            "end": len(events),
        },
    )
