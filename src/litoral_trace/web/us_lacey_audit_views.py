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
) -> str:
    return templates.get_template("us_lacey/audit_log_list.html").render(
        request=request,
        identity=identity,
        entitlement=entitlement,
        events=events,
        filters=filters,
        event_type_options=event_type_options,
        date_range_options=date_range_options,
    )
