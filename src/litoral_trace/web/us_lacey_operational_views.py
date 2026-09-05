"""Jinja-backed operational workspace views for the U.S. Lacey portal."""
from __future__ import annotations

from collections.abc import Mapping, Sequence

from litoral_trace.web.templates import templates


def _render(request, name: str, **context: object) -> str:
    return templates.get_template(f"us_lacey/{name}.html").render(request=request, **context)


def render_operations(*, request, identity, operations: Sequence, entitlement) -> str:
    return _render(request, "operations", identity=identity, operations=operations, entitlement=entitlement)


def render_new_operation(*, request, identity, entitlement, csrf_token: str, error: str | None = None) -> str:
    return _render(request, "new_operation", identity=identity, entitlement=entitlement, csrf_token=csrf_token, error=error)


def render_operation_detail(*, request, identity, detail, engine2_dossier, upload_csrf: str, complete_csrf: str, review_csrf: Mapping[int, str], error: str | None = None, notice: str | None = None) -> str:
    exception_fields = [field for field in detail.fields if field.status in {"MISSING", "REVIEW"}]
    settled_fields = [field for field in detail.fields if field.status not in {"MISSING", "REVIEW"}]
    return _render(request, "operation_detail", identity=identity, detail=detail, engine2_dossier=engine2_dossier, upload_csrf=upload_csrf, complete_csrf=complete_csrf, review_csrf=review_csrf, exception_fields=exception_fields, settled_fields=settled_fields, error=error, notice=notice)
