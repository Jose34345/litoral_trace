"""Server-rendered Assurance attention workspace."""
from __future__ import annotations

from fastapi import Request, status
from fastapi.responses import HTMLResponse

from litoral_trace.assurance.feature_flags import get_assurance_feature_flags
from litoral_trace.assurance.operational_exceptions import (
    AssuranceOperationalExceptionError,
    AssuranceOperationalExceptionService,
)
from litoral_trace.auth.rbac import Permission, has_permission
from litoral_trace.web.runtime import get_html_route_user, render_web_template


def _attention_enabled() -> bool:
    flags = get_assurance_feature_flags()
    return bool(flags.assurance_v1 and flags.operational_exceptions)


def _attention_context(rows, *, can_operate: bool, error: str | None = None) -> dict:
    exceptions = tuple(
        {
            "public_id": str(row.public_id),
            "operation_reference": row.operation_reference,
            "cause_code": row.cause_code,
            "title": row.title,
            "description": row.description,
            "impact": row.impact,
            "priority": row.priority,
            "status": row.status,
            "assigned_to_name": row.assigned_to_name or "Sin asignar",
            "due_at": (
                row.due_at.strftime("%d/%m/%Y %H:%M")
                if row.due_at is not None
                else "Sin fecha límite"
            ),
            "recommended_action": row.recommended_action,
            "source_type": row.source_type,
            "source_reference": row.source_reference or "—",
        }
        for row in rows
    )
    return {
        "attention": {
            "exceptions": exceptions,
            "open_count": len(exceptions),
            "critical_count": sum(1 for row in exceptions if row["priority"] == "CRITICAL"),
            "high_count": sum(1 for row in exceptions if row["priority"] == "HIGH"),
            "unassigned_count": sum(
                1 for row in exceptions if row["assigned_to_name"] == "Sin asignar"
            ),
            "can_operate": can_operate,
            "error": error,
        }
    }


async def render_assurance_attention(request: Request) -> HTMLResponse:
    """Show only actionable tenant exceptions; hidden/fail-closed when disabled."""
    user, denied = get_html_route_user(
        request,
        required_permission=Permission.VAULT_READ,
    )
    if denied is not None:
        return denied

    if not _attention_enabled():
        return HTMLResponse(
            content="Not Found",
            status_code=status.HTTP_404_NOT_FOUND,
        )

    service = AssuranceOperationalExceptionService()
    try:
        # Reconciliation is LT-owned evidence, so refresh it before presenting
        # the queue. Preflight reasons are refreshed whenever preflight runs.
        flags = get_assurance_feature_flags()
        if flags.reconciliation:
            service.sync_reconciliation(organization_id=user.organization_id)
        rows = service.list_attention(organization_id=user.organization_id)
        return render_web_template(
            request,
            "assurance_attention.html",
            user=user,
            context=_attention_context(
                rows,
                can_operate=has_permission(user, Permission.TRACEABILITY_OPERATE),
            ),
        )
    except AssuranceOperationalExceptionError:
        return render_web_template(
            request,
            "assurance_attention.html",
            user=user,
            context=_attention_context(
                (),
                can_operate=has_permission(user, Permission.TRACEABILITY_OPERATE),
                error="La bandeja de excepciones no está disponible temporalmente.",
            ),
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
