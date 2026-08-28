"""Server-rendered friction-zero entry point for Assurance v1."""
from __future__ import annotations

from fastapi import Request, status
from fastapi.responses import HTMLResponse

from litoral_trace.assurance.feature_flags import get_assurance_feature_flags
from litoral_trace.auth.rbac import Permission
from litoral_trace.web.runtime import get_html_route_user, render_web_template


def _workspace_enabled() -> bool:
    flags = get_assurance_feature_flags()
    return bool(flags.assurance_v1 and flags.document_intelligence)


async def render_assurance_workspace(request: Request) -> HTMLResponse:
    user, denied = get_html_route_user(
        request,
        required_permission=Permission.VAULT_UPLOAD,
    )
    if denied is not None:
        return denied
    if not _workspace_enabled():
        return HTMLResponse(content="Not Found", status_code=status.HTTP_404_NOT_FOUND)
    return render_web_template(
        request,
        "assurance_workspace.html",
        user=user,
        context={
            "assurance_upload_url": "/api/v1/assurance/documents",
            "assurance_workspace_enabled": True,
        },
    )
