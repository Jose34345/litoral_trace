"""Explicitly opt-in, read-only Assurance V2 portal adapter.

Never mount an independent authentication surface. SHADOW mode exposes a
tenant-authenticated, deny-by-default evidence case without enabling decisions
or final filings. ENABLED requires a separate release gate and is intentionally
not accepted while the transactional review/export wiring is unfinished.
"""
from __future__ import annotations

import hmac
import os

from fastapi import FastAPI, HTTPException, Request

from litoral_trace.us_lacey.access import (
    UsLaceyOperationalAccessError, require_us_lacey_operational_access,
)
from litoral_trace.us_lacey.assurance_v2_case_reader import read_tenant_assurance_case
from litoral_trace.us_lacey.csrf import us_lacey_csrf_token
from litoral_trace.us_lacey.exporters.assurance_v2_package import (
    PostgresOperationEventPackageStore,
)
from litoral_trace.us_lacey.portal_auth import (
    US_LACEY_SESSION_COOKIE, UsLaceyPortalAuthError,
    resolve_us_lacey_session,
)
from litoral_trace.web.us_lacey_assurance_v2_views import (
    WorkspacePrincipal, create_assurance_v2_router,
)


def configure_assurance_v2_shadow(app: FastAPI, *, mode: str | None = None) -> bool:
    """Install only tenant-scoped read projection; final authority stays OFF."""
    setting = (mode if mode is not None
               else os.environ.get("US_LACEY_ASSURANCE_V2_MODE", "OFF"))
    normalized = str(setting).strip().upper()
    if normalized in {"", "OFF", "DISABLED"}:
        return False
    if normalized != "SHADOW":
        raise ValueError("Assurance V2 final release is blocked until full E2E acceptance")

    def principal_for(request: Request) -> WorkspacePrincipal:
        cookie = request.cookies.get(US_LACEY_SESSION_COOKIE)
        if not cookie:
            raise HTTPException(401, "Authentication required")
        try:
            identity = resolve_us_lacey_session(cookie)
            require_us_lacey_operational_access(
                organization_id=int(identity.organization_id),
                require_mutation_access=False,
            )
        except (UsLaceyPortalAuthError, UsLaceyOperationalAccessError):
            raise HTTPException(401, "Session unavailable") from None
        operation_id = str(request.path_params.get("operation_id") or "")
        csrf = us_lacey_csrf_token(
            session_token=cookie, purpose=f"assurance-v2:{operation_id}",
        )
        return WorkspacePrincipal(
            organization_id=int(identity.organization_id),
            actor_id=str(identity.user_id),
            csrf_token=csrf,
        )

    def rejected_mutation(principal, operation_id, command):
        raise HTTPException(
            status_code=503,
            detail="Assurance V2 authority is shadow-only; review through current workflow.",
        )

    router = create_assurance_v2_router(
        principal_for=principal_for,
        case_for=lambda principal, operation_id: read_tenant_assurance_case(
            organization_id=principal.organization_id,
            operation_public_id=operation_id,
        ),
        decision_gateway=rejected_mutation,
        csrf_verify=lambda principal, token: hmac.compare_digest(
            principal.csrf_token, str(token),
        ),
        package_store=PostgresOperationEventPackageStore(),
        # A source-document URL must be resolved through existing tenant auth;
        # do not produce an invented or generic downloadable path.
        document_link_for=lambda principal, operation_id, doc, page: None,
    )
    app.include_router(router)
    return True
