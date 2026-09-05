"""Superadmin-only control surface on the deployed U.S. Lacey hostname.

Migration 037 stores the U.S. opaque session in ``public.user_sessions``, the
same persistent session table validated by the 042/044 platform control plane.
Admin requests verify that U.S. session and the persisted PLATFORM_ADMIN role,
then invoke only the reviewed SECURITY DEFINER capabilities through the existing
isolated U.S. runtime database session.

No generic DATABASE_URL alias, generic JWT, synthetic bridge session, or direct
cross-tenant table access is introduced.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Cookie, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from litoral_trace.auth.rbac import Permission, has_permission
from litoral_trace.db.models import Organization, User
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.admin import (
    _map_platform_db_error,
    _require_platform_refresh_token_hash,
)
from litoral_trace.us_lacey.csrf import (
    UsLaceyCsrfError,
    us_lacey_csrf_token,
    verify_us_lacey_csrf,
)
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.portal_auth import (
    US_LACEY_SESSION_COOKIE,
    UsLaceyPortalAuthError,
    resolve_us_lacey_session,
)
from litoral_trace.web.templates import templates


router = APIRouter(tags=["Platform Admin"])


def _login_redirect(*, clear_cookie: bool = False) -> RedirectResponse:
    response = RedirectResponse("/login", status_code=status.HTTP_303_SEE_OTHER)
    if clear_cookie:
        response.delete_cookie(US_LACEY_SESSION_COOKIE, path="/")
    return response


def _admin_message(
    *,
    title: str,
    message: str,
    return_href: str,
    return_label: str,
    status_code: int,
) -> HTMLResponse:
    content = templates.get_template("us_lacey/admin_message.html").render(
        authenticated=True,
        title=title,
        message=message,
        return_href=return_href,
        return_label=return_label,
    )
    return HTMLResponse(
        status_code=status_code,
        content=content,
        headers={"Cache-Control": "no-store, max-age=0"},
    )


def _access_denied() -> HTMLResponse:
    return _admin_message(
        title="Access denied",
        message="This account does not have platform-administration access.",
        return_href="/operations",
        return_label="Return to operations",
        status_code=status.HTTP_403_FORBIDDEN,
    )


def _safe_error(message: str, *, status_code: int = 400) -> HTMLResponse:
    return _admin_message(
        title="Admin action unavailable",
        message=message,
        return_href="/admin",
        return_label="Return to admin",
        status_code=status_code,
    )


def _verified_platform_admin(us_session: str):
    """Resolve the U.S. session and verify persisted PLATFORM_ADMIN capability."""
    identity = resolve_us_lacey_session(us_session)
    db = get_us_lacey_db_session()
    try:
        set_tenant_db_context(db, identity.organization_id)
        user = db.execute(
            select(User).where(
                User.id == identity.user_id,
                User.organization_id == identity.organization_id,
            )
        ).scalar_one_or_none()
        organization = db.execute(
            select(Organization).where(Organization.id == identity.organization_id)
        ).scalar_one_or_none()
        if (
            user is None
            or organization is None
            or not user.is_active
            or not organization.is_active
            or not has_permission(user.role, Permission.PLATFORM_ADMIN)
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Platform administration is not available for this account.",
            )
        return identity
    finally:
        db.close()


def _platform_admin_refresh_token(us_session: str) -> str:
    """Return the persisted U.S. opaque session after platform-role verification."""
    _verified_platform_admin(us_session)
    return us_session


def _control_plane_call(
    *,
    refresh_token: str,
    statement: str,
    values: dict[str, Any] | None = None,
    commit: bool = False,
) -> list[dict[str, Any]]:
    """Invoke one 042/044 capability through the isolated U.S. runtime session."""
    db = get_us_lacey_db_session()
    try:
        token_hash = _require_platform_refresh_token_hash(refresh_token)
        parameters = {
            **(values or {}),
            "actor_refresh_token_hash": token_hash,
        }
        rows = db.execute(text(statement), parameters).mappings().all()
        if commit:
            db.commit()
        return [dict(row) for row in rows]
    except DBAPIError as exc:
        db.rollback()
        _map_platform_db_error(exc)
        raise
    finally:
        db.close()


def list_us_lacey_accounts_superadmin(*, refresh_token: str) -> list[dict[str, Any]]:
    return _control_plane_call(
        refresh_token=refresh_token,
        statement=(
            "SELECT * FROM public.platform_us_lacey_account_overview("
            ":actor_refresh_token_hash) ORDER BY organization_id"
        ),
    )


def list_platform_users_superadmin(*, refresh_token: str) -> list[dict[str, Any]]:
    return _control_plane_call(
        refresh_token=refresh_token,
        statement=(
            "SELECT * FROM public.platform_admin_users("
            ":actor_refresh_token_hash)"
        ),
    )


def list_failed_jobs_superadmin(*, refresh_token: str) -> list[dict[str, Any]]:
    return _control_plane_call(
        refresh_token=refresh_token,
        statement=(
            "SELECT * FROM public.platform_admin_failed_jobs("
            ":actor_refresh_token_hash)"
        ),
    )


def set_us_lacey_account_status_superadmin(
    *, refresh_token: str, organization_id: int, account_status: str
) -> dict[str, Any]:
    return _control_plane_call(
        refresh_token=refresh_token,
        statement=(
            "SELECT * FROM public.platform_admin_set_us_lacey_account_status("
            ":actor_refresh_token_hash, :organization_id, :account_status)"
        ),
        values={
            "organization_id": organization_id,
            "account_status": account_status,
        },
        commit=True,
    )[0]


def set_us_lacey_operation_limit_superadmin(
    *, refresh_token: str, organization_id: int, monthly_operation_limit: int
) -> dict[str, Any]:
    return _control_plane_call(
        refresh_token=refresh_token,
        statement=(
            "SELECT * FROM public.platform_admin_set_us_lacey_operation_limit("
            ":actor_refresh_token_hash, :organization_id, :monthly_operation_limit)"
        ),
        values={
            "organization_id": organization_id,
            "monthly_operation_limit": monthly_operation_limit,
        },
        commit=True,
    )[0]


def reset_pilot_account_superadmin(
    *, refresh_token: str, organization_id: int
) -> dict[str, Any]:
    return _control_plane_call(
        refresh_token=refresh_token,
        statement=(
            "SELECT * FROM public.platform_admin_reset_pilot_account("
            ":actor_refresh_token_hash, :organization_id)"
        ),
        values={"organization_id": organization_id},
        commit=True,
    )[0]


def revoke_user_sessions_superadmin(
    *, refresh_token: str, user_id: int
) -> dict[str, Any]:
    return _control_plane_call(
        refresh_token=refresh_token,
        statement=(
            "SELECT * FROM public.platform_admin_revoke_user_sessions("
            ":actor_refresh_token_hash, :user_id)"
        ),
        values={"user_id": user_id},
        commit=True,
    )[0]


def _require_us_session(us_session: str | None) -> str:
    if not us_session:
        raise UsLaceyPortalAuthError("Sign in to continue.", code="session_invalid")
    resolve_us_lacey_session(us_session)
    return us_session


def _admin_context(*, request: Request, us_session: str, notice: str | None = None):
    refresh_token = _platform_admin_refresh_token(us_session)
    accounts = list_us_lacey_accounts_superadmin(refresh_token=refresh_token)
    users = list_platform_users_superadmin(refresh_token=refresh_token)
    failed_jobs = list_failed_jobs_superadmin(refresh_token=refresh_token)

    active_count = sum(1 for account in accounts if account.get("account_status") == "ACTIVE")
    pilot_count = sum(1 for account in accounts if account.get("account_status") == "PILOT")
    pending_count = sum(
        1
        for account in accounts
        if account.get("account_status") in {"PENDING_EMAIL", "PAYMENT_PENDING"}
    )
    failed_job_count = sum(int(account.get("failed_jobs") or 0) for account in accounts)

    return {
        "request": request,
        "authenticated": True,
        "accounts": accounts,
        "account_count": len(accounts),
        "active_count": active_count,
        "pilot_count": pilot_count,
        "pending_count": pending_count,
        "failed_job_count": failed_job_count,
        "users": users,
        "failed_jobs": failed_jobs,
        "notice": notice,
        "status_csrf": {
            int(account["organization_id"]): us_lacey_csrf_token(
                session_token=us_session,
                purpose=f"platform-admin-status:{int(account['organization_id'])}",
            )
            for account in accounts
        },
        "limit_csrf": {
            int(account["organization_id"]): us_lacey_csrf_token(
                session_token=us_session,
                purpose=f"platform-admin-limit:{int(account['organization_id'])}",
            )
            for account in accounts
        },
        "reset_csrf": {
            int(account["organization_id"]): us_lacey_csrf_token(
                session_token=us_session,
                purpose=f"platform-admin-reset:{int(account['organization_id'])}",
            )
            for account in accounts
        },
        "revoke_csrf": {
            int(user["user_id"]): us_lacey_csrf_token(
                session_token=us_session,
                purpose=f"platform-admin-revoke:{int(user['user_id'])}",
            )
            for user in users
        },
    }


@router.get("/admin", response_class=HTMLResponse)
def platform_admin_page(
    request: Request,
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    try:
        session_token = _require_us_session(us_session)
        context = _admin_context(
            request=request,
            us_session=session_token,
            notice=request.query_params.get("notice"),
        )
        content = templates.get_template("us_lacey/admin.html").render(**context)
        return HTMLResponse(
            content=content,
            status_code=status.HTTP_200_OK,
            headers={"Cache-Control": "no-store, max-age=0"},
        )
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return _access_denied()
        raise


@router.post("/admin/us-lacey/accounts/{organization_id}/status")
def platform_admin_set_status(
    organization_id: int,
    account_status: str = Form(...),
    csrf_token: str = Form(...),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    try:
        session_token = _require_us_session(us_session)
        verify_us_lacey_csrf(
            session_token=session_token,
            purpose=f"platform-admin-status:{organization_id}",
            submitted_token=csrf_token,
        )
        refresh_token = _platform_admin_refresh_token(session_token)
        identity = resolve_us_lacey_session(session_token)
        if (
            account_status.strip().upper() == "SUSPENDED"
            and organization_id == identity.organization_id
        ):
            return _safe_error(
                "You cannot suspend the organization that owns your current admin session.",
                status_code=status.HTTP_409_CONFLICT,
            )
        set_us_lacey_account_status_superadmin(
            refresh_token=refresh_token,
            organization_id=organization_id,
            account_status=account_status,
        )
        return RedirectResponse(
            "/admin?notice=Account%20status%20updated",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyCsrfError:
        return _safe_error("The admin form expired. Refresh and try again.", status_code=403)


@router.post("/admin/us-lacey/accounts/{organization_id}/operation-limit")
def platform_admin_set_limit(
    organization_id: int,
    monthly_operation_limit: int = Form(...),
    csrf_token: str = Form(...),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    try:
        session_token = _require_us_session(us_session)
        verify_us_lacey_csrf(
            session_token=session_token,
            purpose=f"platform-admin-limit:{organization_id}",
            submitted_token=csrf_token,
        )
        refresh_token = _platform_admin_refresh_token(session_token)
        set_us_lacey_operation_limit_superadmin(
            refresh_token=refresh_token,
            organization_id=organization_id,
            monthly_operation_limit=monthly_operation_limit,
        )
        return RedirectResponse(
            "/admin?notice=Operation%20limit%20updated",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyCsrfError:
        return _safe_error("The admin form expired. Refresh and try again.", status_code=403)


@router.post("/admin/us-lacey/accounts/{organization_id}/reset-pilot")
def platform_admin_reset_pilot(
    organization_id: int,
    csrf_token: str = Form(...),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    try:
        session_token = _require_us_session(us_session)
        verify_us_lacey_csrf(
            session_token=session_token,
            purpose=f"platform-admin-reset:{organization_id}",
            submitted_token=csrf_token,
        )
        refresh_token = _platform_admin_refresh_token(session_token)
        reset_pilot_account_superadmin(
            refresh_token=refresh_token,
            organization_id=organization_id,
        )
        return RedirectResponse(
            "/admin?notice=Pilot%20test%20data%20reset",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyCsrfError:
        return _safe_error("The admin form expired. Refresh and try again.", status_code=403)


@router.post("/admin/users/{user_id}/revoke-sessions")
def platform_admin_revoke_sessions(
    user_id: int,
    csrf_token: str = Form(...),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    try:
        session_token = _require_us_session(us_session)
        verify_us_lacey_csrf(
            session_token=session_token,
            purpose=f"platform-admin-revoke:{user_id}",
            submitted_token=csrf_token,
        )
        refresh_token = _platform_admin_refresh_token(session_token)
        revoke_user_sessions_superadmin(
            refresh_token=refresh_token,
            user_id=user_id,
        )
        return RedirectResponse(
            "/admin?notice=User%20sessions%20revoked",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyCsrfError:
        return _safe_error("The admin form expired. Refresh and try again.", status_code=403)
