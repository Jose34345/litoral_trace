"""Superadmin-only control surface on the deployed U.S. Lacey hostname.

Migration 037 stores the U.S. opaque session in ``public.user_sessions``, the
same persistent session table validated by the 042/044 platform control plane.
Admin requests reuse that U.S. session and invoke only the reviewed
SECURITY DEFINER capabilities through the existing isolated U.S. runtime
database session. Those capabilities validate the persisted platform-admin role
before returning cross-tenant data or applying a mutation.

No generic DATABASE_URL alias, generic JWT, synthetic bridge session, direct
cross-tenant table access, or direct runtime SELECT on protected identity tables
is introduced.
"""
from __future__ import annotations

import secrets
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Cookie, Form, HTTPException, Query, Request, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

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
from litoral_trace.us_lacey.impersonation_db import (
    IMPERSONATION_COOKIE,
    hash_impersonation_token,
)
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
    request: Request,
    title: str,
    message: str,
    return_href: str,
    return_label: str,
    status_code: int,
) -> HTMLResponse:
    content = templates.get_template("us_lacey/admin_message.html").render(
        request=request,
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


def _access_denied(request: Request) -> HTMLResponse:
    return _admin_message(
        request=request,
        title="Access denied",
        message="This account does not have platform-administration access.",
        return_href="/operations",
        return_label="Return to operations",
        status_code=status.HTTP_403_FORBIDDEN,
    )


def _safe_error(
    request: Request,
    message: str,
    *,
    status_code: int = 400,
) -> HTMLResponse:
    return _admin_message(
        request=request,
        title="Admin action unavailable",
        message=message,
        return_href="/admin",
        return_label="Return to admin",
        status_code=status_code,
    )


def _platform_admin_refresh_token(us_session: str) -> str:
    """Reuse a valid U.S. session; DB capabilities enforce PLATFORM_ADMIN."""
    resolve_us_lacey_session(us_session)
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


def start_readonly_impersonation_superadmin(
    *,
    refresh_token: str,
    organization_id: int,
    reason: str,
    token_hash: str,
) -> dict[str, Any]:
    return _control_plane_call(
        refresh_token=refresh_token,
        statement=(
            "SELECT * FROM "
            "public.platform_admin_start_readonly_impersonation("
            ":actor_refresh_token_hash, :organization_id, :reason, :token_hash)"
        ),
        values={
            "organization_id": organization_id,
            "reason": reason,
            "token_hash": token_hash,
        },
        commit=True,
    )[0]


def end_readonly_impersonation_superadmin(
    *,
    refresh_token: str,
    token_hash: str,
) -> None:
    _control_plane_call(
        refresh_token=refresh_token,
        statement=(
            "SELECT public.platform_admin_end_readonly_impersonation("
            ":actor_refresh_token_hash, :token_hash)"
        ),
        values={"token_hash": token_hash},
        commit=True,
    )


def list_sandbox_conversion_cohorts_superadmin(
    *,
    refresh_token: str,
    from_ts: datetime | None = None,
    to_ts: datetime | None = None,
) -> list[dict[str, Any]]:
    return _control_plane_call(
        refresh_token=refresh_token,
        statement=(
            "SELECT * FROM "
            "public.platform_admin_sandbox_conversion_cohorts("
            ":actor_refresh_token_hash, :from_ts, :to_ts)"
        ),
        values={
            "from_ts": from_ts,
            "to_ts": to_ts,
        },
    )


def convert_sandbox_to_commercial_superadmin(
    *,
    refresh_token: str,
    organization_id: int,
) -> dict[str, Any]:
    return _control_plane_call(
        refresh_token=refresh_token,
        statement=(
            "SELECT * FROM "
            "public.platform_admin_convert_sandbox_to_commercial("
            ":actor_refresh_token_hash, :organization_id)"
        ),
        values={"organization_id": organization_id},
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
        "impersonate_csrf": {
            int(account["organization_id"]): us_lacey_csrf_token(
                session_token=us_session,
                purpose=(
                    "platform-admin-impersonate:"
                    f"{int(account['organization_id'])}"
                ),
            )
            for account in accounts
        },
        "impersonation_end_csrf": us_lacey_csrf_token(
            session_token=us_session,
            purpose="platform-admin-impersonation-end",
        ),
    }


@router.get("/admin/api/us-lacey/analytics/sandbox-conversion")
def platform_admin_sandbox_conversion_analytics(
    response: Response,
    from_day: date | None = Query(default=None),
    to_day: date | None = Query(default=None),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    if from_day is not None and to_day is not None and to_day < from_day:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="to_day must be greater than or equal to from_day",
        )

    if to_day == date.max:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="to_day is outside the supported range",
        )

    try:
        session_token = _require_us_session(us_session)
        refresh_token = _platform_admin_refresh_token(session_token)
    except UsLaceyPortalAuthError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required.",
        ) from exc

    from_ts = (
        datetime.combine(from_day, time.min, tzinfo=timezone.utc)
        if from_day is not None
        else None
    )
    to_ts = (
        datetime.combine(
            to_day + timedelta(days=1),
            time.min,
            tzinfo=timezone.utc,
        )
        if to_day is not None
        else None
    )

    cohorts = list_sandbox_conversion_cohorts_superadmin(
        refresh_token=refresh_token,
        from_ts=from_ts,
        to_ts=to_ts,
    )

    response.headers["Cache-Control"] = "no-store, max-age=0"
    return {"cohorts": cohorts}


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
            return _access_denied(request)
        raise


@router.post("/admin/us-lacey/accounts/{organization_id}/status")
def platform_admin_set_status(
    request: Request,
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
                request,
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
        return _safe_error(
            request,
            "The admin form expired. Refresh and try again.",
            status_code=403,
        )


@router.post("/admin/us-lacey/accounts/{organization_id}/operation-limit")
def platform_admin_set_limit(
    request: Request,
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
        return _safe_error(
            request,
            "The admin form expired. Refresh and try again.",
            status_code=403,
        )


@router.post("/admin/us-lacey/accounts/{organization_id}/reset-pilot")
def platform_admin_reset_pilot(
    request: Request,
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
        return _safe_error(
            request,
            "The admin form expired. Refresh and try again.",
            status_code=403,
        )


@router.post("/admin/users/{user_id}/revoke-sessions")
def platform_admin_revoke_sessions(
    request: Request,
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
        return _safe_error(
            request,
            "The admin form expired. Refresh and try again.",
            status_code=403,
        )

@router.post("/admin/accounts/{organization_id}/impersonate")
def platform_admin_start_impersonation(
    request: Request,
    organization_id: int,
    reason: str = Form(...),
    csrf_token: str = Form(...),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    try:
        session_token = _require_us_session(us_session)
        verify_us_lacey_csrf(
            session_token=session_token,
            purpose=f"platform-admin-impersonate:{organization_id}",
            submitted_token=csrf_token,
        )

        refresh_token = _platform_admin_refresh_token(session_token)
        raw_impersonation_token = secrets.token_urlsafe(32)
        token_hash = hash_impersonation_token(raw_impersonation_token)

        start_readonly_impersonation_superadmin(
            refresh_token=refresh_token,
            organization_id=organization_id,
            reason=reason,
            token_hash=token_hash,
        )

        response = RedirectResponse(
            "/admin/impersonation/operations",
            status_code=status.HTTP_303_SEE_OTHER,
        )
        response.set_cookie(
            IMPERSONATION_COOKIE,
            raw_impersonation_token,
            max_age=15 * 60,
            httponly=True,
            secure=True,
            samesite="strict",
            path="/admin",
        )
        return response
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyCsrfError:
        return _safe_error(
            request,
            "The admin form expired. Refresh and try again.",
            status_code=status.HTTP_403_FORBIDDEN,
        )


@router.post("/admin/impersonation/end")
def platform_admin_end_impersonation(
    request: Request,
    csrf_token: str = Form(...),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
    impersonation_token: str | None = Cookie(
        None,
        alias=IMPERSONATION_COOKIE,
    ),
):
    try:
        session_token = _require_us_session(us_session)
        verify_us_lacey_csrf(
            session_token=session_token,
            purpose="platform-admin-impersonation-end",
            submitted_token=csrf_token,
        )

        if not impersonation_token:
            return _safe_error(
                request,
                "No active read-only impersonation session was found.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        refresh_token = _platform_admin_refresh_token(session_token)
        token_hash = hash_impersonation_token(impersonation_token)
        end_readonly_impersonation_superadmin(
            refresh_token=refresh_token,
            token_hash=token_hash,
        )

        response = RedirectResponse(
            "/admin?notice=Read-only%20impersonation%20ended",
            status_code=status.HTTP_303_SEE_OTHER,
        )
        response.delete_cookie(
            IMPERSONATION_COOKIE,
            path="/admin",
            secure=True,
            httponly=True,
            samesite="strict",
        )
        return response
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyCsrfError:
        return _safe_error(
            request,
            "The admin form expired. Refresh and try again.",
            status_code=status.HTTP_403_FORBIDDEN,
        )

