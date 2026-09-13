"""Read-only platform-owner queries for the U.S. Lacey product."""
from __future__ import annotations

from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from litoral_trace.db.engine import get_db_session
from litoral_trace.services.admin import (
    _map_platform_db_error,
    _require_platform_refresh_token_hash,
)


def list_us_lacey_accounts_superadmin(
    *,
    refresh_token: str | None,
) -> list[dict[str, Any]]:
    """Return the curated cross-tenant U.S. account overview for a superadmin.

    PostgreSQL is mandatory because cross-tenant visibility is implemented only
    by the SECURITY DEFINER control-plane function introduced in migration 042.
    There is deliberately no direct ORM/SQLite fallback that could normalize a
    cross-tenant bypass into application code.
    """
    db_session = get_db_session()
    if db_session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    try:
        bind = db_session.get_bind()
        if bind is None or bind.dialect.name != "postgresql":
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="El panel U.S. Lacey requiere el control-plane PostgreSQL.",
            )

        token_hash = _require_platform_refresh_token_hash(refresh_token)
        rows = db_session.execute(
            text(
                """
                SELECT *
                FROM public.platform_us_lacey_account_overview(
                    :actor_refresh_token_hash
                )
                ORDER BY organization_id
                """
            ),
            {"actor_refresh_token_hash": token_hash},
        ).mappings().all()
        return [dict(row) for row in rows]
    except DBAPIError as exc:
        _map_platform_db_error(exc)
        raise
    finally:
        db_session.close()


def _platform_admin_call(refresh_token: str | None, statement: str, values: dict[str, Any]) -> list[dict[str, Any]]:
    """Call a capability-specific 044 function; never mutate tenants with ORM."""
    db_session = get_db_session()
    if db_session is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Servicio de base de datos no disponible.")
    try:
        bind = db_session.get_bind()
        if bind is None or bind.dialect.name != "postgresql":
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="El control-plane requiere PostgreSQL.")
        values = {**values, "actor_refresh_token_hash": _require_platform_refresh_token_hash(refresh_token)}
        rows = db_session.execute(text(statement), values).mappings().all()
        db_session.commit()
        return [dict(row) for row in rows]
    except DBAPIError as exc:
        db_session.rollback()
        _map_platform_db_error(exc)
        raise
    finally:
        db_session.close()


def set_us_lacey_account_status_superadmin(*, refresh_token: str | None, organization_id: int, account_status: str) -> dict[str, Any]:
    return _platform_admin_call(refresh_token, "SELECT * FROM public.platform_admin_set_us_lacey_account_status(:actor_refresh_token_hash, :organization_id, :account_status)", {"organization_id": organization_id, "account_status": account_status})[0]


def set_us_lacey_operation_limit_superadmin(*, refresh_token: str | None, organization_id: int, monthly_operation_limit: int) -> dict[str, Any]:
    return _platform_admin_call(refresh_token, "SELECT * FROM public.platform_admin_set_us_lacey_operation_limit(:actor_refresh_token_hash, :organization_id, :monthly_operation_limit)", {"organization_id": organization_id, "monthly_operation_limit": monthly_operation_limit})[0]


def revoke_user_sessions_superadmin(*, refresh_token: str | None, user_id: int) -> dict[str, Any]:
    return _platform_admin_call(refresh_token, "SELECT * FROM public.platform_admin_revoke_user_sessions(:actor_refresh_token_hash, :user_id)", {"user_id": user_id})[0]


def reset_pilot_account_superadmin(*, refresh_token: str | None, organization_id: int) -> dict[str, Any]:
    return _platform_admin_call(refresh_token, "SELECT * FROM public.platform_admin_reset_pilot_account(:actor_refresh_token_hash, :organization_id)", {"organization_id": organization_id})[0]


def list_platform_users_superadmin(*, refresh_token: str | None) -> list[dict[str, Any]]:
    return _platform_admin_call(refresh_token, "SELECT * FROM public.platform_admin_users(:actor_refresh_token_hash)", {})


def list_failed_jobs_superadmin(*, refresh_token: str | None) -> list[dict[str, Any]]:
    return _platform_admin_call(refresh_token, "SELECT * FROM public.platform_admin_failed_jobs(:actor_refresh_token_hash)", {})
