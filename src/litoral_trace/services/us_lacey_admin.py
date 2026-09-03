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
