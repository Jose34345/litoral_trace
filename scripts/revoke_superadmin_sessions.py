"""One-off session revocation script for the DB-backed platform superadmin.

This script is intentionally not executed by the test suite or by application
startup. Run it manually only after loading MIGRATION_DATABASE_URL.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy import func, select
from sqlalchemy.orm import Session

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.models import User, UserSession


TARGET_USERNAME = "admin"
TARGET_ROLE = "superadmin"


def _get_migration_database_url() -> str:
    raw_url = os.environ.get("MIGRATION_DATABASE_URL", "").strip()
    if not raw_url:
        raise RuntimeError("MIGRATION_DATABASE_URL es obligatorio para esta operacion.")
    return normalize_database_url(raw_url)


def _load_target_superadmin(session: Session) -> User:
    matches = session.execute(
        select(User).where(
            User.username == TARGET_USERNAME,
            User.role == TARGET_ROLE,
        )
    ).scalars().all()

    if len(matches) != 1:
        raise RuntimeError(
            "La revocacion requiere exactamente un usuario admin/superadmin."
        )

    return matches[0]


def main() -> None:
    engine = create_engine(_get_migration_database_url(), pool_pre_ping=True)
    try:
        with Session(engine) as session:
            target_user = _load_target_superadmin(session)
            active_sessions = session.execute(
                select(UserSession).where(
                    UserSession.user_id == target_user.id,
                    UserSession.revoked_at.is_(None),
                )
            ).scalars().all()

            for user_session in active_sessions:
                user_session.revoked_at = func.now()

            session.commit()
            revoked_count = len(active_sessions)
    finally:
        engine.dispose()

    print(f"Sesiones activas revocadas: {revoked_count}")


if __name__ == "__main__":
    main()
