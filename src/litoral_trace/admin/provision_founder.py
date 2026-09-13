"""Promote one already-authenticated founder identity without handling passwords."""
from __future__ import annotations

import argparse
import os
import sys

from sqlalchemy import text

from litoral_trace.db.engine import get_db_session
from litoral_trace.services.admin import _require_platform_refresh_token_hash


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Provision an existing platform user as founder superadmin.")
    parser.add_argument("--email", required=True)
    parser.add_argument("--set-pilot", action="store_true")
    args = parser.parse_args(argv)
    session_token = os.environ.get("PLATFORM_PROVISIONER_REFRESH_TOKEN")
    if not session_token:
        parser.error("PLATFORM_PROVISIONER_REFRESH_TOKEN must contain a valid existing superadmin session")
    db = get_db_session()
    if db is None or db.get_bind() is None or db.get_bind().dialect.name != "postgresql":
        print("PostgreSQL control-plane database is required.", file=sys.stderr)
        return 2
    try:
        actor_hash = _require_platform_refresh_token_hash(session_token)
        founder = db.execute(text("SELECT * FROM public.platform_admin_promote_existing_user(:actor, :email)"), {"actor": actor_hash, "email": args.email}).mappings().one()
        if args.set_pilot:
            db.execute(text("SELECT * FROM public.platform_admin_set_us_lacey_account_status(:actor, :organization_id, 'PILOT')"), {"actor": actor_hash, "organization_id": founder["organization_id"]}).one()
        # A founder using their own session remains valid only until this final
        # call, so PILOT setup can complete before every session is revoked.
        db.execute(text("SELECT * FROM public.platform_admin_revoke_user_sessions(:actor, :user_id)"), {"actor": actor_hash, "user_id": founder["user_id"]}).one()
        db.commit()
        print(f"Founder provisioned: user_id={founder['user_id']} organization_id={founder['organization_id']} role=superadmin pilot={args.set_pilot}")
        return 0
    except Exception as exc:
        db.rollback()
        print(f"Founder provisioning failed: {exc.__class__.__name__}", file=sys.stderr)
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
