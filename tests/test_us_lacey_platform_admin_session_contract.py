from __future__ import annotations

from pathlib import Path


def test_us_lacey_opaque_session_uses_platform_user_sessions_table():
    migration = Path(
        "alembic/versions/037_add_us_lacey_portal_auth_functions.py"
    ).read_text(encoding="utf-8")
    assert "INSERT INTO public.user_sessions" in migration
    assert "FROM public.user_sessions AS sessions" in migration
    assert "us_lacey_portal_session_lookup" in migration


def test_control_plane_consumes_hash_of_the_same_raw_session_token():
    service = Path("src/litoral_trace/services/us_lacey_admin.py").read_text(
        encoding="utf-8"
    )
    assert "_require_platform_refresh_token_hash(refresh_token)" in service
    assert "actor_refresh_token_hash" in service
