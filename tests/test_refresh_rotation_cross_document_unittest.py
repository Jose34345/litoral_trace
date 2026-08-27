from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COORDINATOR = (
    ROOT
    / "src"
    / "litoral_trace"
    / "static"
    / "src"
    / "js"
    / "session-refresh-coordination.js"
)
BASE_TEMPLATE = ROOT / "src" / "litoral_trace" / "templates" / "base.html"
SESSIONS = ROOT / "src" / "litoral_trace" / "auth" / "sessions.py"


def test_cross_document_refresh_lease_is_short_lived_and_non_sensitive() -> None:
    source = COORDINATOR.read_text(encoding="utf-8")

    assert 'COORDINATION_COOKIE_NAME = "lt_refresh_inflight"' in source
    assert "COORDINATION_MAX_AGE_SECONDS = 15" in source
    assert '"SameSite=Strict"' in source
    assert 'window.location.protocol === "https:" ? "; Secure" : ""' in source
    assert "`${COORDINATION_COOKIE_NAME}=1`" in source
    assert "Max-Age=${COORDINATION_MAX_AGE_SECONDS}" in source
    assert "waitForOutstandingRefresh" in source
    assert "refreshedCookieJarHasFutureRenewalWindow" in source
    assert 'status: 425' in source
    assert "markRefreshInFlight();" in source
    assert "clearRefreshInFlight();" in source
    assert "window.localStorage" not in source
    assert "window.sessionStorage" not in source
    assert "localStorage" not in source
    assert "sessionStorage" not in source


def test_cross_document_coordinator_wraps_refresh_before_session_renewal() -> None:
    base = BASE_TEMPLATE.read_text(encoding="utf-8")

    coordinator = "/src/js/session-refresh-coordination.js"
    renewal = "/src/js/session-renewal.js"

    assert coordinator in base
    assert renewal in base
    assert base.index(coordinator) < base.index(renewal)


def test_refresh_rotation_relocks_parent_after_tenant_context() -> None:
    source = SESSIONS.read_text(encoding="utf-8")
    start = source.index("def rotate_refresh_session(")
    end = source.index("\ndef revoke_session(", start)
    rotation = source[start:end]

    tenant_context = rotation.index(
        "set_tenant_db_context(db_session, session_lookup.organization_id)"
    )
    parent_lookup = rotation.index("current_session = _get_session_by_id(")
    row_lock = rotation.index("for_update=True", parent_lookup)
    reuse_check = rotation.index("if current_session.revoked_at is not None:")

    assert tenant_context < parent_lookup < row_lock < reuse_check
