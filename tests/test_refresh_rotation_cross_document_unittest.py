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


def test_cross_document_refresh_marker_is_non_sensitive_and_does_not_expire_into_replay() -> None:
    source = COORDINATOR.read_text(encoding="utf-8")

    assert 'COORDINATION_COOKIE_NAME = "lt_refresh_inflight"' in source
    assert "COORDINATION_MAX_AGE_SECONDS" not in source
    assert "COORDINATION_WAIT_SECONDS" in source
    assert '"SameSite=Strict"' in source
    assert 'window.location.protocol === "https:" ? "; Secure" : ""' in source
    assert "`${COORDINATION_COOKIE_NAME}=1`" in source

    marker_function = source.split(
        "function markRefreshInFlight()",
        1,
    )[1].split(
        "function clearRefreshInFlight()",
        1,
    )[0]
    assert "Max-Age=" not in marker_function

    assert "waitForOutstandingRefresh(rawFetch)" in source
    assert "refreshedCookieJarHasFutureRenewalWindow" in source
    assert 'return "ambiguous";' in source
    assert "Refresh outcome ambiguous" in source
    assert "response.status >= 500" in source
    assert "markRefreshInFlight();" in source

    sent_request = source.split("markRefreshInFlight();", 1)[1]
    assert "catch (_error)" in sent_request
    assert "finally" not in sent_request

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


def test_logout_revocation_relocks_parent_and_revokes_whole_family() -> None:
    source = SESSIONS.read_text(encoding="utf-8")
    revocation = source.split("def revoke_session(", 1)[1]

    tenant_context = revocation.index(
        "set_tenant_db_context(db_session, session_lookup.organization_id)"
    )
    parent_lookup = revocation.index("session_record = _get_session_by_id(")
    row_lock = revocation.index("for_update=True", parent_lookup)
    family_revoke = revocation.index("_revoke_family(")

    assert tenant_context < parent_lookup < row_lock < family_revoke
    assert "family_id=session_record.family_id" in revocation
    assert "organization_id=session_record.organization_id" in revocation
