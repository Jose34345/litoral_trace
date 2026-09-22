from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID

from fastapi.testclient import TestClient

import litoral_trace.us_lacey.growth_attribution as growth
import litoral_trace.web.us_lacey_sandbox as sandbox_web
from litoral_trace.us_lacey.growth_attribution import (
    OUTREACH_ATTRIBUTION_COOKIE,
    OutreachAttributionSession,
    UsLaceyOutreachError,
)
from litoral_trace.us_lacey.sandbox import UsLaceySandboxSession
from litoral_trace.web.us_lacey_unified_app import app


client = TestClient(app)
ATTRIBUTION_ID = UUID("3d434cf8-3d1f-4aec-b159-880032d9d66d")


def _sandbox_session() -> UsLaceySandboxSession:
    return UsLaceySandboxSession(
        organization_id=701,
        user_id=702,
        session_id=703,
        session_token="sandbox-attribution-token",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=4),
    )


def test_outreach_referral_sets_http_only_first_party_cookie(monkeypatch):
    client.cookies.clear()
    monkeypatch.setattr(
        sandbox_web,
        "load_us_lacey_portal_config",
        lambda: SimpleNamespace(session_cookie_secure=False),
    )
    monkeypatch.setattr(
        sandbox_web,
        "open_outreach_link",
        lambda _slug: OutreachAttributionSession(
            attribution_session_id=ATTRIBUTION_ID,
            prospect_label="Concannon Lumber",
            campaign_code="concannon-sep26",
            source="direct_outreach",
        ),
    )

    response = client.get(
        "/sandbox/ref/concannon-sep26-a1b2c3",
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/sandbox/start"
    cookie = response.headers["set-cookie"]
    assert f"{OUTREACH_ATTRIBUTION_COOKIE}={ATTRIBUTION_ID}" in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=lax" in cookie
    assert "Max-Age=604800" in cookie
    assert response.headers["cache-control"] == "no-store, max-age=0"
    assert response.headers["referrer-policy"] == "no-referrer"


def test_outreach_referral_invalid_or_inactive_link_is_404(monkeypatch):
    client.cookies.clear()
    monkeypatch.setattr(
        sandbox_web,
        "load_us_lacey_portal_config",
        lambda: SimpleNamespace(session_cookie_secure=False),
    )

    def unavailable(_slug):
        raise UsLaceyOutreachError("unavailable")

    monkeypatch.setattr(sandbox_web, "open_outreach_link", unavailable)

    response = client.get(
        "/sandbox/ref/unknown-prospect",
        follow_redirects=False,
    )

    assert response.status_code == 404
    assert "Sandbox link is unavailable." in response.text
    assert OUTREACH_ATTRIBUTION_COOKIE not in response.headers.get(
        "set-cookie",
        "",
    )


def test_attributed_sandbox_post_binds_tenant_and_clears_referral_cookie(monkeypatch):
    client.cookies.clear()
    monkeypatch.setattr(
        sandbox_web,
        "load_us_lacey_portal_config",
        lambda: SimpleNamespace(session_cookie_secure=False),
    )
    monkeypatch.setattr(
        sandbox_web,
        "provision_us_lacey_sandbox",
        lambda **_kwargs: _sandbox_session(),
    )
    calls = []

    def bind(**kwargs):
        calls.append(kwargs)
        return True

    monkeypatch.setattr(sandbox_web, "bind_outreach_to_sandbox", bind)
    client.cookies.set(OUTREACH_ATTRIBUTION_COOKIE, str(ATTRIBUTION_ID))
    try:
        response = client.post(
            "/sandbox/start",
            data={"consent": "accepted"},
            follow_redirects=False,
        )
    finally:
        client.cookies.clear()

    assert response.status_code == 303
    assert response.headers["location"] == "/operations/new"
    assert calls == [
        {
            "attribution_session_id": str(ATTRIBUTION_ID),
            "session_token": "sandbox-attribution-token",
            "organization_id": 701,
        }
    ]
    cookies = response.headers.get_list("set-cookie")
    assert any("us_lacey_session=sandbox-attribution-token" in item for item in cookies)
    assert any(
        item.startswith(f"{OUTREACH_ATTRIBUTION_COOKIE}=")
        and ("Max-Age=0" in item or "expires=" in item.lower())
        for item in cookies
    )


def test_attribution_failure_never_blocks_sandbox_provision(monkeypatch):
    client.cookies.clear()
    monkeypatch.setattr(
        sandbox_web,
        "load_us_lacey_portal_config",
        lambda: SimpleNamespace(session_cookie_secure=False),
    )
    monkeypatch.setattr(
        sandbox_web,
        "provision_us_lacey_sandbox",
        lambda **_kwargs: _sandbox_session(),
    )

    def fail_bind(**_kwargs):
        raise UsLaceyOutreachError("growth plane unavailable")

    monkeypatch.setattr(sandbox_web, "bind_outreach_to_sandbox", fail_bind)
    client.cookies.set(OUTREACH_ATTRIBUTION_COOKIE, str(ATTRIBUTION_ID))
    try:
        response = client.post(
            "/sandbox/start",
            data={"consent": "accepted"},
            follow_redirects=False,
        )
    finally:
        client.cookies.clear()

    assert response.status_code == 303
    assert response.headers["location"] == "/operations/new"
    assert "us_lacey_session=sandbox-attribution-token" in response.headers["set-cookie"]


def test_safe_outreach_event_is_fail_open(monkeypatch):
    def fail(**_kwargs):
        raise UsLaceyOutreachError("telemetry unavailable")

    monkeypatch.setattr(growth, "record_outreach_event", fail)

    assert growth.safe_record_outreach_event(
        session_token="opaque",
        organization_id=1,
        event_name="REVIEW_REACHED",
        event_key="operation-1",
    ) is False
