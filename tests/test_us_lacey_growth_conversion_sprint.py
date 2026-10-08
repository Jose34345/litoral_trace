"""Commercial demo routing and qualified engagement regression tests.

Outreach URLs are public, first-party, privacy-bounded and never confer access
to customer data. Scanner GET requests must not count as product engagement.
"""
from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

import litoral_trace.web.us_lacey_sandbox as sandbox
from litoral_trace.us_lacey.growth_attribution import (
    OUTREACH_ATTRIBUTION_COOKIE,
    OutreachAttributionSession,
)
from litoral_trace.web.us_lacey_unified_app import app

from pathlib import Path


ATTRIBUTION_ID = UUID("3d434cf8-3d1f-4aec-b159-880032d9d66d")
TEMPLATES = Path(__file__).resolve().parents[1] / "src/litoral_trace/templates/us_lacey"
STATIC_JS = Path(__file__).resolve().parents[1] / "src/litoral_trace/static/js"


def _mock_referral(monkeypatch):
    monkeypatch.setattr(
        sandbox,
        "load_us_lacey_portal_config",
        lambda: SimpleNamespace(session_cookie_secure=False),
    )
    monkeypatch.setattr(
        sandbox,
        "open_outreach_link",
        lambda _slug: OutreachAttributionSession(
            attribution_session_id=ATTRIBUTION_ID,
            prospect_label="Fixture Importer",
            campaign_code="growth-conversion-test",
            source="direct_outreach",
        ),
    )


@pytest.mark.parametrize(
    ("demo", "destination"),
    [
        ("importer", "/try/importer"),
        ("broker", "/try/broker"),
        ("IMPORTER", "/try/importer"),
        ("not-a-persona", "/sandbox/start"),
        ("https://another.example", "/sandbox/start"),
    ],
)
def test_persona_referral_keeps_cookie_and_uses_only_safe_routes(
    monkeypatch, demo, destination
):
    _mock_referral(monkeypatch)
    with TestClient(app) as client:
        response = client.get(
            f"/sandbox/ref/known-prospect?demo={demo}",
            follow_redirects=False,
        )
    assert response.status_code == 303
    assert response.headers["location"] == destination
    cookie = response.headers["set-cookie"]
    assert "HttpOnly" in cookie and "SameSite=lax" in cookie
    assert str(ATTRIBUTION_ID) in cookie
    assert response.headers["referrer-policy"] == "no-referrer"


def test_old_referral_route_does_not_change(monkeypatch):
    _mock_referral(monkeypatch)
    with TestClient(app) as client:
        response = client.get(
            "/sandbox/ref/known-prospect", follow_redirects=False
        )
    assert response.status_code == 303
    assert response.headers["location"] == "/sandbox/start"


@pytest.mark.parametrize("persona,step", [("importer", 1), ("broker", 2)])
def test_public_sample_get_does_not_record_scanner_as_engaged(
    monkeypatch, persona, step
):
    recorded = []
    monkeypatch.setattr(
        sandbox,
        "safe_record_pre_sandbox_outreach_event",
        lambda **kwargs: recorded.append(kwargs),
    )
    with TestClient(app) as client:
        client.cookies.set(OUTREACH_ATTRIBUTION_COOKIE, str(ATTRIBUTION_ID))
        response = client.get(f"/try/{persona}?step={step}")
    assert response.status_code == 200
    assert recorded == []
    assert f'data-outreach-persona="{persona}"' in response.text
    assert f'data-outreach-step="{step}"' in response.text
    assert "us_lacey_outreach_human_visit.js" in response.text
    assert "Request a 15-minute workflow review" in response.text
    assert "mailto:comercial@litoraltrace.com" in response.text


def test_browser_visible_sample_post_records_likely_human_and_sample_events(
    monkeypatch,
):
    calls = []
    monkeypatch.setattr(
        sandbox,
        "safe_record_pre_sandbox_outreach_event",
        lambda **kwargs: calls.append(kwargs),
    )
    with TestClient(app) as client:
        client.cookies.set(OUTREACH_ATTRIBUTION_COOKIE, str(ATTRIBUTION_ID))
        first = client.post(
            "/sandbox/engagement/human-visit",
            data={"sample_persona": "importer", "sample_step": "1"},
        )
        second = client.post(
            "/sandbox/engagement/human-visit",
            data={"sample_persona": "importer", "sample_step": "2"},
        )
    assert (first.status_code, second.status_code) == (204, 204)
    assert [call["event_name"] for call in calls] == [
        "HUMAN_VISIT", "SAMPLE_STARTED",
        "HUMAN_VISIT", "SAMPLE_REUSE_REACHED", "SAMPLE_COMPLETED",
    ]
    assert all(call["attribution_session_id"] == str(ATTRIBUTION_ID) for call in calls)
    assert all("document" not in str(call).lower() for call in calls)


@pytest.mark.parametrize(
    "data", [
        {"sample_persona": "external-url", "sample_step": "1"},
        {"sample_persona": "broker", "sample_step": "42"},
        {"sample_persona": "broker", "sample_step": "0"},
    ]
)
def test_invalid_sample_signal_cannot_create_product_engagement(monkeypatch, data):
    calls = []
    monkeypatch.setattr(
        sandbox,
        "safe_record_pre_sandbox_outreach_event",
        lambda **kwargs: calls.append(kwargs),
    )
    with TestClient(app) as client:
        client.cookies.set(OUTREACH_ATTRIBUTION_COOKIE, str(ATTRIBUTION_ID))
        response = client.post("/sandbox/engagement/human-visit", data=data)
    assert response.status_code == 204
    assert [call["event_name"] for call in calls] == ["HUMAN_VISIT"]


def test_unattributed_signal_does_not_create_any_commercial_event(monkeypatch):
    calls = []
    monkeypatch.setattr(
        sandbox,
        "safe_record_pre_sandbox_outreach_event",
        lambda **kwargs: calls.append(kwargs),
    )
    with TestClient(app) as client:
        response = client.post(
            "/sandbox/engagement/human-visit",
            data={"sample_persona": "importer", "sample_step": "1"},
        )
    assert response.status_code == 204
    assert calls == []


def test_conversion_copy_admin_links_and_browser_signal_contract():
    landing = (TEMPLATES / "sandbox_start.html").read_text(encoding="utf-8")
    sample = (TEMPLATES / "evaluation_sample.html").read_text(encoding="utf-8")
    admin = (TEMPLATES / "admin.html").read_text(encoding="utf-8")
    script = (STATIC_JS / "us_lacey_outreach_human_visit.js").read_text(encoding="utf-8")

    assert "Request a 15-minute guided review" in landing
    assert "Request a 15-minute workflow review" in sample
    assert "No documents, account or payment required." in sample
    assert "?demo=importer" in admin
    assert "?demo=broker" in admin
    assert "navigator.webdriver !== true" in script
    assert 'document.visibilityState === "visible"' in script
    assert "new URLSearchParams()" in script
    assert 'payload.set("sample_persona", samplePersona)' in script
    assert 'payload.set("sample_step", sampleStep)' in script
    assert 'window.setTimeout(send, 4000)' in script
    assert "userAgent" not in script
