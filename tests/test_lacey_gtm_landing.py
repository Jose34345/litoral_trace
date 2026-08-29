from __future__ import annotations

from fastapi.testclient import TestClient

from litoral_trace.web.lacey_experiment_app import app


client = TestClient(app)


def test_lacey_microsite_health_and_root_redirect():
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json() == {"status": "healthy"}

    root = client.get("/", follow_redirects=False)
    assert root.status_code == 307
    assert root.headers["location"] == "/lacey"


def test_lacey_landing_renders_narrow_private_beta_offer():
    response = client.get("/lacey")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store, max-age=0"

    html = response.text
    assert '<html lang="en">' in html
    assert "Stop chasing Lacey data across supplier documents." in html
    assert "Test one completed shipment" in html
    assert "Extract" in html
    assert "Reconcile" in html
    assert "Evidence" in html
    assert "No ERP access" in html
    assert "No automated Lacey filing" in html
    assert "No live ACE or LAWGS integration" in html
    assert "No legal compliance determination" in html
    assert "No guarantee of APHIS or CBP acceptance" in html
    assert "comercial@litoraltrace.com" in html


def test_lacey_landing_has_exact_five_field_conversion_form():
    response = client.get("/lacey")
    html = response.text

    for field_name in (
        "work_email",
        "role",
        "volume",
        "workflow",
        "willingness",
    ):
        assert f'name="{field_name}"' in html

    assert html.count('name="work_email"') == 1
    assert html.count('name="role"') == 1
    assert html.count('name="volume"') == 1
    assert html.count('name="workflow"') == 1
    assert html.count('name="willingness"') == 1
    assert "Yes" in html
    assert "Maybe" in html
    assert "No" in html


def test_lacey_landing_contains_responsive_and_accessibility_contracts():
    response = client.get("/lacey")
    html = response.text

    assert "@media (max-width: 900px)" in html
    assert "@media (max-width: 620px)" in html
    assert 'href="#main-content"' in html
    assert 'aria-live="polite"' in html
    assert 'label for="work_email"' in html
    assert 'label for="role"' in html


def test_lacey_event_endpoint_accepts_only_aggregate_whitelisted_events():
    for event_name in (
        "lacey_visit",
        "lacey_cta_click",
        "lacey_form_start",
        "lacey_form_submit",
    ):
        response = client.post("/lacey/event", data={"event": event_name})
        assert response.status_code == 204
        assert response.headers["cache-control"] == "no-store"

    rejected = client.post(
        "/lacey/event",
        data={
            "event": "work_email=user@example.com",
            "work_email": "user@example.com",
        },
    )
    assert rejected.status_code == 422


def test_lacey_copy_does_not_claim_existing_filing_or_legal_outcome():
    html = client.get("/lacey").text.lower()

    prohibited_positive_claims = (
        "we file your lacey declaration",
        "automated lacey filing is live",
        "ace integration is live",
        "lawgs integration is live",
        "guaranteed compliant",
        "guaranteed acceptance",
    )
    for claim in prohibited_positive_claims:
        assert claim not in html

    assert "does not file declarations" in html
    assert "provide legal advice" in html
