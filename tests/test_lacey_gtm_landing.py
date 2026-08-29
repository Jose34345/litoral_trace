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


def test_lacey_landing_leads_with_document_to_data_differentiation():
    response = client.get("/lacey")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store, max-age=0"

    html = response.text
    assert '<html lang="en">' in html
    assert "Stop manually preparing Lacey spreadsheets." in html
    assert "Start before the spreadsheet" in html
    assert "Upload the shipment documents you already receive" in html
    assert "See a sample shipment" in html
    assert "Extract" in html
    assert "Compare" in html
    assert "Preserve evidence" in html
    assert "No ERP access" in html
    assert "comercial@litoraltrace.com" in html


def test_lacey_landing_has_exact_five_field_conversion_form():
    html = client.get("/lacey").text
    for field_name in ("work_email", "role", "volume", "workflow", "willingness"):
        assert f'name="{field_name}"' in html
        assert html.count(f'name="{field_name}"') == 1
    assert "No sales call is required" in html
    assert "Yes" in html and "Maybe" in html and "No" in html


def test_lacey_demo_is_synthetic_and_surfaces_missing_conflicting_data():
    response = client.get("/lacey/demo")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store, max-age=0"
    html = response.text
    assert "Illustrative sample" in html
    assert "Analyze sample shipment" in html
    assert "25" in html
    assert "Country of Harvest" in html
    assert "Manufacturer ID" in html
    assert "5,000 kg" in html
    assert "4,850 kg" in html
    assert "MISSING" in html
    assert "REVIEW" in html
    assert "Download prepared XLSX" in html
    assert "litoral-trace-lacey-demo-output.xlsx" in html
    assert "not a Lacey compliance decision" in html


def test_lacey_landing_contains_responsive_and_accessibility_contracts():
    landing = client.get("/lacey").text
    demo = client.get("/lacey/demo").text
    assert 'href="#main-content"' in landing
    assert 'aria-live="polite"' in landing
    assert 'label for="work_email"' in landing
    assert 'label for="role"' in landing
    assert 'href="#main-content"' in demo
    assert 'aria-live="polite"' in demo


def test_lacey_event_endpoint_accepts_only_aggregate_whitelisted_events():
    for event_name in (
        "lacey_visit",
        "lacey_cta_click",
        "lacey_form_start",
        "lacey_form_submit",
        "lacey_demo_open",
        "lacey_demo_run",
        "lacey_demo_download",
    ):
        response = client.post("/lacey/event", data={"event": event_name})
        assert response.status_code == 204
        assert response.headers["cache-control"] == "no-store"

    rejected = client.post(
        "/lacey/event",
        data={"event": "work_email=user@example.com", "work_email": "user@example.com"},
    )
    assert rejected.status_code == 422


def test_lacey_copy_does_not_claim_existing_filing_or_legal_outcome():
    html = (client.get("/lacey").text + client.get("/lacey/demo").text).lower()
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
