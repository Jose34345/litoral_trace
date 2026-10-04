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


def test_lacey_landing_leads_with_compliance_infrastructure_positioning():
    response = client.get("/lacey")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store, max-age=0"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"

    html = response.text
    assert '<html lang="en-US"' in html
    assert "U.S. Lacey Act Compliance Infrastructure" in html
    assert "Turn supplier evidence, BOM data and shipment documents into review-ready Lacey data." in html
    assert "Built for import compliance teams" in html
    assert "Run a sample shipment" in html
    assert "Analyze my documents" in html
    assert "Synthetic data. No upload required." in html
    assert "7" in html
    assert "31" in html
    assert "24" in html
    assert "4" in html
    assert "3" in html
    assert "Professional Plan — USD 149/month" not in html
    assert "USD 149" in html
    assert "/month" in html
    assert 'id="pricing"' in html
    assert "USD 99/month" not in html
    assert "USD 199" not in html
    assert "25 operations" not in html
    assert "Phase VII" not in html
    assert 'href="/demo"' in html
    assert 'href="/sandbox/start"' in html
    assert 'href="/login"' in html
    assert "comercial@litoraltrace.com" in html


def test_lacey_landing_surfaces_full_compliance_workflow():
    html = client.get("/lacey").text

    assert "Evidence to review-ready data" in html
    assert "One controlled workflow for recurring Lacey preparation." in html
    for step in (
        "Upload",
        "Reconcile",
        "Resolve",
        "Review",
        "Export",
    ):
        assert step in html

    assert "Know what is missing before filing." in html
    for evidence_item in (
        "Commercial invoice",
        "Packing list",
        "Bill of lading",
        "Species declaration",
        "Harvest affidavit",
        "Country-of-harvest evidence",
        "Supplier questionnaire",
    ):
        assert evidence_item in html

    assert "Taxonomy and conflicts" in html
    assert "White Oak" in html
    assert "Quercus" in html
    assert "alba" in html
    assert "supplier_species.xlsx · row 18" in html

    for flow_stage in (
        "Shipment and supplier evidence",
        "Cross-document values",
        "Taxonomy and structured fields",
        "Exceptions and missing evidence",
        "Review-ready declaration package",
    ):
        assert flow_stage in html


def test_lacey_landing_routes_evaluation_to_sample_before_document_upload():
    html = client.get("/lacey").text

    sample_index = html.index("Run a sample shipment")
    upload_index = html.index("Analyze my documents")
    assert sample_index < upload_index
    assert html.count('href="/demo"') >= 2
    assert html.count('href="/sandbox/start"') >= 2
    assert "See Litoral Trace analyze a sample shipment." in html
    assert "Synthetic data. No upload required." in html
    assert 'id="lacey-beta-form"' not in html
    assert "Early Access" not in html
    assert "Private Beta" not in html
    for field_name in ("work_email", "role", "volume", "workflow", "willingness"):
        assert f'name="{field_name}"' not in html


def test_lacey_demo_is_synthetic_and_surfaces_missing_conflicting_data():
    response = client.get("/lacey/demo")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store, max-age=0"
    html = response.text
    assert "Illustrative sample" in html
    assert "No documents are required for this walkthrough." in html
    assert "Analyze sample shipment" in html
    assert "Analyze my documents" in html
    assert "Evaluate with your workflow" in html
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
    assert 'aria-label="Run a synthetic U.S. Lacey sample shipment"' in landing
    assert 'aria-label="Analyze my documents in the four-hour sandbox"' in landing
    assert 'aria-label="Illustrative Lacey readiness pipeline"' in landing
    assert 'aria-label="Illustrative supplier evidence checklist"' in landing
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
