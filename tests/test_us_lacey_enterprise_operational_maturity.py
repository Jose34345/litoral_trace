from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
WEB = ROOT / "src" / "litoral_trace" / "web"
US_LACEY = ROOT / "src" / "litoral_trace" / "us_lacey"


def test_evidence_is_a_real_customer_surface_not_a_planned_affordance() -> None:
    base = (TEMPLATES / "us_lacey" / "base.html").read_text(encoding="utf-8")
    evidence = (TEMPLATES / "us_lacey" / "evidence.html").read_text(encoding="utf-8")
    app = (WEB / "us_lacey_pilot_app.py").read_text(encoding="utf-8")

    assert 'href="/evidence"' in base
    assert ">Settings<" not in base
    assert "Planned" not in base
    assert '@app.get("/evidence"' in app
    for contract in (
        "Supplier evidence registry",
        "Supplier → product → evidence → claims",
        "Evidence validity watch",
        "0–30 days",
        "31–60 days",
        "61–90 days",
    ):
        assert contract in evidence


def test_operations_command_center_exposes_business_semantics() -> None:
    template = (TEMPLATES / "us_lacey" / "operations.html").read_text(encoding="utf-8")
    core = (US_LACEY / "_operations_core.py").read_text(encoding="utf-8")
    views = (WEB / "us_lacey_operational_views.py").read_text(encoding="utf-8")

    for metric in ("Open", "Needs review", "Ready to export", "Completed this month"):
        assert metric in template
    for column in ("Reference", "Supplier", "Documents", "Exceptions", "Readiness", "Updated"):
        assert column in template
    assert "Supplier unresolved" in template
    assert "business_reference" in core
    assert 'f"Entry {derived[\'filing_entry_reference\']}"' in core
    assert 'f"B/L {derived[\'bill_of_lading\']}"' in core
    assert "exception_count" in core
    assert "def _relative_time" in views


def test_shipment_workspace_is_readiness_first_and_upload_is_modal() -> None:
    template = (TEMPLATES / "us_lacey" / "operation_detail.html").read_text(encoding="utf-8")
    fragments = TEMPLATES / "us_lacey" / "fragments"
    readiness = (fragments / "shipment_readiness_status.html").read_text(encoding="utf-8")
    country = (fragments / "country_status.html").read_text(encoding="utf-8")
    assert "Shipment system of record" in template
    assert "Last processed" in template
    assert "shipment_readiness_status.html" in template
    assert "Shipment readiness" in readiness
    assert "Exceptions:" in readiness
    assert "Country of harvest" in country
    assert "reused from verified supplier evidence" in template
    assert "Inspect source" in template
    assert "Add evidence" in template
    assert 'id="add-evidence-dialog"' in template
    assert "lt-upload-dropzone" not in template


def test_exception_review_is_matrix_based_and_auditable() -> None:
    template = (
        TEMPLATES / "us_lacey" / "fragments" / "operation_workspace.html"
    ).read_text(encoding="utf-8")
    assert "Exception work queue" in template
    for column in ("Field", "Proposed value", "Evidence", "Status", "Decision"):
        assert column in template
    for action in ("Accept", "Override", "Request evidence", "Mark not applicable"):
        assert action in template
    assert "Resolved by Litoral Trace" in template
    assert "Authorized reviewer" in template
    assert 'id="request-evidence-dialog"' in template
    assert "does not send supplier email on your behalf" in template
    assert "default_review_tab" in template
    assert "Preparation Package" in template


def test_marketing_uses_product_proof_security_and_lawgs_output() -> None:
    landing = (TEMPLATES / "public" / "lacey.html").read_text(encoding="utf-8")
    assert "Operations Command Center" in landing
    assert "SYSTEM OF RECORD" in landing
    assert 'id="security"' in landing
    for claim in (
        "Tenant isolation",
        "Protected transport & storage",
        "Ephemeral evaluation sandbox",
        "No customer-document training pipeline",
    ):
        assert claim in landing
    assert 'id="lawgs-output"' in landing
    assert "LAWGS XML · review-ready work product" in landing
    assert "&lt;LaceyDeclaration&gt;" in landing
    assert "does not itself submit a filing to ACE or LAWGS" in landing
