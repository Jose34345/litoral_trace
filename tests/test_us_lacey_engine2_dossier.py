"""Pure rendering contracts for the read-only Engine 2 dossier preview."""
from __future__ import annotations

import re
from types import SimpleNamespace
from uuid import uuid4

from starlette.requests import Request
from litoral_trace.us_lacey.lacey_engine_dossier import Engine2DossierAvailability, Engine2DossierEvidenceView, Engine2DossierFieldView, Engine2DossierIssueView, Engine2DossierView
from litoral_trace.web.us_lacey_pilot_app import app
from litoral_trace.web.us_lacey_operational_views import render_operation_detail


def _html(dossier):
    detail = SimpleNamespace(public_id=uuid4(), client_reference="DOSSIER-UI", status="NEW", document_count=1, merchandise_line_count=1, importer_name="Authoritative BRAZIL", supplier_name=None, documents=(), fields=(), plant_declarations=(), conflicts=())
    identity = SimpleNamespace(legal_name="Portal customer")
    request = Request({"type": "http", "method": "GET", "path": "/operations/test", "headers": [], "scheme": "http", "server": ("testserver", 80), "app": app})
    return render_operation_detail(request=request, identity=identity, detail=detail, engine2_dossier=dossier, upload_csrf="upload", complete_csrf="complete", review_csrf={})


def test_current_dossier_renders_all_states_provenance_issues_and_harvest_separately():
    evidence = Engine2DossierEvidenceView("bill.pdf", 7, "Species: radiata", "radiata", "RADIATA", "EXPLICIT", .91, .8, "PLANT_COMPONENT", "1", "a", (1, 2, 3, 4))
    derived = Engine2DossierEvidenceView("declaration.pdf", 2, "Pinus radiata", "Pinus", "PINUS", "DERIVED", .7, .6, "PLANT_COMPONENT", "1", "a", None)
    fields = (
        Engine2DossierFieldView("country_of_origin", "Country of Origin", "SUPPORTED", ("New Zealand",), (evidence,)),
        Engine2DossierFieldView("country_of_harvest", "Country of Harvest", "MISSING", (), ()),
        Engine2DossierFieldView("container_number", "Container Number", "SUPPORTED_MULTIPLE", ("MSKU1", "MSKU2"), (evidence,)),
        Engine2DossierFieldView("consignee_name", "Consignee Name", "NEAR_MATCH", ("WOOD BROKERAGE", "WOOD BROKERAGE INTL"), (evidence,)),
        Engine2DossierFieldView("species", "Species", "CONFLICT", ("radiata", "taeda"), (evidence, derived)),
        Engine2DossierFieldView("genus", "Genus", "REVIEW_REQUIRED", ("Pinus", "Picea"), (derived, evidence)),
    )
    issue = Engine2DossierIssueView("genus", "Genus", "MEDIUM", "CONFLICT", "Human review required.", True, ("bill.pdf",), "1", "a")
    html = _html(Engine2DossierView(Engine2DossierAvailability.CURRENT, "REVIEW_REQUIRED", "engine", "rules", "schema", document_count=2, fields=fields, issues=(issue,)))
    for state in ("MISSING", "SUPPORTED", "SUPPORTED_MULTIPLE", "NEAR_MATCH", "CONFLICT", "REVIEW_REQUIRED"):
        assert f'data-engine2-state="{state}"' in html
    assert 'data-engine2-readiness="REVIEW_REQUIRED"' in html and "Preparation readiness" in html
    assert "MSKU1, MSKU2" in html and "WOOD BROKERAGE INTL" in html and 'data-engine2-issue' in html
    assert 'data-engine2-evidence-class="EXPLICIT"' in html and 'data-engine2-evidence-class="DERIVED"' in html and 'data-engine2-source-page="7"' in html
    assert "Raw: radiata" in html and "Normalized: RADIATA" in html and "bbox 1, 2, 3, 4" in html
    harvest = re.search(r'<article[^>]*data-engine2-field="country_of_harvest".*?</article>', html, re.S).group(0)
    assert "Missing" in harvest and "New Zealand" not in harvest and "Evidence" not in harvest
    assert "not a legal compliance determination" in html and "PPQ and human review below remain authoritative" in html and "ACE or LAWGS" in html
    assert "accepted" not in html.lower().split("data-engine2-dossier", 1)[1].split('aria-labelledby="shipment-information-heading"', 1)[0]


def test_non_current_dossier_states_hide_canonical_values():
    for availability in (Engine2DossierAvailability.DISABLED, Engine2DossierAvailability.NOT_AVAILABLE, Engine2DossierAvailability.STALE, Engine2DossierAvailability.FAILED, Engine2DossierAvailability.INVALID):
        html = _html(Engine2DossierView(availability, fields=(Engine2DossierFieldView("species", "Species", "SUPPORTED", ("radiata",), ()),), safe_status_message="Safe state."))
        assert f'data-engine2-availability="{availability}"' in html and "Safe state." in html and "radiata" not in html
