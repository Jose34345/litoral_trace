from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from starlette.requests import Request
from starlette.routing import Mount, Router

from litoral_trace.us_lacey.regulatory_assessment_snapshot import RegulatoryAssessmentView
from litoral_trace.web import us_lacey_operational_views as operational_views


def _request(*, query_string: bytes = b"include_product_intelligence=1") -> Request:
    router = Router(routes=[Mount("/static", app=Router(), name="static")])
    return Request({
        "type": "http",
        "method": "GET",
        "path": "/operations/test/workspace-fragment",
        "query_string": query_string,
        "headers": [],
        "scheme": "http",
        "server": ("testserver", 80),
        "router": router,
    })


def _detail():
    return SimpleNamespace(
        public_id=uuid4(),
        client_reference="PO-RULES-001",
        status="READY_FOR_REVIEW",
        document_count=1,
        documents=(),
        fields=(),
        conflicts=(),
    )


def _engine2():
    return SimpleNamespace(availability="INVALID", safe_status_message="Evidence preview unavailable.", readiness="REVIEW_REQUIRED", fields=(), issues=())


def _view() -> RegulatoryAssessmentView:
    return RegulatoryAssessmentView(
        status="CURRENT",
        generation=2,
        source_set_fingerprint="f" * 64,
        ruleset_version="us-lacey-regulatory-rules-v1",
        input_fingerprint="a" * 64,
        assessment_count=2,
        indeterminate_count=1,
        payload={
            "schema_version": "regulatory-assessment-snapshot-v1",
            "summary": {"assessment_count": 2, "indeterminate_count": 1},
            "assessments": [
                {
                    "rule_id": "DE_MINIMIS",
                    "subject_ref": "CHAIR-001",
                    "status": "INDETERMINATE",
                    "reason_codes": ["MISSING_REQUIRED_INPUTS"],
                    "explanation": "Required de minimis inputs are missing or invalid; review is required.",
                    "calculation_trace": {},
                    "evidence_refs": [],
                    "review_required": True,
                },
                {
                    "rule_id": "SPECIAL_COMPOSITE",
                    "subject_ref": "CHAIR-001:row:2",
                    "status": "FAIL",
                    "reason_codes": ["THIN_SOLID_PLIES_DISQUALIFY"],
                    "explanation": "Thin plies or layers of solid wood do not support SPECIAL / COMPOSITE.",
                    "calculation_trace": {"thin_solid_plies_or_layers": "YES"},
                    "evidence_refs": [{"source_type": "BOM_MATERIAL", "source_id": "doc-1", "locator": "BOM:row:2"}],
                    "review_required": False,
                },
            ],
        },
    )


def test_terminal_workspace_hydrates_rule_scoped_regulatory_panel(monkeypatch):
    monkeypatch.setattr(operational_views, "_semantic_evidence_for_detail", lambda *_: {})
    monkeypatch.setattr(operational_views, "_product_intelligence_for_detail", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        operational_views,
        "_regulatory_assessment_for_detail",
        lambda *_args, **_kwargs: _view(),
        raising=False,
    )

    html = operational_views.render_operation_workspace(
        request=_request(),
        identity=SimpleNamespace(organization_id=7),
        detail=_detail(),
        engine2_dossier=_engine2(),
        complete_csrf="complete",
        review_csrf={},
    )

    assert 'data-regulatory-assessment-status="CURRENT"' in html
    assert "Regulatory assessment" in html
    assert "DE_MINIMIS" in html
    assert "INDETERMINATE" in html
    assert "SPECIAL_COMPOSITE" in html
    assert "FAIL" in html
    assert "does not replace final compliance determination" in html.lower()
    assert "shipment pass" not in html.lower()


def test_direct_operation_detail_renders_same_noncanonical_regulatory_panel(monkeypatch):
    monkeypatch.setattr(operational_views, "_semantic_evidence_for_detail", lambda *_: {})
    monkeypatch.setattr(operational_views, "_product_intelligence_for_detail", lambda *_args, **_kwargs: None)

    html = operational_views.render_operation_detail(
        request=_request(query_string=b""),
        identity=SimpleNamespace(organization_id=7),
        detail=_detail(),
        engine2_dossier=_engine2(),
        product_intelligence=None,
        regulatory_assessment=_view(),
        upload_csrf="upload",
        complete_csrf="complete",
        review_csrf={},
    )

    assert 'data-regulatory-assessment-status="CURRENT"' in html
    assert "DE_MINIMIS" in html
    assert "SPECIAL_COMPOSITE" in html
    assert "does not replace final compliance determination" in html.lower()
    assert "shipment pass" not in html.lower()
