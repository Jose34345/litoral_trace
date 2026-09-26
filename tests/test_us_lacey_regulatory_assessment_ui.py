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
        ruleset_version="us-lacey-regulatory-rules-v3",
        input_fingerprint="a" * 64,
        assessment_count=2,
        indeterminate_count=1,
        payload={
            "schema_version": "regulatory-assessment-snapshot-v3",
            "summary": {"subject_count": 1, "assessment_count": 2, "indeterminate_count": 1},
            "assessments": [
                {
                    "rule_id": "HTS_APPLICABILITY",
                    "subject_ref": "LT-LINE-1",
                    "status": "PASS",
                    "reason_codes": ["HTS_ON_APHIS_SCHEDULE"],
                    "explanation": "HTS is included in the APHIS Lacey declaration implementation schedule. Final applicability also depends on entry type, plant content and applicable exceptions.",
                    "calculation_trace": {"matched_prefix": "4407"},
                    "evidence_refs": [],
                    "review_required": False,
                },
                {
                    "rule_id": "DE_MINIMIS",
                    "subject_ref": "LT-LINE-1",
                    "status": "INDETERMINATE",
                    "reason_codes": ["MISSING_REQUIRED_INPUTS"],
                    "explanation": "Required de minimis inputs are missing or invalid; review is required.",
                    "calculation_trace": {},
                    "evidence_refs": [],
                    "review_required": True,
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
    assert "Regulatory Analysis" in html
    assert "De Minimis Exemption Assessment" in html
    assert "Needs information" in html
    assert "HTS Schedule Coverage" in html
    assert "Check passed" in html
    assert "<strong>2</strong> compliance checks" in html
    assert "does not represent an overall legal compliance determination" in html.lower()
    assert "shipment pass" not in html.lower()
    assert "Reason:" not in html
    assert "MISSING_REQUIRED_INPUTS" not in html
    assert "U.S. Lacey ruleset" not in html
    assert "border-l-amber-500" in html
    assert "ring-amber-600/20" in html
    assert "provide the missing quantities or values in the Action Required tab" in html


def test_direct_workspace_renders_same_noncanonical_regulatory_panel(monkeypatch):
    monkeypatch.setattr(operational_views, "_semantic_evidence_for_detail", lambda *_: {})
    monkeypatch.setattr(operational_views, "_product_intelligence_for_detail", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        operational_views,
        "_regulatory_assessment_for_detail",
        lambda *_args, **_kwargs: _view(),
        raising=False,
    )

    html = operational_views.render_operation_workspace(
        request=_request(query_string=b""),
        identity=SimpleNamespace(organization_id=7),
        detail=_detail(),
        engine2_dossier=_engine2(),
        complete_csrf="complete",
        review_csrf={},
    )

    assert 'data-regulatory-assessment-status="CURRENT"' in html
    assert "De Minimis Exemption Assessment" in html
    assert "HTS Schedule Coverage" in html
    assert "<strong>2</strong> compliance checks" in html
    assert "does not represent an overall legal compliance determination" in html.lower()
    assert "shipment pass" not in html.lower()
