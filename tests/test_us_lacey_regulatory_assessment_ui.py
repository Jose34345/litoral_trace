from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from starlette.requests import Request
from starlette.routing import Mount, Router

from litoral_trace.us_lacey.regulatory_assessment_snapshot import (
    RegulatoryAssessmentView,
    blocking_regulatory_assessments,
)
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
        ruleset_version="us-lacey-regulatory-rules-v4",
        input_fingerprint="a" * 64,
        assessment_count=2,
        indeterminate_count=0,
        payload={
            "schema_version": "regulatory-assessment-snapshot-v3",
            "summary": {"subject_count": 1, "assessment_count": 2, "indeterminate_count": 0},
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
                    "status": "NOT_EVALUATED",
                    "reason_codes": ["EXEMPTION_NOT_CLAIMED"],
                    "explanation": (
                        "De Minimis exemption was not claimed. "
                        "Does not block the declaration package."
                    ),
                    "calculation_trace": {},
                    "evidence_refs": [],
                    "review_required": False,
                    "is_blocking": False,
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
    assert 'data-regulatory-matrix' in html
    assert "Regulatory Analysis" in html
    assert "De Minimis Exemption Assessment" in html
    assert "Not evaluated" in html
    assert "HTS Schedule Coverage" in html
    assert "Check passed" in html
    assert ">Line<" in html
    assert ">Check<" in html
    assert ">Status<" in html
    assert ">Reason<" in html
    assert ">Action<" in html
    assert "2 checks" in html
    assert "need information" not in html
    assert "De Minimis exemption was not claimed. Does not block the declaration package." in html
    assert "Provide quantity / mass evidence" not in html
    assert "ACTION REQUIRED: Missing quantity or mass information." not in html
    assert 'data-action-required-count>0<' in html
    assert 'data-action-tab-count>0<' in html
    assert 'id="workflow-stepper"' in html
    assert 'id="shipment-readiness-card"' in html
    assert 'id="species-status"' in html
    assert 'id="country-status"' in html
    assert html.count('hx-swap-oob="true"') >= 4
    assert "does not represent an overall legal compliance determination" in html.lower()
    assert "shipment pass" not in html.lower()
    assert "EXEMPTION_NOT_CLAIMED" not in html
    assert "U.S. Lacey ruleset" not in html


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
    assert 'data-regulatory-matrix' in html
    assert "De Minimis Exemption Assessment" in html
    assert "HTS Schedule Coverage" in html
    assert "2 checks" in html
    assert 'data-action-required-count>0<' in html
    assert "De Minimis exemption was not claimed. Does not block the declaration package." in html
    assert "does not represent an overall legal compliance determination" in html.lower()
    assert "shipment pass" not in html.lower()


def test_blocking_helper_uses_review_required_not_raw_rule_status():
    payload = {
        "assessments": [
            {"rule_id": "BLOCK", "status": "INDETERMINATE", "review_required": True},
            {"rule_id": "OPTIONAL", "status": "INDETERMINATE", "review_required": False},
            {"rule_id": "FAIL_NONBLOCKING", "status": "FAIL", "review_required": False},
            {
                "rule_id": "EXPLICIT_NONBLOCKING",
                "status": "INDETERMINATE",
                "review_required": True,
                "is_blocking": False,
            },
        ]
    }

    blockers = blocking_regulatory_assessments(payload)

    assert [item["rule_id"] for item in blockers] == ["BLOCK"]


def test_optional_indeterminate_is_not_evaluated_and_does_not_block():
    view = _view()
    payload = dict(view.payload)
    payload["assessments"] = [
        {
            "rule_id": "SPECIAL_COMPOSITE",
            "subject_ref": "LT-LINE-1",
            "status": "INDETERMINATE",
            "reason_codes": ["MISSING_REQUIRED_INPUTS"],
            "explanation": "Optional enrichment is unavailable.",
            "calculation_trace": {},
            "evidence_refs": [],
            "review_required": False,
        }
    ]
    optional_view = RegulatoryAssessmentView(
        status=view.status,
        generation=view.generation,
        source_set_fingerprint=view.source_set_fingerprint,
        ruleset_version=view.ruleset_version,
        input_fingerprint=view.input_fingerprint,
        assessment_count=1,
        indeterminate_count=1,
        payload=payload,
    )

    presented = operational_views._present_regulatory_assessment(optional_view)
    [assessment] = presented.payload["assessments"]

    assert assessment["display_status"] == "Not evaluated"
    assert assessment["blocks_package"] is False
    assert "Does not block the current preparation package." in assessment["customer_message"]


def test_readiness_is_not_ready_when_only_regulatory_blockers_remain():
    detail = SimpleNamespace(
        status="COMPLETED",
        fields=(),
    )
    processing = SimpleNamespace(terminal=True, failed=False)

    blocked = operational_views._readiness_summary(
        detail,
        attention_fields=(),
        processing=processing,
        regulatory_action_items=({"action_id": "de-minimis-line-1"},),
    )
    ready = operational_views._readiness_summary(
        detail,
        attention_fields=(),
        processing=processing,
        regulatory_action_items=(),
    )

    assert blocked["overall"] == "NOT READY"
    assert blocked["package_ready"] is False
    assert blocked["exception_count"] == 1
    assert blocked["regulatory_exception_count"] == 1
    assert ready["overall"] == "PACKAGE READY"
    assert ready["package_ready"] is True


def test_workspace_summary_uses_combined_regulatory_action_count(monkeypatch):
    view = _view()
    payload = dict(view.payload)
    payload["assessments"] = [
        {
            "rule_id": "HTS_APPLICABILITY",
            "subject_ref": "LT-LINE-1",
            "status": "INDETERMINATE",
            "reason_codes": ["HTS10_MISSING"],
            "explanation": "A valid 10-digit HTS code is required.",
            "calculation_trace": {},
            "evidence_refs": [],
            "review_required": True,
            "is_blocking": True,
        },
        view.payload["assessments"][1],
    ]
    blocking_view = RegulatoryAssessmentView(
        status=view.status,
        generation=view.generation,
        source_set_fingerprint=view.source_set_fingerprint,
        ruleset_version=view.ruleset_version,
        input_fingerprint=view.input_fingerprint,
        assessment_count=2,
        indeterminate_count=1,
        payload=payload,
    )
    monkeypatch.setattr(operational_views, "_semantic_evidence_for_detail", lambda *_: {})
    monkeypatch.setattr(
        operational_views,
        "_product_intelligence_for_detail",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        operational_views,
        "_regulatory_assessment_for_detail",
        lambda *_args, **_kwargs: blocking_view,
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

    assert "data-review-required-provenance-count>1 need attention<" in html
    assert "data-action-required-count>1<" in html
    assert "Provide HTS" in html
    assert "Provide quantity / mass evidence" not in html


def test_regulatory_detail_lazily_rebuilds_when_active_ruleset_snapshot_is_missing(monkeypatch):
    refreshed = _view()
    monkeypatch.setattr(
        operational_views,
        "get_current_regulatory_assessment_view",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        operational_views,
        "refresh_current_regulatory_assessment_view",
        lambda **_kwargs: refreshed,
    )

    result = operational_views._regulatory_assessment_for_detail(
        SimpleNamespace(organization_id=7),
        _detail(),
    )

    assert result is refreshed


def test_regulatory_detail_revalidates_terminal_snapshot_against_current_ppq_fields(monkeypatch):
    # A CURRENT snapshot for a source-set revision may predate the canonical
    # plant fields on the same revision; "CURRENT" alone is not fresh enough.
    refreshed = _view()
    monkeypatch.setattr(
        operational_views,
        "get_current_regulatory_assessment_view",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("do not reuse a potentially stale terminal snapshot")
        ),
    )
    monkeypatch.setattr(
        operational_views,
        "refresh_current_regulatory_assessment_view",
        lambda **_kwargs: refreshed,
    )

    result = operational_views._regulatory_assessment_for_detail(
        SimpleNamespace(organization_id=7),
        _detail(),
    )

    assert result is refreshed

def test_completed_operation_reuses_snapshot_without_reopening_review(monkeypatch):
    current = _view()
    monkeypatch.setattr(
        operational_views, "get_current_regulatory_assessment_view",
        lambda **_kwargs: current,
    )
    monkeypatch.setattr(
        operational_views, "refresh_current_regulatory_assessment_view",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("viewing COMPLETED must never demote review status")
        ),
    )
    detail = _detail()
    detail.status = "COMPLETED"
    assert operational_views._regulatory_assessment_for_detail(
        SimpleNamespace(organization_id=7), detail
    ) is current
