from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from starlette.requests import Request
from starlette.routing import Mount, Router

from litoral_trace.web import us_lacey_operational_views as operational_views
from litoral_trace.web.us_lacey_operational_views import render_operation_workspace


def _request() -> Request:
    router = Router(routes=[Mount("/static", app=Router(), name="static")])
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/operations/test",
            "query_string": b"",
            "headers": [],
            "scheme": "http",
            "server": ("testserver", 80),
            "router": router,
        }
    )


def _field(
    field_id: int,
    *,
    name: str,
    label: str,
    status: str,
    value: str | None,
    provenance: str,
):
    return SimpleNamespace(
        id=field_id,
        line_reference="CHAIR-001",
        field_name=name,
        label=label,
        ppq_number=14,
        scope="PLANT_LINE",
        proposed_value=value,
        effective_value=value,
        status=status,
        confidence=1.0 if value else 0.0,
        source_assurance_document_id=None,
        source_page=None,
        source_locator=(
            "reused_evidence:00000000-0000-0000-0000-000000000001"
            if provenance == "reused_evidence"
            else None
        ),
        extractor=(
            "reusable-supplier-evidence"
            if provenance == "reused_evidence"
            else "deterministic-extractor"
        ),
        extractor_version="1",
        reviewed_by_user_id=None,
        reviewed_at=None,
        validation_status="VALID" if value else "MISSING",
        validation_error=None,
        not_required_reason_code=None,
        candidates=(),
        provenance=provenance,
    )


def _detail():
    return SimpleNamespace(
        public_id=uuid4(),
        client_reference="REUSE-UI-001",
        status="REVIEW_REQUIRED",
        document_count=2,
        documents=(),
        conflicts=(),
        fields=(
            _field(
                1,
                name="genus",
                label="Plant Scientific Name — Genus",
                status="FOUND",
                value="Quercus",
                provenance="current_shipment",
            ),
            _field(
                2,
                name="species",
                label="Plant Scientific Name — Species",
                status="FOUND",
                value="Quercus alba",
                provenance="reused_evidence",
            ),
            _field(
                3,
                name="country_of_harvest",
                label="Country of Harvest",
                status="MISSING",
                value=None,
                provenance="review_required",
            ),
        ),
    )


def _engine2():
    return SimpleNamespace(
        availability="INVALID",
        safe_status_message="Evidence preview unavailable.",
        readiness="REVIEW_REQUIRED",
        fields=(),
        issues=(),
    )


def test_workspace_shows_reuse_summary_and_field_badge(monkeypatch):
    monkeypatch.setattr(operational_views, "_semantic_evidence_for_detail", lambda *_: {})
    monkeypatch.setattr(
        operational_views,
        "_product_intelligence_for_detail",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        operational_views,
        "_regulatory_assessment_for_detail",
        lambda *_args, **_kwargs: None,
    )

    html = render_operation_workspace(
        request=_request(),
        identity=SimpleNamespace(organization_id=7),
        detail=_detail(),
        engine2_dossier=_engine2(),
        complete_csrf="complete",
        review_csrf={3: "review-country"},
    )

    assert "1 from this shipment" in html
    assert "1 reused" in html
    assert "1 need attention" in html
    assert 'data-provenance-summary' in html
    assert 'data-reused-evidence-count' in html
    assert "Reused from verified supplier evidence" in html
    assert "Source: Verified supplier evidence from a prior shipment" in html
    assert 'data-auto-resolved-field="2"' in html
