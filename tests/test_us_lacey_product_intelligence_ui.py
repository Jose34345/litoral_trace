from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from starlette.requests import Request
from starlette.routing import Mount, Router

from litoral_trace.us_lacey.product_intelligence_snapshot import ProductIntelligenceView
from litoral_trace.web import us_lacey_operational_views as operational_views
from litoral_trace.web.us_lacey_operational_views import (
    render_operation_detail,
    render_operation_workspace,
    render_processing_fragment,
)


def _request(*, query_string: bytes = b"", headers: list[tuple[bytes, bytes]] | None = None) -> Request:
    router = Router(routes=[Mount("/static", app=Router(), name="static")])
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/operations/test",
            "query_string": query_string,
            "headers": headers or [],
            "scheme": "http",
            "server": ("testserver", 80),
            "router": router,
        }
    )


def _detail():
    return SimpleNamespace(
        public_id=uuid4(),
        client_reference="PO-PI-001",
        status="READY_FOR_REVIEW",
        document_count=1,
        documents=(
            SimpleNamespace(
                filename="BOM.xlsx",
                document_role="UNKNOWN",
                job_status="COMPLETED",
                processing_status="EXTRACTED",
            ),
        ),
        fields=(),
        conflicts=(),
    )


def _engine2():
    return SimpleNamespace(
        availability="INVALID",
        safe_status_message="Evidence preview unavailable.",
        readiness="REVIEW_REQUIRED",
        fields=(),
        issues=(),
    )


def _product_intelligence() -> ProductIntelligenceView:
    return ProductIntelligenceView(
        status="READY",
        generation=2,
        source_set_fingerprint="f" * 64,
        document_count=1,
        eligible_document_count=1,
        recognized_bom_table_count=1,
        unique_sku_count=1,
        component_count=2,
        material_count=2,
        issue_count=0,
        payload={
            "schema_version": "product-intelligence-snapshot-v1",
            "sources": [
                {
                    "filename": "BOM.xlsx",
                    "tables": [
                        {
                            "name": "BOM",
                            "source": {"sheet": "BOM", "row": 1, "locator": "sheet:BOM;header_row:1"},
                            "issues": [],
                            "compositions": [
                                {
                                    "sku": "CHAIR-001",
                                    "product_name": "Chair",
                                    "components": [
                                        {
                                            "component_key": "CHAIR-001:row:2",
                                            "description_raw": "Front leg",
                                            "quantity": "2",
                                            "source": {"row": 2, "sheet": "BOM", "locator": "sheet:BOM;header_row:1"},
                                            "material": {
                                                "name_raw": "Rubberwood",
                                                "name_normalized": "rubberwood",
                                                "mass": {"raw_value": "0.5", "raw_unit": "kg", "kilograms": "0.5"},
                                                "source": {"row": 2, "sheet": "BOM", "locator": "sheet:BOM;header_row:1"},
                                            },
                                        },
                                        {
                                            "component_key": "CHAIR-001:row:3",
                                            "description_raw": "Seat",
                                            "quantity": "1",
                                            "source": {"row": 3, "sheet": "BOM", "locator": "sheet:BOM;header_row:1"},
                                            "material": {
                                                "name_raw": "Plywood",
                                                "name_normalized": "plywood",
                                                "mass": {"raw_value": "1.2", "raw_unit": "kg", "kilograms": "1.2"},
                                                "source": {"row": 3, "sheet": "BOM", "locator": "sheet:BOM;header_row:1"},
                                            },
                                        },
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ],
            "issues": [],
        },
    )


def test_workspace_renders_product_composition_with_source_provenance(monkeypatch):
    monkeypatch.setattr(operational_views, "_semantic_evidence_for_detail", lambda *_: {})
    monkeypatch.setattr(
        operational_views,
        "_product_intelligence_for_detail",
        lambda *_args, **_kwargs: _product_intelligence(),
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
        review_csrf={},
    )

    assert 'data-product-intelligence-status="READY"' in html
    assert "Product composition evidence" in html
    assert "CHAIR-001" in html
    assert "Front leg" in html
    assert "Rubberwood" in html
    assert "Plywood" in html
    assert "BOM.xlsx" in html
    assert "row 2" in html

def test_workspace_renders_non_applicable_product_intelligence_compactly(monkeypatch):
    monkeypatch.setattr(operational_views, "_semantic_evidence_for_detail", lambda *_: {})
    view = ProductIntelligenceView(
        status="NOT_APPLICABLE",
        generation=1,
        source_set_fingerprint="e" * 64,
        document_count=1,
        eligible_document_count=1,
        recognized_bom_table_count=0,
        unique_sku_count=0,
        component_count=0,
        material_count=0,
        issue_count=0,
        payload={"schema_version": "product-intelligence-snapshot-v1", "sources": [], "issues": []},
    )
    monkeypatch.setattr(
        operational_views,
        "_product_intelligence_for_detail",
        lambda *_args, **_kwargs: view,
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
        review_csrf={},
    )

    assert 'data-product-intelligence-status="NOT_APPLICABLE"' in html
    assert "No explicit BOM was detected" in html

def test_processing_poll_requests_product_intelligence_with_terminal_workspace():
    html = render_processing_fragment(
        request=_request(headers=[(b"hx-request", b"true")]),
        detail=_detail(),
    )

    assert "workspace-fragment?include_product_intelligence=1" in html


def test_operation_workspace_hydration_renders_product_composition_without_manual_reload(monkeypatch):
    monkeypatch.setattr(operational_views, "_semantic_evidence_for_detail", lambda *_: {})
    monkeypatch.setattr(
        operational_views,
        "_product_intelligence_for_detail",
        lambda *_args, **_kwargs: _product_intelligence(),
    )

    html = render_operation_workspace(
        request=_request(query_string=b"include_product_intelligence=1"),
        identity=SimpleNamespace(organization_id=7),
        detail=_detail(),
        engine2_dossier=_engine2(),
        complete_csrf="complete",
        review_csrf={},
    )

    assert 'data-product-intelligence-status="READY"' in html
    assert "Product composition" in html
    assert "CHAIR-001" in html
    assert "Rubberwood" in html
    assert "BOM.xlsx" in html
    assert "row 2" in html


def test_direct_terminal_workspace_renders_product_intelligence_once(monkeypatch):
    monkeypatch.setattr(operational_views, "_semantic_evidence_for_detail", lambda *_: {})
    monkeypatch.setattr(
        operational_views,
        "_product_intelligence_for_detail",
        lambda *_args, **_kwargs: _product_intelligence(),
    )

    html = render_operation_workspace(
        request=_request(),
        identity=SimpleNamespace(organization_id=7),
        detail=_detail(),
        engine2_dossier=_engine2(),
        complete_csrf="complete",
        review_csrf={},
    )

    assert html.count('data-product-intelligence-status="READY"') == 1
