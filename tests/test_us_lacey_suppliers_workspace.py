from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID

from starlette.requests import Request
from starlette.routing import Mount, Router

from litoral_trace.us_lacey.evidence_catalog import (
    EvidenceCatalogView,
    EvidenceClaimView,
    EvidenceExpiryView,
    EvidenceRecordView,
    SupplierEvidenceView,
    SupplierProductEvidenceView,
)
from litoral_trace.us_lacey.supplier_intelligence import (
    UsLaceySupplierIntelligenceService,
)
from litoral_trace.web.us_lacey_supplier_views import (
    render_supplier_detail,
    render_suppliers_list,
)


NOW = datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc)
SUPPLIER_ID = UUID("10000000-0000-0000-0000-000000000001")
EVIDENCE_ID = UUID("20000000-0000-0000-0000-000000000001")


class _Catalog:
    def __init__(self, view: EvidenceCatalogView) -> None:
        self.view = view

    def catalog(self, **_kwargs) -> EvidenceCatalogView:
        return self.view


def _request(path: str = "/suppliers") -> Request:
    router = Router(routes=[Mount("/static", app=Router(), name="static")])
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "query_string": b"",
            "headers": [],
            "scheme": "https",
            "server": ("testserver", 443),
            "router": router,
        }
    )


def _catalog(*, expired: bool = False) -> EvidenceCatalogView:
    evidence = EvidenceRecordView(
        public_id=EVIDENCE_ID,
        evidence_type="HUMAN_VERIFIED_SUPPLIER_DOCUMENT",
        source_reference="supplier-declaration.pdf",
        valid_from=NOW - timedelta(days=30),
        valid_until=NOW - timedelta(days=1) if expired else NOW + timedelta(days=300),
        verified_at=NOW - timedelta(days=10),
        verified_by="Verified reviewer",
        status="VERIFIED",
        claims=(
            EvidenceClaimView(field_name="species", value="Quercus alba"),
            EvidenceClaimView(
                field_name="country_of_harvest",
                value="United States",
            ),
        ),
    )
    product = SupplierProductEvidenceView(
        public_id=UUID("30000000-0000-0000-0000-000000000001"),
        product_key="white-oak-board",
        sku="OAK-001",
        display_name="White Oak Board",
        status="ACTIVE",
        evidence=(evidence,),
    )
    supplier = SupplierEvidenceView(
        public_id=SUPPLIER_ID,
        supplier_key="supplier-acme",
        display_name="Acme Timber",
        status="ACTIVE",
        products=(product,),
    )
    return EvidenceCatalogView(
        suppliers=(supplier,),
        supplier_count=1,
        product_count=1,
        evidence_count=1,
        claim_count=2,
        expiry=EvidenceExpiryView(
            next_30_days=0,
            days_31_60=0,
            days_61_90=0,
            expired=1 if expired else 0,
        ),
    )


def _service(view: EvidenceCatalogView) -> UsLaceySupplierIntelligenceService:
    service = UsLaceySupplierIntelligenceService(session_factory=lambda: None)
    service._catalog = _Catalog(view)
    return service


def _identity():
    return SimpleNamespace(
        legal_name="Litoral Trace",
        email="owner@example.com",
        full_name="David Lezcano",
    )


def _entitlement():
    return SimpleNamespace(
        remaining_operations=64,
        monthly_operation_limit=100,
        used_operations=36,
    )


def test_directory_marks_only_current_claims_verified() -> None:
    active_service = _service(_catalog(expired=False))
    active = active_service.directory(organization_id=7, as_of=NOW)

    assert active.supplier_count == 1
    assert active.verified_count == 1
    assert active.needs_review_count == 0
    assert active.active_claim_count == 2
    assert active.suppliers[0].status == "VERIFIED"
    assert active.suppliers[0].active_claim_count == 2
    assert active.suppliers[0].valid_until == NOW + timedelta(days=300)

    expired_service = _service(_catalog(expired=True))
    expired = expired_service.directory(organization_id=7, as_of=NOW)

    assert expired.verified_count == 0
    assert expired.needs_review_count == 1
    assert expired.active_claim_count == 0
    assert expired.suppliers[0].status == "NEEDS_REVIEW"


def test_supplier_detail_counts_distinct_supported_operations(monkeypatch) -> None:
    service = _service(_catalog(expired=False))
    monkeypatch.setattr(
        service,
        "_usage_for_evidence",
        lambda **_kwargs: {EVIDENCE_ID: {101, 102, 102}},
    )

    detail = service.detail(
        organization_id=7,
        supplier_public_id=SUPPLIER_ID,
        as_of=NOW,
    )

    assert detail.display_name == "Acme Timber"
    assert detail.total_products == 1
    assert detail.verified_claims == 2
    assert detail.operations_supported == 2
    assert detail.status == "VERIFIED"
    assert len(detail.evidence) == 1
    assert detail.evidence[0].used_in_shipments == 2
    assert detail.evidence[0].source_label == "Supplier Declaration"


def test_supplier_templates_expose_b2b_directory_and_reuse_value(monkeypatch) -> None:
    service = _service(_catalog(expired=False))
    monkeypatch.setattr(
        service,
        "_usage_for_evidence",
        lambda **_kwargs: {EVIDENCE_ID: {101, 102}},
    )
    directory = service.directory(organization_id=7, as_of=NOW)
    detail = service.detail(
        organization_id=7,
        supplier_public_id=SUPPLIER_ID,
        as_of=NOW,
    )

    list_html = render_suppliers_list(
        request=_request(),
        identity=_identity(),
        entitlement=_entitlement(),
        directory=directory,
    )
    for column in (
        "Supplier Name",
        "Products",
        "Active Evidence",
        "Valid Until",
        "Status",
    ):
        assert column in list_html
    assert "Acme Timber" in list_html
    assert "Verified" in list_html
    assert f"/suppliers/{SUPPLIER_ID}" in list_html

    detail_html = render_supplier_detail(
        request=_request(f"/suppliers/{SUPPLIER_ID}"),
        identity=_identity(),
        entitlement=_entitlement(),
        supplier=detail,
    )
    assert "Supplier intelligence" in detail_html
    assert "Total Products" in detail_html
    assert "Verified Claims" in detail_html
    assert "Operations Supported" in detail_html
    assert "Botanical species" in detail_html
    assert "Country of harvest" in detail_html
    assert "Source: Supplier Declaration" in detail_html
    assert "Used automatically in 2 shipments" in detail_html


def test_supplier_navigation_and_routes_are_real_customer_surfaces() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    base = (
        root / "src" / "litoral_trace" / "templates" / "us_lacey" / "base.html"
    ).read_text(encoding="utf-8")
    app = (
        root / "src" / "litoral_trace" / "web" / "us_lacey_pilot_app.py"
    ).read_text(encoding="utf-8")

    assert 'href="/suppliers"' in base
    assert ">Suppliers<" in base
    assert "terms: ['suppliers', 'supplier', 'vendor', 'memory', 'reusable evidence']" in base
    assert '@app.get("/suppliers"' in app
    assert '@app.get("/suppliers/{supplier_id}"' in app
