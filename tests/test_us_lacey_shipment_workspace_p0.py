from pathlib import Path
from types import SimpleNamespace
import re
import pytest
from jinja2 import Environment, FileSystemLoader, select_autoescape
from tests.test_us_lacey_phase_f_export_ui import _render_export_card

ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src/litoral_trace/templates"
ENV = Environment(loader=FileSystemLoader(str(TEMPLATES)), autoescape=select_autoescape(("html",)))

def test_three_primary_sections_keep_existing_anchor_destinations():
    source=(TEMPLATES/"us_lacey/operation_detail.html").read_text(encoding="utf-8")
    nav=re.search(r"<nav[^>]*data-shipment-section-nav.*?</nav>",source,re.S).group()
    html=ENV.from_string(nav).render(private_v2=False,processing=SimpleNamespace(terminal=True,failed=False),readiness_summary=SimpleNamespace(exception_count=9),detail=SimpleNamespace(document_count=3))
    assert re.findall(r'href="([^"]+)"',html)==["#operation-workspace","#documents","#declaration-package"]
    assert "Overview" not in html and "Audit trail" not in html

@pytest.mark.parametrize("pending,supported,confirmed",[(9,25,3),(1,0,7),(0,0,8)])
def test_summary_displays_server_counts_and_keeps_readiness(pending,supported,confirmed):
    html=ENV.get_template("us_lacey/fragments/shipment_workspace_summary.html").render(detail=SimpleNamespace(status="READY_FOR_REVIEW",documents=(object(),)),processing=SimpleNamespace(terminal=True,failed=False),private_v2=False,readiness_summary=SimpleNamespace(overall="NOT READY",exception_count=pending,package_ready=False,species="MISSING",country_of_harvest="MISSING"),auto_supported_fields=[object()]*supported,settled_fields=[object()]*confirmed,reused_evidence_summary=None)
    assert f'data-workspace-count="pending">{pending}<' in html
    assert f'data-workspace-count="supported">{supported}<' in html
    assert f'data-workspace-count="confirmed">{confirmed}<' in html
    assert "NOT READY" in html
    assert 'href="#review-summary"' in html
    assert "No automatic ACE / LAWGS submission" in html

def test_blocked_package_folds_formats_and_offers_remaining_requirements():
    html=_render_export_card(status="READY_FOR_REVIEW",open_items=9,auto_items=25)
    assert "Declaration package — Not ready" in html
    assert 'data-package-formats-details' in html
    details=re.search(r'<details[^>]*data-package-formats-details.*?</details>',html,re.S).group()
    assert ' open' not in details.split('>')[0]
    assert details.count('aria-disabled="true"')==2
    assert 'href="#review-summary"' in html
    assert 'data-export-download' not in html

def test_ready_package_keeps_real_downloads_without_fold():
    html=_render_export_card(status="COMPLETED")
    assert html.count('data-export-download')==2
    assert 'data-package-formats-details' not in html

def test_htmx_refresh_replaces_summary_and_moves_regulatory_view_to_package():
    text=(TEMPLATES/"us_lacey/fragments/operation_workspace.html").read_text(encoding="utf-8")
    assert 'shipment_workspace_summary.html' in text
    assert 'data-review-filter-bar' in text
    assert 'data-package-regulatory' in _render_export_card(status="READY_FOR_REVIEW",open_items=1)


def test_applicability_is_preserved_across_workspace_replacement():
    script=(ROOT/"src/litoral_trace/static/js/shipment_workspace.js").read_text(encoding="utf-8")
    assert "htmx:beforeSwap" in script and "staging.append(source)" in script
    assert "htmx:afterSettle" in script and "slot.append(source)" in script
    assert "decodeURIComponent" in script and "getElementById" in script


@pytest.mark.parametrize("method", ["GET", "POST"])
def test_post_fragment_has_single_summary_and_no_nested_oob_status_cards(monkeypatch,method):
    from litoral_trace.web import us_lacey_operational_views as views
    from tests.test_us_lacey_regulatory_assessment_ui import _detail, _request, _engine2
    monkeypatch.setattr(views,"_semantic_evidence_for_detail",lambda *_:{})
    monkeypatch.setattr(views,"_product_intelligence_for_detail",lambda *a,**k:None)
    monkeypatch.setattr(views,"_regulatory_assessment_for_detail",lambda *a,**k:None)
    request=_request();request.scope["method"]=method
    html=views.render_operation_workspace(request=request,identity=SimpleNamespace(organization_id=7),detail=_detail(),engine2_dossier=_engine2(),complete_csrf="test",review_csrf={})
    for id in ("shipment-summary","shipment-readiness-card","species-status","country-status"):
        assert html.count(f'id="{id}"')==1
    summary=html[html.index('<section id="shipment-summary"'):]
    assert summary.count('hx-swap-oob=')==1


def test_header_collapses_technical_uuid_and_copies_only_that_value():
    source=(TEMPLATES/"us_lacey/operation_detail.html").read_text(encoding="utf-8")
    header=re.search(r'<header class="lt-page-header".*?</header>',source,re.S).group()
    assert 'data-copy-operation-id' in header and 'data-operation-id-value' in header
    assert '<details' in header and 'data-open-shipment-audit' in header
    assert '<span>{{ business_reference }}</span>' not in header
    script=(ROOT/"src/litoral_trace/static/js/shipment_workspace.js").read_text(encoding="utf-8")
    assert '[data-operation-id-value]' in script
    assert 'shouldSwap === false' in script
