from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
T = ROOT / "src" / "litoral_trace" / "templates" / "us_lacey"


def test_enterprise_ux_51_60_contract() -> None:
    detail = (T / "operation_detail.html").read_text(encoding="utf-8")
    workspace = (T / "fragments" / "operation_workspace.html").read_text(encoding="utf-8")
    audit = (T / "fragments" / "activity_audit_log.html").read_text(encoding="utf-8")
    summary = (T / "fragments" / "shipment_workspace_summary.html").read_text(encoding="utf-8")
    regulatory = (T / "fragments" / "shipment_package_regulatory.html").read_text(encoding="utf-8")

    # 51-53: persistent shipment context and direct section navigation.
    assert 'data-shipment-section-nav' in detail
    nav = re.search(r"<nav[^>]*data-shipment-section-nav.*?</nav>", detail, re.S).group()
    assert re.findall(r'href="([^"]+)"', nav) == [
        "#operation-workspace", "#documents", "#declaration-package",
    ]
    for label in ("Review", "Documents", "Package"):
        assert label in nav
    assert 'shipment_workspace_summary.html' in detail
    assert 'id="shipment-summary"' in summary
    assert 'id="documents"' in detail
    assert 'id="operation-workspace"' in workspace

    # 54-56: readiness and human filing boundary stay explicit.
    assert 'data-readiness-summary' in summary
    assert 'data-human-filing-boundary' in summary
    assert "No automatic ACE / LAWGS submission" in summary
    assert "your authorized team remains responsible for final review and filing" in summary

    # 57-58: evidence reuse and raw retention are first-class.
    assert 'data-reuse-memory-summary' in summary
    assert 'data-raw-retention' in detail
    assert "Add evidence" in detail

    # 59: regulatory section can be opened from the workspace nav.
    assert 'href="#declaration-package"' in detail
    assert 'data-review-tab-button="regulatory"' in workspace
    assert 'id="review-panel-regulatory"' in regulatory
    assert 'data-package-regulatory' in regulatory

    # 60: audit remains embedded and deep-linkable.
    assert 'href="#activity-audit-log"' in detail
    assert 'data-unified-audit' in detail
    assert 'id="activity-audit-log"' in audit
    assert '/audit-log?operation_id=' in audit
