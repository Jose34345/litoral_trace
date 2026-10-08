from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
T = ROOT / "src" / "litoral_trace" / "templates" / "us_lacey"


def test_enterprise_ux_51_60_contract() -> None:
    detail = (T / "operation_detail.html").read_text(encoding="utf-8")
    workspace = (T / "fragments" / "operation_workspace.html").read_text(encoding="utf-8")
    audit = (T / "fragments" / "activity_audit_log.html").read_text(encoding="utf-8")

    # 51-53: persistent shipment context and direct section navigation.
    assert 'data-shipment-section-nav' in detail
    for label in ("Summary", "Documents", "Human review", "Regulatory", "Package", "Audit trail"):
        assert label in detail
    assert 'id="shipment-summary"' in detail
    assert 'id="documents"' in detail
    assert 'id="operation-workspace"' in workspace

    # 54-56: readiness and human filing boundary stay explicit.
    assert 'data-readiness-summary' in detail
    assert 'data-human-filing-boundary' in detail
    assert "No automatic ACE / LAWGS submission" in detail
    assert "your authorized team remains responsible for final review and filing" in detail

    # 57-58: evidence reuse and raw retention are first-class.
    assert 'data-reuse-memory-summary' in detail
    assert 'data-raw-retention' in detail
    assert "Add evidence" in detail

    # 59: regulatory section can be opened from the workspace nav.
    assert 'data-open-regulatory-tab' in detail
    assert 'data-review-tab-button="regulatory"' in workspace

    # 60: audit remains embedded and deep-linkable.
    assert 'id="activity-audit-log"' in audit
    assert '/audit-log?operation_id=' in audit
