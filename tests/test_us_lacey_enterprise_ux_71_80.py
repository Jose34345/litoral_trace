from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
T = ROOT / "src" / "litoral_trace" / "templates" / "us_lacey"


def test_enterprise_ux_71_80_contract() -> None:
    audit = (T / "audit_log_list.html").read_text(encoding="utf-8")
    regulatory = (T / "regulatory_overview.html").read_text(encoding="utf-8")

    assert 'name="actor_type"' in audit
    assert "Human actions" in audit
    assert "Automated events" in audit
    assert "pagination.total_results" in audit
    assert "pagination.total_pages" in audit
    assert "Audit log pages" in audit
    assert "Open shipment record" in audit

    assert 'data-regulatory-control-catalog' in regulatory
    for control in ("HTS schedule coverage", "Declaration applicability", "Botanical identity", "Country of harvest", "Plant quantity and unit", "Rule-specific exceptions"):
        assert control in regulatory
    assert "us_lacey_ruleset_version" in regulatory
    assert "us_lacey_hts_catalog_version" in regulatory
    assert 'data-regulatory-status-semantics' in regulatory
    assert "Supported" in regulatory
    assert "Needs information" in regulatory
    assert "Not required" in regulatory

    assert 'data-regulatory-boundary' in regulatory
    assert "Preparation, not automatic filing" in regulatory
    assert 'data-regulatory-next-actions' in regulatory
    assert '/operations?state=needs_review' in regulatory
    assert 'href="/operations"' in regulatory
