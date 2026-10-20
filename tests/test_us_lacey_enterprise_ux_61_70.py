from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
T = ROOT / "src" / "litoral_trace" / "templates" / "us_lacey"


def test_enterprise_ux_61_70_contract() -> None:
    suppliers = (T / "suppliers_list.html").read_text(encoding="utf-8")
    supplier_detail = (T / "supplier_detail.html").read_text(encoding="utf-8")
    evidence = (T / "evidence.html").read_text(encoding="utf-8")

    # 61-63: supplier directory is searchable, filterable and sortable.
    assert 'data-supplier-search' in suppliers
    assert 'data-supplier-status="VERIFIED"' in suppliers
    assert 'data-supplier-status="NEEDS_REVIEW"' in suppliers
    assert 'data-supplier-sort' in suppliers
    assert "Most active claims" in suppliers
    assert "Nearest expiry" in suppliers

    # 64-66: evidence registry supports search and validity triage.
    assert 'data-evidence-search' in evidence
    for state in ("active", "expiring", "expired"):
        assert f'data-evidence-validity="{state}"' in evidence
    assert 'data-valid-until=' in evidence
    assert "Expiring ≤30d" in evidence

    # 67-69: provenance, validity and realized reuse are visible.
    assert "Source:" in supplier_detail
    assert "Valid through" in supplier_detail
    assert "Used automatically in" in supplier_detail
    assert "Evidence ID" in supplier_detail
    assert "Verified claim" in supplier_detail

    # 70: filtered zero-state feedback is explicit.
    assert "No suppliers match these filters" in suppliers
    assert "No evidence matches these filters" in evidence
