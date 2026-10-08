from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates" / "us_lacey"


def test_enterprise_ux_41_50_contract() -> None:
    operations = (TEMPLATES / "operations.html").read_text(encoding="utf-8")
    base = (TEMPLATES / "base.html").read_text(encoding="utf-8")

    # 41: user-facing queue language is shipment-centric.
    assert "Compliance work queue" in operations
    assert ">Shipments<" in operations
    assert ">Shipments</span>" in base
    assert "New shipment" in operations

    # 42-45: enterprise queue controls.
    assert 'name="q"' in operations
    assert 'name="state"' in operations
    assert 'name="sort"' in operations
    for state in ("Needs review", "Ready", "Processing", "Completed"):
        assert state in operations
    for sort_label in ("Recently updated", "Most exceptions", "Newest created", "Reference A–Z"):
        assert sort_label in operations

    # 46: bounded server pagination.
    assert "pagination.total_pages" in operations
    assert "Previous" in operations
    assert "Next" in operations

    # 47-48: queue exposes priority and operational freshness.
    assert '"Priority"' in operations
    assert "item.exception_count >= 5" in operations
    assert "relative_time(item.updated_at)" in operations

    # 49-50: filter state is explicit and reversible.
    assert "No shipments match these filters" in operations
    assert "Clear filters" in operations
    assert "pagination.total_results" in operations
