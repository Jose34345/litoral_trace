from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "src" / "litoral_trace" / "templates" / "us_lacey" / "base.html"
AUDIT = ROOT / "src" / "litoral_trace" / "templates" / "us_lacey" / "audit_log_list.html"


def test_enterprise_ux_81_90_contract() -> None:
    base = BASE.read_text(encoding="utf-8")
    audit = AUDIT.read_text(encoding="utf-8")

    assert 'data-command-palette' in base
    assert 'role="combobox"' in base
    assert 'aria-controls="lt-command-results"' in base
    assert 'role="listbox"' in base
    assert "ArrowDown" in base
    assert "ArrowUp" in base
    assert "Escape" in base
    assert "aria-activedescendant" in base
    assert "No workspace destination matches this search." in base

    for label in ("Shipments", "New shipment", "Evidence", "Suppliers", "Regulatory Analysis", "Audit Log"):
        assert f"label: '{label}'" in base
    assert "{% if not evaluation_mode %}{label: 'Billing'" in base

    assert 'aria-current="page"' in base
    assert 'href="#main-content"' in base
    assert ">Shipments</span>" in base
    assert '["Time", "Shipment", "Actor", "Event", "Details"]' in audit
    assert "Search / Shipment" in audit
