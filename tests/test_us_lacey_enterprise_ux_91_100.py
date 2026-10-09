from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
T = ROOT / "src" / "litoral_trace" / "templates" / "us_lacey"
PILOT = ROOT / "src" / "litoral_trace" / "web" / "us_lacey_pilot_app.py"


def test_enterprise_ux_91_100_contract() -> None:
    trust = (T / "trust_center.html").read_text(encoding="utf-8")
    base = (T / "base.html").read_text(encoding="utf-8")
    pilot = PILOT.read_text(encoding="utf-8")

    assert '@app.get("/trust"' in pilot
    assert 'href="/trust"' in base
    assert "Trust &amp; Controls" in base
    assert "label: 'Trust & Controls'" in base

    assert "Workspace isolated" in trust
    assert "Customer-facing records are scoped to the active organization" in trust
    assert "Raw evaluation source retention" in trust
    assert "evaluation contribution is off by default" in trust
    assert "Audit trail enabled" in trust
    assert "Regulatory analysis is tied to explicit versions" in trust
    assert "us_lacey_ruleset_version" in trust
    assert "us_lacey_hts_catalog_version" in trust
    assert "authorized team" in trust
    assert "Control matrix" in trust

    for href in ("/audit-log", "/operations", "/regulatory", "/legal/privacy", "/legal/product-use-terms"):
        assert f'href="{href}"' in trust
