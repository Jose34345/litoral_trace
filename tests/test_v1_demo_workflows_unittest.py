from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "src" / "litoral_trace" / "templates" / "dashboard.html"
DEMO_GUIDE = ROOT / "COMMERCIAL_DEMO_GUIDE.md"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_dashboard_uses_real_tenant_lotes_and_satellite_api() -> None:
    content = _read(DASHBOARD)

    assert "/api/v1/lotes" in content
    assert "/api/v1/satellite/jobs" in content
    assert "X-CSRF-Token" in content
    assert "no se muestran rodales ficticios" in content
    assert "No emite un certificado oficial EUDR" in content


def test_dashboard_does_not_present_fabricated_compliance_success() -> None:
    content = _read(DASHBOARD).lower()

    forbidden = (
        "100% compliant",
        "0 bloqueos",
        "certificados dds emitidos",
        "apto eudr",
        "traces nt listos",
    )

    for phrase in forbidden:
        assert phrase not in content


def test_demo_guide_contains_no_embedded_demo_password() -> None:
    content = _read(DEMO_GUIDE).lower()

    assert "admin123" not in content
    assert "credenciales provisionadas por el canal seguro" in content
    assert "nunca incluir usuarios o contraseñas de demo" in content


def test_demo_guide_preserves_compliance_boundary() -> None:
    content = _read(DEMO_GUIDE).lower()

    assert "no se presenta como certificadora" in content
    assert "no constituyen por sí solos prueba legal" in content
    assert "artefactos de soporte al proceso de debida diligencia" in content
