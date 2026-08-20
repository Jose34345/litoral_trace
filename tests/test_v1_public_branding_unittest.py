from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HOME = ROOT / "src" / "litoral_trace" / "templates" / "public" / "home.html"
LOGIN = ROOT / "src" / "litoral_trace" / "templates" / "login.html"
PUBLIC_BASE = (
    ROOT / "src" / "litoral_trace" / "templates" / "public" / "base_public.html"
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_public_home_is_spanish_first_and_commercially_actionable() -> None:
    content = _read(HOME)

    assert "Evidencia trazable para decisiones de debida diligencia EUDR" in content
    assert "Solicitar demostración" in content
    assert "Acceso clientes" in content
    assert "id=\"platform\"" in content
    assert "id=\"eudr\"" in content
    assert "id=\"security\"" in content
    assert "id=\"regional-intelligence\"" in content


def test_public_site_preserves_regulatory_boundary() -> None:
    content = _read(HOME).lower()

    assert "no emite certificaciones eudr" in content
    assert "no representa una certificación automática" in content
    for forbidden in (
        "100% compliant",
        "cumplimiento garantizado",
        "aprobación garantizada",
    ):
        assert forbidden not in content


def test_login_contains_no_embedded_demo_credentials_or_stress_test_copy() -> None:
    content = _read(LOGIN).lower()

    assert 'value="admin"' not in content
    assert 'value="admin123"' not in content
    assert "stress test en vivo" not in content
    assert "acceso seguro a litoral trace" in content
    assert "aislamiento por organización" in content


def test_public_navigation_uses_customer_facing_spanish_ctas() -> None:
    content = _read(PUBLIC_BASE)

    assert "Plataforma" in content
    assert "Inteligencia regional" in content
    assert "Auditabilidad" in content
    assert "Acceso clientes" in content
    assert "Solicitar demo" in content
