from __future__ import annotations

from pathlib import Path

from jinja2 import Environment


ROOT = Path(__file__).resolve().parents[1]
WEB_PATH = ROOT / "src/litoral_trace/web/eudr_acceptance.py"
TEMPLATE_PATH = ROOT / "src/litoral_trace/templates/eudr_acceptance_transport.html"
TRACEABILITY_PATH = ROOT / "src/litoral_trace/api/traceability.py"


def test_acceptance_transport_template_parses() -> None:
    source = TEMPLATE_PATH.read_text(encoding="utf-8")
    Environment().parse(source)


def test_acceptance_transport_browser_surface_is_composed() -> None:
    web = WEB_PATH.read_text(encoding="utf-8")
    composition = TRACEABILITY_PATH.read_text(encoding="utf-8")
    assert '"/eudr-acceptance/transport"' in web
    assert '"/eudr-acceptance/transport/prepare"' in web
    assert '"/eudr-acceptance/transport/submit"' in web
    assert "eudr_acceptance_web_router" in composition
    assert "router.include_router(eudr_acceptance_web_router)" in composition


def test_acceptance_transport_ui_is_conformance_and_permission_gated() -> None:
    source = TEMPLATE_PATH.read_text(encoding="utf-8")
    assert "{% if conformance.ready %}" in source
    assert "{% if acceptance_can_manage %}" in source
    assert "required_permission=Permission.TRACEABILITY_EVIDENCE" in WEB_PATH.read_text(encoding="utf-8")
    assert "Tu rol puede auditar el intento ACCEPTANCE" in source


def test_acceptance_transport_ui_never_offers_ambiguous_retry() -> None:
    source = TEMPLATE_PATH.read_text(encoding="utf-8")
    web = WEB_PATH.read_text(encoding="utf-8")
    assert "SENT" in source
    assert "TRANSPORT_ERROR" in source
    assert "no ofrece reintento" in source
    assert "allow_retry_after_transport_error=False" in web
    assert "allow_retry_after_transport_error=True" not in web
    assert "force_retry" not in web.lower()


def test_acceptance_transport_ui_exposes_no_credentials_and_labels_nonlegal_effect() -> None:
    source = TEMPLATE_PATH.read_text(encoding="utf-8")
    forbidden = (
        "authentication_key",
        "EUDR_ACCEPTANCE_USERNAME",
        "EUDR_ACCEPTANCE_AUTHENTICATION_KEY",
        "EUDR_ACCEPTANCE_WEB_SERVICE_CLIENT_ID",
        "PasswordDigest",
    )
    for token in forbidden:
        assert token not in source
    assert "NON_LEGAL_ACCEPTANCE" in source
    assert "LIVE no está habilitado" in source
