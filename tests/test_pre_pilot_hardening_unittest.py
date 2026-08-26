from __future__ import annotations

from pathlib import Path

from starlette.requests import Request

from litoral_trace.web.csrf import (
    CSRF_BROWSER_COOKIE_KEY,
    CSRF_HEADER_NAME,
    create_csrf_browser_nonce,
    create_csrf_token,
)
from litoral_trace.web.middleware import validate_cookie_csrf_request
from litoral_trace.auth.sessions import (
    ACCESS_TOKEN_COOKIE_KEY,
    REFRESH_TOKEN_COOKIE_KEY,
)


ROOT = Path(__file__).resolve().parents[1]
STATIC_JS = ROOT / "src" / "litoral_trace" / "static" / "src" / "js"
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
SECRET = "pre-pilot-test-secret-key-with-at-least-32-chars"


def _request(*, browser_nonce: str, csrf_token: str, access_token: str = "expired") -> Request:
    cookies = "; ".join(
        (
            f"{ACCESS_TOKEN_COOKIE_KEY}={access_token}",
            f"{REFRESH_TOKEN_COOKIE_KEY}=refresh-cookie-value",
            f"{CSRF_BROWSER_COOKIE_KEY}={browser_nonce}",
        )
    )
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "scheme": "https",
        "path": "/api/v1/auth/refresh",
        "raw_path": b"/api/v1/auth/refresh",
        "query_string": b"",
        "headers": [
            (b"cookie", cookies.encode("utf-8")),
            (CSRF_HEADER_NAME.lower().encode("ascii"), csrf_token.encode("utf-8")),
        ],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 443),
        "root_path": "",
    }
    return Request(scope)


def test_refresh_accepts_browser_bound_csrf_after_access_token_is_unusable() -> None:
    browser_nonce = create_csrf_browser_nonce()
    refresh_csrf = create_csrf_token(
        subject=None,
        browser_nonce=browser_nonce,
        secret_key=SECRET,
    )

    assert (
        validate_cookie_csrf_request(
            _request(browser_nonce=browser_nonce, csrf_token=refresh_csrf),
            secret_key=SECRET,
        )
        is None
    )


def test_refresh_rejects_browser_token_bound_to_another_browser() -> None:
    browser_nonce = create_csrf_browser_nonce()
    other_nonce = create_csrf_browser_nonce()
    refresh_csrf = create_csrf_token(
        subject=None,
        browser_nonce=other_nonce,
        secret_key=SECRET,
    )

    assert validate_cookie_csrf_request(
        _request(browser_nonce=browser_nonce, csrf_token=refresh_csrf),
        secret_key=SECRET,
    ) == "csrf_invalid"


def test_base_loads_pre_pilot_hardening_layers() -> None:
    base = (TEMPLATES / "base.html").read_text(encoding="utf-8")
    assert "lt-refresh-csrf-token" in base
    assert "lt-session-refresh-after" in base
    assert "/src/js/session-renewal.js" in base
    assert "/src/js/evidence-context.js" in base
    assert "/src/js/datetime-local.js" in base
    assert "/src/js/business-language.js" in base


def test_session_renewal_preserves_forms_and_rotates_rendered_csrf() -> None:
    source = (STATIC_JS / "session-renewal.js").read_text(encoding="utf-8")
    assert 'input[name="csrf_token"]' in source
    assert "window.location.href" in source
    assert "DOMParser" in source
    assert "credentials: \"same-origin\"" in source
    assert "localStorage" not in source
    assert "sessionStorage" not in source


def test_evidence_context_never_silently_enables_default_subject_mutations() -> None:
    source = (STATIC_JS / "evidence-context.js").read_text(encoding="utf-8")
    assert "SHIPMENT|" in source
    assert "Confirmá el eslabón antes de vincular" in source
    assert 'form[action="/evidence/link"]' in source
    assert 'form[action="/evidence/upload-link"]' in source
    assert "button.disabled = true" in source


def test_datetime_local_guard_handles_timestamptz_rendering_without_timezone_shift() -> None:
    source = (STATIC_JS / "datetime-local.js").read_text(encoding="utf-8")
    assert 'input[type="datetime-local"]' in source
    assert "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}" in source
    assert "getAttribute(\"value\")" in source
    assert "getTimezoneOffset" not in source


def test_business_language_layer_translates_internal_codes_without_mutating_values() -> None:
    source = (STATIC_JS / "business-language.js").read_text(encoding="utf-8")
    required_pairs = (
        ('"CONFORMANCE_READY", "Preparado para conformidad"',),
        ('"POSTED", "Contabilizado"',),
        ('"DISPATCHED", "Despachado"',),
        ('"RISK_CONCLUSION", "Conclusión de riesgo"',),
        ('"RISK_ASSESSED_AT", "Fecha de evaluación de riesgo"',),
    )
    for (pair,) in required_pairs:
        assert pair in source
    assert "node.nodeValue = translated" in source
    assert ".value =" not in source
