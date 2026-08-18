from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NGINX_CONFIG = ROOT / "nginx" / "nginx.conf"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


def _nginx_text() -> str:
    return NGINX_CONFIG.read_text(encoding="utf-8")


def _ci_text() -> str:
    return CI_WORKFLOW.read_text(encoding="utf-8")


def test_login_rate_limit_covers_browser_and_api_post_routes() -> None:
    nginx = _nginx_text()

    for token in (
        'map "$request_method:$uri" $auth_login_limit_key',
        '"POST:/login" $binary_remote_addr;',
        '"POST:/api/v1/auth/login" $binary_remote_addr;',
        "zone=auth_login_per_ip:10m rate=10r/m",
        "limit_req zone=auth_login_per_ip burst=5 nodelay;",
        "limit_req_status 429;",
    ):
        assert token in nginx


def test_refresh_rate_limit_covers_refresh_post_route() -> None:
    nginx = _nginx_text()

    for token in (
        'map "$request_method:$uri" $auth_refresh_limit_key',
        '"POST:/api/v1/auth/refresh" $binary_remote_addr;',
        "zone=auth_refresh_per_ip:10m rate=60r/m",
        "limit_req zone=auth_refresh_per_ip burst=30 nodelay;",
        "limit_req_status 429;",
    ):
        assert token in nginx


def test_browser_security_headers_include_csp_and_modern_baseline() -> None:
    nginx = _nginx_text()

    required_headers = (
        'add_header X-Frame-Options "SAMEORIGIN" always;',
        'add_header X-Content-Type-Options "nosniff" always;',
        'add_header X-XSS-Protection "0" always;',
        'add_header Referrer-Policy "strict-origin-when-cross-origin" always;',
        'add_header Permissions-Policy "camera=(), microphone=(), geolocation=(), payment=(), usb=()" always;',
        'add_header Cross-Origin-Opener-Policy "same-origin" always;',
        'add_header Cross-Origin-Resource-Policy "same-origin" always;',
        'add_header X-Permitted-Cross-Domain-Policies "none" always;',
        "add_header Content-Security-Policy ",
    )

    for token in required_headers:
        assert token in nginx

    for directive in (
        "default-src 'self'",
        "base-uri 'self'",
        "form-action 'self'",
        "frame-ancestors 'self'",
        "object-src 'none'",
        "connect-src 'self'",
        "upgrade-insecure-requests",
    ):
        assert directive in nginx

    assert 'X-XSS-Protection "1; mode=block"' not in nginx


def test_ci_runs_on_v1_closure_and_syntax_checks_nginx() -> None:
    workflow = _ci_text()

    assert "- p2-v1-closure" in workflow
    assert "Validate production Nginx configuration" in workflow
    assert "nginx:1.25-alpine" in workflow
    assert "nginx -t" in workflow
