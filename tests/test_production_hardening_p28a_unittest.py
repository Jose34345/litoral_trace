from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NGINX_CONF_PATH = ROOT / "nginx" / "nginx.conf"
COMPOSE_PATH = ROOT / "docker-compose.prod.yml"
RUNBOOK_PATH = ROOT / "DEPLOYMENT_RUNBOOK.md"


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _server_blocks(config_text: str) -> list[str]:
    blocks: list[str] = []
    token = "server {"
    start = 0

    while True:
        index = config_text.find(token, start)
        if index == -1:
            break

        depth = 0
        end = index
        while end < len(config_text):
            char = config_text[end]
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    end += 1
                    break
            end += 1

        blocks.append(config_text[index:end])
        start = end

    return blocks


def _http_server_block(config_text: str) -> str:
    for block in _server_blocks(config_text):
        if "listen 80;" in block:
            return block
    raise AssertionError("HTTP server block not found.")


def _https_server_block(config_text: str) -> str:
    for block in _server_blocks(config_text):
        if "listen 443 ssl;" in block:
            return block
    raise AssertionError("HTTPS server block not found.")


def _load_docs_contract(environment: str) -> dict[str, str | None]:
    command = [
        sys.executable,
        "-c",
        (
            "import json, main; "
            "print(json.dumps({"
            "'docs_url': main.app.docs_url, "
            "'redoc_url': main.app.redoc_url, "
            "'openapi_url': main.app.openapi_url"
            "}))"
        ),
    ]
    env = dict(**__import__("os").environ)
    env["ENVIRONMENT"] = environment
    result = subprocess.run(
        command,
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout.strip())


def test_p28a_nginx_http_redirects_to_https():
    config_text = _read_text(NGINX_CONF_PATH)
    http_block = _http_server_block(config_text)

    assert "listen 80;" in http_block
    assert "return 301 https://$host$request_uri;" in http_block
    assert "Strict-Transport-Security" not in http_block


def test_p28a_nginx_has_real_tls_listener_and_certificate_contract():
    config_text = _read_text(NGINX_CONF_PATH)
    https_block = _https_server_block(config_text)

    assert "listen 443 ssl;" in https_block
    assert "ssl_certificate /etc/nginx/certs/fullchain.pem;" in https_block
    assert "ssl_certificate_key /etc/nginx/certs/privkey.pem;" in https_block
    assert "ssl_protocols TLSv1.2 TLSv1.3;" in https_block
    assert "TLSv1;" not in https_block
    assert "TLSv1.1" not in https_block


def test_p28a_hsts_is_scoped_to_https():
    config_text = _read_text(NGINX_CONF_PATH)
    http_block = _http_server_block(config_text)
    https_block = _https_server_block(config_text)

    assert "Strict-Transport-Security" not in http_block
    assert (
        'add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;'
        in https_block
    )


def test_p28a_https_proxy_preserves_forwarding_headers():
    config_text = _read_text(NGINX_CONF_PATH)
    https_block = _https_server_block(config_text)

    assert "proxy_set_header Host $host;" in https_block
    assert "proxy_set_header X-Real-IP $remote_addr;" in https_block
    assert "proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;" in https_block
    assert "proxy_set_header X-Forwarded-Proto $scheme;" in https_block


def test_p28a_production_disables_fastapi_schema_endpoints():
    contract = _load_docs_contract("production")

    assert contract == {
        "docs_url": None,
        "redoc_url": None,
        "openapi_url": None,
    }


def test_p28a_non_production_preserves_developer_docs():
    contract = _load_docs_contract("development")

    assert contract == {
        "docs_url": "/docs",
        "redoc_url": "/redoc",
        "openapi_url": "/openapi.json",
    }


def test_p28a_compose_keeps_tls_certificates_read_only():
    compose_text = _read_text(COMPOSE_PATH)

    assert '- "80:80"' in compose_text
    assert '- "443:443"' in compose_text
    assert "- ./nginx/certs:/etc/nginx/certs:ro" in compose_text


def test_p28a_runbook_documents_tls_and_docs_contract():
    runbook_text = _read_text(RUNBOOK_PATH)

    assert "Nginx is the canonical TLS termination point." in runbook_text
    assert "Port 80 only redirects to HTTPS." in runbook_text
    assert "/etc/nginx/certs/fullchain.pem" in runbook_text
    assert "/etc/nginx/certs/privkey.pem" in runbook_text
    assert "must not be committed" in runbook_text
    assert "provisioned before deployment" in runbook_text
    assert "private-key permissions must remain restricted" in runbook_text
    assert "renewal/rotation is an operational responsibility" in runbook_text
    assert "/docs" in runbook_text
    assert "/redoc" in runbook_text
    assert "/openapi.json" in runbook_text
