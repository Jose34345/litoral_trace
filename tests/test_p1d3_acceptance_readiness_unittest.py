from __future__ import annotations

import json
from pathlib import Path

import pytest

from litoral_trace.config.eudr_acceptance import (
    ACCEPTANCE_HOST,
    DDS_V3_SERVICE_PATH,
    READINESS_SMOKE_READY,
    READINESS_TRANSPORT_READY,
    READINESS_WAITING_FOR_CREDENTIALS,
    EudrAcceptanceSettings,
)


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = (ROOT / "docker-compose.prod.yml").read_text(encoding="utf-8")
ENV_EXAMPLE = (ROOT / ".env.example").read_text(encoding="utf-8")
API = (ROOT / "src/litoral_trace/api/eudr_acceptance.py").read_text(encoding="utf-8")


def _endpoint() -> str:
    return f"https://{ACCEPTANCE_HOST}{DDS_V3_SERVICE_PATH}"


def test_disabled_runtime_is_transport_ready_without_network() -> None:
    settings = EudrAcceptanceSettings()
    payload = settings.sanitized_readiness()

    assert settings.readiness_state == READINESS_TRANSPORT_READY
    assert payload["network_ready"] is False
    assert payload["live_enabled"] is False
    assert payload["target_environment"] == "ACCEPTANCE"
    assert set(payload["missing_configuration"]) == {
        "EUDR_ACCEPTANCE_ENDPOINT_URL",
        "EUDR_ACCEPTANCE_USERNAME",
        "EUDR_ACCEPTANCE_AUTHENTICATION_KEY",
        "EUDR_ACCEPTANCE_WEB_SERVICE_CLIENT_ID",
    }


def test_enabled_incomplete_runtime_waits_for_authorized_credentials() -> None:
    settings = EudrAcceptanceSettings(enabled=True, endpoint_url=_endpoint())

    assert settings.readiness_state == READINESS_WAITING_FOR_CREDENTIALS
    assert settings.network_ready is False
    assert settings.missing_network_configuration == (
        "EUDR_ACCEPTANCE_USERNAME",
        "EUDR_ACCEPTANCE_AUTHENTICATION_KEY",
        "EUDR_ACCEPTANCE_WEB_SERVICE_CLIENT_ID",
    )


def test_complete_runtime_is_smoke_ready_but_never_leaks_secret_values() -> None:
    settings = EudrAcceptanceSettings(
        enabled=True,
        endpoint_url=_endpoint(),
        username="operator-secret-username",
        authentication_key="auth-secret-value",
        web_service_client_id="client-secret-id",
    )
    payload = settings.sanitized_readiness()
    serialized = json.dumps(payload, sort_keys=True)

    assert settings.readiness_state == READINESS_SMOKE_READY
    assert payload["network_ready"] is True
    assert payload["missing_configuration"] == []
    assert "operator-secret-username" not in serialized
    assert "auth-secret-value" not in serialized
    assert "client-secret-id" not in serialized


def test_acceptance_endpoint_remains_pinned_to_official_acceptance_boundary() -> None:
    with pytest.raises(ValueError):
        EudrAcceptanceSettings(
            enabled=True,
            endpoint_url=f"https://example.com{DDS_V3_SERVICE_PATH}",
        )


def test_production_compose_passes_acceptance_values_only_to_app_runtime() -> None:
    app_section, worker_section = COMPOSE.split("  worker:", 1)
    for variable in (
        "EUDR_ACCEPTANCE_ENABLED",
        "EUDR_ACCEPTANCE_ENDPOINT_URL",
        "EUDR_ACCEPTANCE_USERNAME",
        "EUDR_ACCEPTANCE_AUTHENTICATION_KEY",
        "EUDR_ACCEPTANCE_WEB_SERVICE_CLIENT_ID",
    ):
        assert f"{variable}: ${{{variable}" in app_section
        assert f"{variable}:" not in worker_section

    assert "EUDR_ACCEPTANCE_ENABLED: ${EUDR_ACCEPTANCE_ENABLED:-0}" in app_section


def test_env_template_and_api_are_safe_and_discoverable() -> None:
    assert "EUDR_ACCEPTANCE_ENABLED=0" in ENV_EXAMPLE
    assert f"# EUDR_ACCEPTANCE_ENDPOINT_URL={_endpoint()}" in ENV_EXAMPLE
    assert "# EUDR_ACCEPTANCE_AUTHENTICATION_KEY=<load-from-secret-store>" in ENV_EXAMPLE
    assert '@router.get("/readiness")' in API
    assert "sanitized_readiness()" in API
    assert '"live_enabled": False' in API
