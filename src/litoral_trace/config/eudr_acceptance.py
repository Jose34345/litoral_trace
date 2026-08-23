"""Fail-closed configuration for EUDR Information System ACCEPTANCE only."""
from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field


ACCEPTANCE_HOST = "acceptance.eudr.webcloud.ec.europa.eu"
DDS_V3_SERVICE_PATH = "/tracesnt/ws/EUDRDueDiligenceStatementServiceV3"

READINESS_TRANSPORT_READY = "TRANSPORT_READY"
READINESS_WAITING_FOR_CREDENTIALS = "WAITING_FOR_ACCEPTANCE_CREDENTIALS"
READINESS_SMOKE_READY = "ACCEPTANCE_SMOKE_READY"


def _optional(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def _bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"{name} debe ser booleano.")


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} debe ser entero.") from exc


class EudrAcceptanceSettings(BaseModel):
    """Runtime-only web-service settings; secrets are never database fields."""

    model_config = ConfigDict(frozen=True)

    enabled: bool = False
    endpoint_url: str | None = None
    username: str | None = Field(default=None, repr=False)
    authentication_key: str | None = Field(default=None, repr=False)
    web_service_client_id: str | None = Field(default=None, repr=False)
    connect_timeout_seconds: int = Field(default=10, ge=1, le=120)
    read_timeout_seconds: int = Field(default=30, ge=1, le=300)
    timestamp_validity_seconds: int = Field(default=60, ge=1, le=60)

    def model_post_init(self, __context: object) -> None:
        if not self.endpoint_url:
            return
        parsed = urlsplit(self.endpoint_url)
        if parsed.scheme != "https":
            raise ValueError("EUDR ACCEPTANCE requiere HTTPS.")
        if parsed.hostname != ACCEPTANCE_HOST:
            raise ValueError("EUDR_ACCEPTANCE_ENDPOINT_URL sólo puede apuntar al host ACCEPTANCE oficial.")
        if parsed.username or parsed.password:
            raise ValueError("El endpoint EUDR no puede contener credenciales.")
        normalized_path = parsed.path.rstrip("/")
        if normalized_path != DDS_V3_SERVICE_PATH:
            raise ValueError(
                "EUDR_ACCEPTANCE_ENDPOINT_URL debe apuntar exactamente al servicio DDS V3 ACCEPTANCE."
            )

    @property
    def network_ready(self) -> bool:
        return bool(
            self.enabled
            and self.endpoint_url
            and self.username
            and self.authentication_key
            and self.web_service_client_id
        )

    @property
    def missing_network_configuration(self) -> tuple[str, ...]:
        """Return variable names only; never return credential values."""
        return tuple(
            name
            for name, value in (
                ("EUDR_ACCEPTANCE_ENDPOINT_URL", self.endpoint_url),
                ("EUDR_ACCEPTANCE_USERNAME", self.username),
                ("EUDR_ACCEPTANCE_AUTHENTICATION_KEY", self.authentication_key),
                ("EUDR_ACCEPTANCE_WEB_SERVICE_CLIENT_ID", self.web_service_client_id),
            )
            if not value
        )

    @property
    def readiness_state(self) -> str:
        if self.network_ready:
            return READINESS_SMOKE_READY
        if self.enabled:
            return READINESS_WAITING_FOR_CREDENTIALS
        return READINESS_TRANSPORT_READY

    def sanitized_readiness(self) -> dict[str, Any]:
        """Expose deployment readiness without credential material."""
        return {
            "state": self.readiness_state,
            "enabled": self.enabled,
            "network_ready": self.network_ready,
            "endpoint_configured": bool(self.endpoint_url),
            "username_configured": bool(self.username),
            "authentication_key_configured": bool(self.authentication_key),
            "web_service_client_id_configured": bool(self.web_service_client_id),
            "missing_configuration": list(self.missing_network_configuration),
            "target_environment": "ACCEPTANCE",
            "api_family": "V3",
            "legal_effect": "NONE_NON_LEGAL_ACCEPTANCE",
            "live_enabled": False,
        }

    def require_network_ready(self) -> None:
        if not self.enabled:
            raise RuntimeError("EUDR_ACCEPTANCE_ENABLED está deshabilitado.")
        missing = list(self.missing_network_configuration)
        if missing:
            raise RuntimeError("Configuración ACCEPTANCE incompleta: " + ", ".join(missing))

    @classmethod
    def from_environment(cls) -> "EudrAcceptanceSettings":
        return cls(
            enabled=_bool("EUDR_ACCEPTANCE_ENABLED", False),
            endpoint_url=_optional("EUDR_ACCEPTANCE_ENDPOINT_URL"),
            username=_optional("EUDR_ACCEPTANCE_USERNAME"),
            authentication_key=_optional("EUDR_ACCEPTANCE_AUTHENTICATION_KEY"),
            web_service_client_id=_optional("EUDR_ACCEPTANCE_WEB_SERVICE_CLIENT_ID"),
            connect_timeout_seconds=_int("EUDR_ACCEPTANCE_CONNECT_TIMEOUT_SECONDS", 10),
            read_timeout_seconds=_int("EUDR_ACCEPTANCE_READ_TIMEOUT_SECONDS", 30),
            timestamp_validity_seconds=_int("EUDR_ACCEPTANCE_TIMESTAMP_VALIDITY_SECONDS", 60),
        )


def get_eudr_acceptance_settings() -> EudrAcceptanceSettings:
    return EudrAcceptanceSettings.from_environment()
