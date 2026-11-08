"""Public-browser configuration for the U.S. Lacey self-service portal."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os

from litoral_trace.us_lacey.config import load_us_lacey_runtime_config


class UsLaceyPortalConfigurationError(RuntimeError):
    pass


@dataclass(frozen=True)
class UsLaceyPortalConfig:
    public_origin: str
    terms_url: str
    privacy_url: str
    beta_terms_url: str
    session_cookie_secure: bool


def _required_https_url(env: Mapping[str, str], name: str) -> str:
    value = str(env.get(name, "")).strip()
    if not value:
        raise UsLaceyPortalConfigurationError(f"{name} is required.")
    if not value.lower().startswith("https://"):
        raise UsLaceyPortalConfigurationError(f"{name} must use HTTPS.")
    return value.rstrip("/")


def load_us_lacey_portal_config(
    environ: Mapping[str, str] | None = None,
) -> UsLaceyPortalConfig:
    env = os.environ if environ is None else environ
    runtime = load_us_lacey_runtime_config(env)
    environment = runtime.environment.strip().lower()
    public_origin = f"https://{runtime.app_hostname.strip().lower()}"
    return UsLaceyPortalConfig(
        public_origin=public_origin,
        terms_url=_required_https_url(env, "US_LACEY_TERMS_URL"),
        privacy_url=_required_https_url(env, "US_LACEY_PRIVACY_URL"),
        beta_terms_url=_required_https_url(env, "US_LACEY_BETA_TERMS_URL"),
        session_cookie_secure=environment not in {"test", "development", "local"},
    )
