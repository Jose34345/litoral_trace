"""Feature flags for the Assurance v1 pivot.

All Assurance functionality is disabled by default so the current production
surface can coexist with the new workflow while it is validated with pilots.
Flags are intentionally environment-driven and dependency-free.
"""
from __future__ import annotations

from dataclasses import dataclass
import os

_TRUTHY = {"1", "true", "yes", "on", "enabled"}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUTHY


@dataclass(frozen=True, slots=True)
class FeatureFlags:
    """Runtime switches for incremental rollout of Assurance v1."""

    assurance_v1: bool = False
    document_ingestion_v1: bool = False
    automated_reconciliation_v1: bool = False
    preflight_v2: bool = False
    market_ready_inventory_v1: bool = False

    @classmethod
    def from_env(cls) -> "FeatureFlags":
        assurance_enabled = _env_bool("LT_ASSURANCE_V1_ENABLED", False)
        return cls(
            assurance_v1=assurance_enabled,
            document_ingestion_v1=_env_bool(
                "LT_DOCUMENT_INGESTION_V1_ENABLED", assurance_enabled
            ),
            automated_reconciliation_v1=_env_bool(
                "LT_RECONCILIATION_V1_ENABLED", assurance_enabled
            ),
            preflight_v2=_env_bool("LT_PREFLIGHT_V2_ENABLED", assurance_enabled),
            market_ready_inventory_v1=_env_bool(
                "LT_MARKET_READY_INVENTORY_V1_ENABLED", assurance_enabled
            ),
        )

    def enabled(self, flag_name: str) -> bool:
        """Return one named flag, rejecting typos instead of silently disabling it."""
        if not hasattr(self, flag_name):
            raise KeyError(f"Unknown feature flag: {flag_name}")
        return bool(getattr(self, flag_name))


def get_feature_flags() -> FeatureFlags:
    """Read flags from the current environment.

    Kept as a function rather than a module-level singleton so tests and
    deployment configuration changes are deterministic.
    """
    return FeatureFlags.from_env()
