"""Environment-driven rollout switches for Assurance v1.

Every new Assurance surface defaults to OFF. This lets staging/pilots enable the
new workflow without altering the current production behavior.
"""
from __future__ import annotations

from dataclasses import dataclass
import os

_TRUTHY = frozenset({"1", "true", "yes", "on"})
_FALSY = frozenset({"0", "false", "no", "off"})


def _read_flag(name: str, *, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in _TRUTHY:
        return True
    if normalized in _FALSY:
        return False
    raise RuntimeError(f"{name} debe ser booleano.")


@dataclass(frozen=True, slots=True)
class AssuranceFeatureFlags:
    assurance_v1: bool = False
    document_intelligence: bool = False
    reconciliation: bool = False
    preflight_v2: bool = False
    market_ready_inventory: bool = False

    @classmethod
    def from_environment(cls) -> "AssuranceFeatureFlags":
        master = _read_flag("LT_ASSURANCE_V1_ENABLED", default=False)
        return cls(
            assurance_v1=master,
            document_intelligence=_read_flag(
                "LT_ASSURANCE_DOCUMENT_INTELLIGENCE_ENABLED", default=master
            ),
            reconciliation=_read_flag(
                "LT_ASSURANCE_RECONCILIATION_ENABLED", default=master
            ),
            preflight_v2=_read_flag(
                "LT_ASSURANCE_PREFLIGHT_V2_ENABLED", default=master
            ),
            market_ready_inventory=_read_flag(
                "LT_ASSURANCE_MARKET_READY_INVENTORY_ENABLED", default=master
            ),
        )

    def enabled(self, name: str) -> bool:
        if not hasattr(self, name):
            raise KeyError(f"Feature flag desconocida: {name}")
        return bool(getattr(self, name))


def get_assurance_feature_flags() -> AssuranceFeatureFlags:
    return AssuranceFeatureFlags.from_environment()
