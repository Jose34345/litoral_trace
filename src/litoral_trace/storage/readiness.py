"""Safe readiness checks for the private Vault object-storage dependency."""
from __future__ import annotations

import sys
from collections.abc import Callable
from typing import Any

from litoral_trace.config import get_settings
from litoral_trace.config.settings import Settings, StorageSettings
from litoral_trace.storage.s3 import Boto3S3ObjectStorage


StorageFactory = Callable[[StorageSettings], Any]


def is_vault_storage_ready(
    *,
    settings: Settings | None = None,
    require_configured: bool | None = None,
    storage_factory: StorageFactory = Boto3S3ObjectStorage,
) -> bool:
    """Return a sanitized readiness verdict without leaking provider details."""
    try:
        active_settings = settings or get_settings()
    except Exception:
        return False

    required = (
        active_settings.is_production
        if require_configured is None
        else bool(require_configured)
    )

    storage_settings = active_settings.storage

    if not storage_settings.is_configured:
        return not required

    try:
        storage = storage_factory(storage_settings)
        return bool(storage.health_check())
    except Exception:
        return False


def main() -> int:
    """CLI used by controlled deployment preflight and runtime verification."""
    try:
        active_settings = get_settings()
    except Exception:
        print(
            "Vault storage readiness: configuration invalid.",
            file=sys.stderr,
        )
        return 1

    if not is_vault_storage_ready(
        settings=active_settings,
        require_configured=active_settings.is_production,
    ):
        print(
            "Vault storage readiness: unavailable.",
            file=sys.stderr,
        )
        return 1

    print("Vault storage readiness: ready.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())