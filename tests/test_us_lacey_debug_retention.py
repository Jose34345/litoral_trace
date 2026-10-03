from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from litoral_trace.config.settings import StorageSettings
import litoral_trace.us_lacey.ingestion as ingestion_module
from litoral_trace.us_lacey.ingestion import UsLaceyIngestionService
from litoral_trace.us_lacey.sandbox import UsLaceyDebugRetentionPolicy


OPERATION_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


def _service() -> UsLaceyIngestionService:
    service = object.__new__(UsLaceyIngestionService)
    service._storage_settings = StorageSettings(
        backend="s3",
        bucket_name="private-test-bucket",
        key_prefix="us-lacey",
    )
    return service


def test_no_debug_consent_keeps_standard_private_storage_prefix(monkeypatch):
    monkeypatch.setattr(
        ingestion_module,
        "get_us_lacey_debug_retention_policy",
        lambda **_kwargs: UsLaceyDebugRetentionPolicy(
            support_debug_consent=False,
            debug_retention_until=None,
        ),
    )
    service = _service()

    settings = service._storage_settings_for_document(
        organization_id=42,
        operation_public_id=OPERATION_ID,
    )

    assert settings.normalized_key_prefix == "us-lacey"
    assert "support-quarantine" not in settings.normalized_key_prefix


def test_debug_consent_routes_originals_to_operation_scoped_quarantine(monkeypatch):
    retention_until = datetime.now(timezone.utc) + timedelta(hours=72)
    monkeypatch.setattr(
        ingestion_module,
        "get_us_lacey_debug_retention_policy",
        lambda **_kwargs: UsLaceyDebugRetentionPolicy(
            support_debug_consent=True,
            debug_retention_until=retention_until,
        ),
    )
    service = _service()

    settings = service._storage_settings_for_document(
        organization_id=42,
        operation_public_id=OPERATION_ID,
    )

    assert settings.normalized_key_prefix == (
        f"us-lacey/support-quarantine/{OPERATION_ID}"
    )


def test_debug_consent_without_deadline_fails_closed_before_storage(monkeypatch):
    monkeypatch.setattr(
        ingestion_module,
        "get_us_lacey_debug_retention_policy",
        lambda **_kwargs: UsLaceyDebugRetentionPolicy(
            support_debug_consent=True,
            debug_retention_until=None,
        ),
    )
    service = _service()

    with pytest.raises(
        RuntimeError,
        match="missing its retention deadline",
    ):
        service._storage_settings_for_document(
            organization_id=42,
            operation_public_id=OPERATION_ID,
        )
