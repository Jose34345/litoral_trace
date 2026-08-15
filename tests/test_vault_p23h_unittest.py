from __future__ import annotations

import asyncio
from dataclasses import dataclass

import main
from litoral_trace.storage import readiness as readiness_module


@dataclass
class FakeStorageSettings:
    is_configured: bool


@dataclass
class FakeSettings:
    is_production: bool
    storage: FakeStorageSettings


class FakeStorage:
    def __init__(self, ready: bool):
        self.ready = ready

    def health_check(self) -> bool:
        return self.ready


class FakeSession:
    def __init__(self, *, fail_execute: bool = False):
        self.fail_execute = fail_execute
        self.closed = False
        self.rolled_back = False

    def execute(self, _statement):
        if self.fail_execute:
            raise RuntimeError("database unavailable")
        return None

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def test_unconfigured_storage_is_allowed_outside_production():
    settings = FakeSettings(
        is_production=False,
        storage=FakeStorageSettings(
            is_configured=False,
        ),
    )

    assert readiness_module.is_vault_storage_ready(
        settings=settings,
    ) is True


def test_unconfigured_storage_fails_closed_in_production():
    settings = FakeSettings(
        is_production=True,
        storage=FakeStorageSettings(
            is_configured=False,
        ),
    )

    assert readiness_module.is_vault_storage_ready(
        settings=settings,
    ) is False


def test_configured_storage_uses_health_check_and_returns_true():
    settings = FakeSettings(
        is_production=True,
        storage=FakeStorageSettings(
            is_configured=True,
        ),
    )

    assert readiness_module.is_vault_storage_ready(
        settings=settings,
        storage_factory=lambda _settings: FakeStorage(True),
    ) is True


def test_configured_storage_health_failure_returns_false_without_exception():
    settings = FakeSettings(
        is_production=True,
        storage=FakeStorageSettings(
            is_configured=True,
        ),
    )

    assert readiness_module.is_vault_storage_ready(
        settings=settings,
        storage_factory=lambda _settings: FakeStorage(False),
    ) is False


def test_storage_factory_exception_fails_closed_without_leaking():
    settings = FakeSettings(
        is_production=True,
        storage=FakeStorageSettings(
            is_configured=True,
        ),
    )

    def failing_factory(_settings):
        raise RuntimeError(
            "provider secret detail must not escape"
        )

    assert readiness_module.is_vault_storage_ready(
        settings=settings,
        storage_factory=failing_factory,
    ) is False


def test_api_readiness_requires_database_and_vault_storage(monkeypatch):
    session = FakeSession()

    monkeypatch.setattr(
        main,
        "get_db_session",
        lambda: session,
    )
    monkeypatch.setattr(
        main,
        "is_vault_storage_ready",
        lambda: True,
    )

    response = asyncio.run(
        main.readiness_check()
    )

    assert response.status_code == 200
    assert response.body == b'{"status":"ready"}'
    assert session.closed is True


def test_api_readiness_returns_503_when_vault_storage_is_unavailable(monkeypatch):
    session = FakeSession()

    monkeypatch.setattr(
        main,
        "get_db_session",
        lambda: session,
    )
    monkeypatch.setattr(
        main,
        "is_vault_storage_ready",
        lambda: False,
    )

    response = asyncio.run(
        main.readiness_check()
    )

    assert response.status_code == 503
    assert response.body == b'{"status":"unavailable"}'
    assert session.closed is True


def test_api_readiness_stops_at_database_failure(monkeypatch):
    session = FakeSession(
        fail_execute=True,
    )
    storage_called = False

    def storage_readiness():
        nonlocal storage_called
        storage_called = True
        return True

    monkeypatch.setattr(
        main,
        "get_db_session",
        lambda: session,
    )
    monkeypatch.setattr(
        main,
        "is_vault_storage_ready",
        storage_readiness,
    )

    response = asyncio.run(
        main.readiness_check()
    )

    assert response.status_code == 503
    assert response.body == b'{"status":"unavailable"}'
    assert session.rolled_back is True
    assert session.closed is True
    assert storage_called is False