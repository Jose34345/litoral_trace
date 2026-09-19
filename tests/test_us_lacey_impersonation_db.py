from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from litoral_trace.us_lacey import impersonation_db


class _ScalarResult:
    def scalar_one(self):
        return impersonation_db.IMPERSONATION_ROLE


class _FakeTransaction:
    def __init__(self, events):
        self.events = events

    def rollback(self):
        self.events.append(("rollback", None))


class _FakeConnection:
    def __init__(self, events):
        self.events = events

    def __enter__(self):
        self.events.append(("enter", None))
        return self

    def __exit__(self, exc_type, exc, tb):
        self.events.append(("exit", None))

    def begin(self):
        self.events.append(("begin", None))
        return _FakeTransaction(self.events)

    def exec_driver_sql(self, statement):
        self.events.append(("driver", statement))

    def execute(self, statement, params=None):
        rendered = str(statement)
        self.events.append(("execute", rendered, params))
        if rendered == "SELECT current_user":
            return _ScalarResult()
        return SimpleNamespace()


class _FakeEngine:
    def __init__(self, events):
        self.events = events

    def connect(self):
        return _FakeConnection(self.events)


def _request(method: str = "GET") -> Request:
    return Request(
        {
            "type": "http",
            "method": method,
            "path": "/admin/impersonation/operations",
            "headers": [],
            "query_string": b"",
            "server": ("testserver", 443),
            "scheme": "https",
        }
    )


def test_impersonation_engine_has_no_primary_database_fallback(monkeypatch) -> None:
    impersonation_db.reset_impersonation_engine_state()
    monkeypatch.delenv("US_LACEY_IMPERSONATION_DATABASE_URL", raising=False)
    monkeypatch.setenv(
        "US_LACEY_DATABASE_URL",
        "postgresql+psycopg://must-not-be-used.invalid/db",
    )

    with pytest.raises(RuntimeError, match="US_LACEY_IMPERSONATION_DATABASE_URL"):
        impersonation_db.get_impersonation_engine()


def test_context_rejects_mutating_http_method_before_database_access() -> None:
    with pytest.raises(HTTPException) as exc:
        impersonation_db.resolve_readonly_impersonation_context(
            _request("POST"),
            us_session="not-used",
            impersonation_token="not-used",
        )

    assert exc.value.status_code == 405
    assert exc.value.headers == {"Allow": "GET, HEAD, OPTIONS"}


def test_readonly_dependency_sets_transaction_role_and_rls_then_rolls_back(
    monkeypatch,
) -> None:
    events = []
    monkeypatch.setattr(
        impersonation_db,
        "get_impersonation_engine",
        lambda: _FakeEngine(events),
    )

    context = impersonation_db.ReadOnlyImpersonationContext(
        session_id=123,
        admin_user_id=10,
        admin_organization_id=20,
        target_organization_id=30,
    )

    dependency = impersonation_db.readonly_impersonation_db(
        _request("GET"),
        context,
    )

    connection = next(dependency)
    assert isinstance(connection, _FakeConnection)

    dependency.close()

    assert events[0:4] == [
        ("enter", None),
        ("begin", None),
        ("driver", "SET TRANSACTION READ ONLY"),
        (
            "driver",
            "SET LOCAL ROLE litoral_trace_impersonation_reader",
        ),
    ]
    set_config = events[4]
    assert set_config[0] == "execute"
    assert "set_config" in set_config[1]
    assert set_config[2] == {"org_id": "30"}
    assert events[5] == ("execute", "SELECT current_user", None)
    assert ("rollback", None) in events
    assert events[-1] == ("exit", None)


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
def test_readonly_dependency_rejects_mutating_methods(method: str) -> None:
    context = impersonation_db.ReadOnlyImpersonationContext(
        session_id=1,
        admin_user_id=1,
        admin_organization_id=1,
        target_organization_id=1,
    )

    with pytest.raises(HTTPException) as exc:
        next(
            impersonation_db.readonly_impersonation_db(
                _request(method),
                context,
            )
        )

    assert exc.value.status_code == 405
