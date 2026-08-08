import os
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from litoral_trace.db.tenant import (
    TENANT_CONTEXT_GUC,
    get_tenant_scoped_db_session,
    set_tenant_db_context,
)


def _build_session_mock(*, dialect_name: str = "postgresql") -> Mock:
    session = Mock()
    session.get_bind.return_value = SimpleNamespace(
        dialect=SimpleNamespace(name=dialect_name)
    )
    session.execute = Mock()
    session.commit = Mock()
    return session


def test_set_tenant_db_context_uses_transaction_local_guc_for_postgresql():
    session = _build_session_mock()

    normalized = set_tenant_db_context(session, 42)

    assert normalized == 42
    session.execute.assert_called_once()
    statement = session.execute.call_args.args[0]
    parameters = session.execute.call_args.args[1]
    assert TENANT_CONTEXT_GUC in str(statement)
    assert "set_config" in str(statement)
    assert "true" in str(statement).lower()
    assert parameters == {"organization_id": "42"}
    session.commit.assert_not_called()


def test_set_tenant_db_context_rejects_invalid_organization_id():
    session = _build_session_mock()

    with pytest.raises(ValueError):
        set_tenant_db_context(session, "abc")

    session.execute.assert_not_called()
    session.commit.assert_not_called()


def test_set_tenant_db_context_is_noop_outside_postgresql():
    session = _build_session_mock(dialect_name="sqlite")

    normalized = set_tenant_db_context(session, 7)

    assert normalized == 7
    session.execute.assert_not_called()
    session.commit.assert_not_called()


def test_set_tenant_db_context_does_not_modify_global_process_state(monkeypatch):
    session = _build_session_mock()
    monkeypatch.setenv("LT_TENANT_SENTINEL", "unchanged")

    set_tenant_db_context(session, 11)

    assert os.environ["LT_TENANT_SENTINEL"] == "unchanged"


def test_get_tenant_scoped_db_session_applies_context(monkeypatch):
    session = _build_session_mock()
    monkeypatch.setattr(
        "litoral_trace.db.tenant.get_db_session",
        lambda: session,
    )

    scoped_session = get_tenant_scoped_db_session(99)

    assert scoped_session is session
    session.execute.assert_called_once()
