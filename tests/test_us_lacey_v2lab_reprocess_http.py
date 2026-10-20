"""Private V2 lab replay HTTP policy: explicit access, CSRF and no upload."""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException
from starlette.requests import Request

from litoral_trace.web import us_lacey_pilot_app as portal


def _request(*, allowed: bool) -> Request:
    app = FastAPI()
    app.state.assurance_v2_lab_org_ids = frozenset({1}) if allowed else frozenset()
    return Request({
        "type": "http", "method": "POST",
        "path": "/operations/00000000-0000-0000-0000-000000000001/actions/reprocess",
        "headers": [], "app": app,
    })


def test_reprocess_route_requires_explicit_lab_allowlist(monkeypatch):
    monkeypatch.setattr(
        portal, "_operational_mutation_context",
        lambda _cookie: (SimpleNamespace(organization_id=1, user_id=7), None),
    )
    called = []
    monkeypatch.setattr(
        portal, "reprocess_existing_v2lab_documents",
        lambda **kwargs: called.append(kwargs),
    )
    with pytest.raises(HTTPException) as err:
        portal.operation_reprocess_existing_documents(
            "00000000-0000-0000-0000-000000000001",
            _request(allowed=False), csrf_token="dummy", us_session="session",
        )
    assert err.value.status_code == 404
    assert called == []


def test_reprocess_route_validates_csrf_and_wakes_worker(monkeypatch):
    monkeypatch.setattr(
        portal, "_operational_mutation_context",
        lambda _cookie: (SimpleNamespace(organization_id=1, user_id=7), None),
    )
    called = []
    monkeypatch.setattr(
        portal, "verify_us_lacey_csrf",
        lambda **kwargs: called.append(("csrf", kwargs)),
    )
    monkeypatch.setattr(
        portal, "reprocess_existing_v2lab_documents",
        lambda **kwargs: called.append(("replay", kwargs)) or 6,
    )
    monkeypatch.setattr(
        portal, "wake_us_lacey_worker",
        lambda: called.append(("wake", None)),
    )
    operation = "00000000-0000-0000-0000-000000000001"
    result = portal.operation_reprocess_existing_documents(
        operation, _request(allowed=True),
        csrf_token="valid-csrf", us_session="session",
    )
    assert result.status_code == 303
    assert result.headers["location"].endswith("?reprocessing=1")
    assert [item[0] for item in called] == ["csrf", "replay", "wake"]
    assert called[0][1]["purpose"] == f"reprocess:{operation}"
    assert called[1][1]["organization_id"] == 1
    assert called[1][1]["user_id"] == 7
