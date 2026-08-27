from __future__ import annotations

import pytest
from fastapi import HTTPException

import main
from litoral_trace.api.assurance import _require_document_intelligence_enabled
from litoral_trace.assurance.feature_flags import get_assurance_feature_flags


def test_assurance_universal_upload_and_progress_routes_are_registered():
    routes = {route.path for route in main.app.routes if hasattr(route, "path")}
    assert "/api/v1/assurance/documents" in routes
    assert "/api/v1/assurance/documents/{assurance_document_id}/progress" in routes


def test_assurance_remains_disabled_by_default(monkeypatch):
    for name in (
        "LT_ASSURANCE_V1_ENABLED",
        "LT_ASSURANCE_DOCUMENT_INTELLIGENCE_ENABLED",
    ):
        monkeypatch.delenv(name, raising=False)
    flags = get_assurance_feature_flags()
    assert flags.assurance_v1 is False
    assert flags.document_intelligence is False
    with pytest.raises(HTTPException) as exc_info:
        _require_document_intelligence_enabled()
    assert exc_info.value.status_code == 404


def test_assurance_document_intelligence_can_be_enabled_explicitly(monkeypatch):
    monkeypatch.setenv("LT_ASSURANCE_V1_ENABLED", "1")
    monkeypatch.setenv("LT_ASSURANCE_DOCUMENT_INTELLIGENCE_ENABLED", "1")
    flags = get_assurance_feature_flags()
    assert flags.assurance_v1 is True
    assert flags.document_intelligence is True
    _require_document_intelligence_enabled()
