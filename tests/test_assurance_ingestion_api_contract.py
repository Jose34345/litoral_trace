from __future__ import annotations

import ast
from pathlib import Path

import pytest
from fastapi import HTTPException

from litoral_trace.api.assurance import (
    _require_document_intelligence_enabled,
    build_assurance_router,
)
from litoral_trace.api.assurance_exceptions import (
    _require_operational_exceptions_enabled,
)
from litoral_trace.api.assurance_preflight import _require_preflight_enabled
from litoral_trace.assurance.feature_flags import get_assurance_feature_flags


ROOT = Path(__file__).resolve().parents[1]
MAIN_PATH = ROOT / "main.py"


def _main_registers_assurance_router() -> bool:
    tree = ast.parse(MAIN_PATH.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        if not (
            isinstance(function, ast.Attribute)
            and function.attr == "include_router"
            and isinstance(function.value, ast.Name)
            and function.value.id == "app"
        ):
            continue
        if node.args and isinstance(node.args[0], ast.Name):
            if node.args[0].id == "assurance_router":
                return True
    return False


def test_assurance_operational_routes_are_registered_on_each_router_instance():
    assurance_router = build_assurance_router()
    routes = {route.path for route in assurance_router.routes if hasattr(route, "path")}
    assert "/api/v1/assurance/documents" in routes
    assert "/api/v1/assurance/documents/{assurance_document_id}/progress" in routes
    assert "/api/v1/assurance/preflight" in routes
    assert "/api/v1/assurance/preflight/reasons" in routes
    assert "/api/v1/assurance/exceptions" in routes
    assert "/api/v1/assurance/exceptions/{exception_id}/resolve" in routes
    assert _main_registers_assurance_router() is True


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


def test_preflight_has_independent_fail_closed_feature_flag(monkeypatch):
    monkeypatch.setenv("LT_ASSURANCE_V1_ENABLED", "1")
    monkeypatch.setenv("LT_ASSURANCE_PREFLIGHT_V2_ENABLED", "0")
    with pytest.raises(HTTPException) as exc_info:
        _require_preflight_enabled()
    assert exc_info.value.status_code == 404

    monkeypatch.setenv("LT_ASSURANCE_PREFLIGHT_V2_ENABLED", "1")
    _require_preflight_enabled()


def test_operational_exceptions_have_independent_fail_closed_feature_flag(monkeypatch):
    monkeypatch.setenv("LT_ASSURANCE_V1_ENABLED", "1")
    monkeypatch.setenv("LT_ASSURANCE_OPERATIONAL_EXCEPTIONS_ENABLED", "0")
    with pytest.raises(HTTPException) as exc_info:
        _require_operational_exceptions_enabled()
    assert exc_info.value.status_code == 404

    monkeypatch.setenv("LT_ASSURANCE_OPERATIONAL_EXCEPTIONS_ENABLED", "1")
    _require_operational_exceptions_enabled()
