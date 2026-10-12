"""Fail-closed rules for the owner-only production Assurance V2 laboratory."""
from __future__ import annotations

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from litoral_trace.us_lacey.assurance_v2_review_adapter import review_journal_enabled
from litoral_trace.web.us_lacey_assurance_v2_runtime import configure_assurance_v2_shadow


def test_production_lab_cannot_start_without_explicit_organization_allowlist(monkeypatch):
    monkeypatch.setenv("US_LACEY_ENVIRONMENT", "production")
    monkeypatch.delenv("US_LACEY_ASSURANCE_V2_LAB_ORG_IDS", raising=False)
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_MODE", "SHADOW")
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_COMMANDS", "ON")
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_REVIEW_JOURNAL", "ON")
    app = FastAPI()
    with pytest.raises(ValueError, match="allowlist"):
        configure_assurance_v2_shadow(app)


def test_production_review_journal_restricted_to_allowlisted_test_shipments(monkeypatch):
    monkeypatch.setenv("US_LACEY_ENVIRONMENT", "production")
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_REVIEW_JOURNAL", "ON")
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_LAB_ORG_IDS", "1")
    assert review_journal_enabled(1, "V2LAB-PACK1")
    assert review_journal_enabled(1, "v2lab-pack2")
    assert not review_journal_enabled(1, "LACEY-20261012-CUSTOMER")
    assert not review_journal_enabled(2, "V2LAB-PACK1")
    assert not review_journal_enabled()
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_REVIEW_JOURNAL", "OFF")
    assert not review_journal_enabled(1, "V2LAB-PACK1")


def test_disabled_lab_keeps_all_legacy_portal_routes_unmodified(monkeypatch):
    monkeypatch.setenv("US_LACEY_ENVIRONMENT", "production")
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_MODE", "OFF")
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_LAB_ORG_IDS", "1")
    app = FastAPI()
    assert configure_assurance_v2_shadow(app) is False
    assert app.state.assurance_v2_lab_org_ids == frozenset()
    assert not any(route.path.startswith("/assurance-v2") for route in app.routes)


def test_explicit_private_lab_registers_only_scoped_routes(monkeypatch):
    monkeypatch.setenv("US_LACEY_ENVIRONMENT", "production")
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_MODE", "SHADOW")
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_LAB_ORG_IDS", "1")
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_COMMANDS", "ON")
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_REVIEW_JOURNAL", "ON")
    app = FastAPI()
    assert configure_assurance_v2_shadow(app)
    assert app.state.assurance_v2_lab_org_ids == frozenset({1})
    assert any(route.path == "/assurance-v2" for route in app.routes)
    assert any(route.path == "/assurance-v2/operations/{operation_id}" for route in app.routes)
    # A visitor without a valid U.S. portal cookie is forbidden.
    with TestClient(app) as client:
        response = client.get("/assurance-v2", follow_redirects=False)
    assert response.status_code == 401
