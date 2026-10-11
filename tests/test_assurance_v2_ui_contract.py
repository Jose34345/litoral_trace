"""Standalone UI contract: only authority gateway owns decisions."""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient
from fastapi.staticfiles import StaticFiles

from litoral_trace.us_lacey.exporters.assurance_v2_package import MemoryPackageStore, issue_assurance_package
from litoral_trace.web.us_lacey_assurance_v2_views import (
    WorkspacePrincipal, create_assurance_v2_router,
)
from test_assurance_v2_outputs_core import OPERATION_ID, make_case, authorize


def make_client(case=None, *, test_issue_enabled=True, test_authorization_enabled=False):
    case = case or make_case()
    receipts: list[dict[str, str]] = []
    store = MemoryPackageStore()
    app = FastAPI()
    # Production mounts the shared static app; the inherited Jinja shell
    # resolves its CSS/favicon through url_for("static", ...).
    static_dir = Path(__file__).resolve().parents[1] / "src/litoral_trace/static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    def principal(request: Request) -> WorkspacePrincipal:
        tenant = request.headers.get("X-Demo-Tenant")
        if not tenant:
            raise HTTPException(401, "Authentication required")
        return WorkspacePrincipal(organization_id=int(tenant),
                                  actor_id="user:42", csrf_token="fixture-csrf")

    def read(principal: WorkspacePrincipal, operation_id: str):
        if principal.organization_id == case.organization_id and operation_id == case.operation_id:
            return case
        return None

    def gateway(principal: WorkspacePrincipal, operation_id: str, command: dict[str, str]):
        receipts.append(command)
        return {"status": "PENDING_AUTHORITY", "request_id": "fixture-request-1"}

    def fixture_issue(principal: WorkspacePrincipal, operation_id: str,
                      expected_source_set_fingerprint: str, expected_case_fingerprint: str):
        # This fixture's local MemoryPackageStore is NOT the production gateway.
        # A real gateway must re-read, lock, and persist atomically in PostgreSQL.
        assert expected_source_set_fingerprint == case.source_set_fingerprint
        assert expected_case_fingerprint == case.authority_fingerprint()
        package = issue_assurance_package(case)
        store.save(organization_id=principal.organization_id,
                   operation_id=operation_id, package=package)
        return {"status": "FILING_READY", "package_fingerprint": package.fingerprint,
                "submitted_to_agency": False}

    def fixture_authorize(principal: WorkspacePrincipal, operation_id: str,
                          expected_source_set_fingerprint: str, expected_case_fingerprint: str,
                          reason: str, explicit_confirmation: bool):
        nonlocal case
        # Synthetic fixture only; production authority persists a separately
        # confirmed audit event and rechecks the same case under DB locks.
        assert principal.organization_id == case.organization_id
        assert operation_id == case.operation_id
        assert expected_source_set_fingerprint == case.source_set_fingerprint
        assert expected_case_fingerprint == case.authority_fingerprint()
        assert explicit_confirmation is True and reason
        case = authorize(case)
        return case.export_authorization

    app.include_router(create_assurance_v2_router(
        principal_for=principal,
        case_for=read,
        decision_gateway=gateway,
        csrf_verify=lambda principal, token: token == principal.csrf_token,
        package_store=store,
        issue_gateway=fixture_issue if test_issue_enabled else None,
        authorization_gateway=fixture_authorize if test_authorization_enabled else None,
        document_link_for=lambda principal, op, doc, page:
            f"/operations/{op}/documents/{doc}" + (f"?page={page}" if page else ""),
    ))
    return TestClient(app), receipts


def test_ui_separates_candidate_supported_human_and_memory_and_links_origin():
    case = make_case()
    field = list(case.fields)
    field[0] = {**field[0], "authority": "CANDIDATE"}
    field[1] = {**field[1], "authority": "HUMAN_CONFIRMED"}
    field[2] = {**field[2], "authority": "VERIFIED_REUSE"}
    ex = {
        "id": "ex-1", "field_name": "genus", "line_reference": "LINE-1",
        "entity_ref": "sup-us-1/prod-oak", "blocking": True,
        "status": "OPEN", "reason": "Conflicting species",
        "historical_evidence": "hist-1, expiring 2030",
        "candidates": [{"id": "cand-1", "value": "Quercus",
                        "document_id": "doc-invoice", "document_name": "Invoice",
                        "page": 2, "locator": "p2:span:7", "evidence_id": "ev1"}],
    }
    client, _ = make_client(authorize(replace(case, fields=tuple(field), exceptions=(ex,))))
    html = client.get(f"/assurance-v2/operations/{OPERATION_ID}",
                      headers={"X-Demo-Tenant": "11"})
    assert html.status_code == 200
    for label in ["CANDIDATE · NOT CONFIRMED", "SOURCE SUPPORTED",
                  "HUMAN DECISION", "VERIFIED MEMORY", "HISTORICAL MEMORY",
                  "View original source", "Blocked for final export", "SHA-256"]:
        assert label in html.text
    assert f"/operations/{OPERATION_ID}/documents/doc-invoice?page=2" in html.text
    assert "Conflicting species" in html.text


def test_review_action_never_mutates_frontend_truth_and_requires_csrf():
    case = authorize(replace(make_case(), exceptions=({
        "id": "ex-1", "field_name": "genus", "status": "OPEN",
        "blocking": True, "line_reference": "LINE-1", "candidates": [],
    },)))
    client, sent = make_client(case)
    url = f"/assurance-v2/operations/{OPERATION_ID}/decisions"
    command = {"csrf_token": "fixture-csrf", "exception_id": "ex-1",
               "action": "CORRECT", "value": "Quercus",
               "reason": "Reviewed supplier certificate"}
    assert client.post(url, json={**command, "csrf_token": "invalid"},
                       headers={"X-Demo-Tenant": "11"}).status_code == 403
    assert client.post(url, json=command, headers={"X-Demo-Tenant": "12"}).status_code == 404
    response = client.post(url, json=command, headers={"X-Demo-Tenant": "11"})
    assert response.status_code == 202
    assert response.json()["status"] == "PENDING_AUTHORITY"
    assert sent[0]["expected_source_set_fingerprint"] == case.source_set_fingerprint
    assert sent[0]["expected_case_fingerprint"] == case.authority_fingerprint()
    assert case.fields[0]["value"] == "2026-10-25"


def test_authorized_package_endpoints_are_tenant_scoped_and_reproducible():
    client, _ = make_client()
    base = f"/assurance-v2/operations/{OPERATION_ID}"
    headers = {"X-Demo-Tenant": "11"}
    assert client.get(base).status_code == 401
    response = client.post(base + "/issue", json={"csrf_token": "fixture-csrf"}, headers=headers)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["submitted_to_agency"] is False
    fp = result["package_fingerprint"]
    xml = client.get(base + "/packages/" + fp + "/lawgs_xml", headers=headers)
    assert xml.status_code == 200
    assert xml.headers["X-Assurance-Status"] == "FILING_READY"
    assert b"merchandiseList" in xml.content
    xlsx = client.get(base + "/packages/" + fp + "/lacey_excel", headers=headers)
    assert xlsx.status_code == 200
    assert client.get(base + "/packages/" + fp + "/lacey_excel", headers=headers).content == xlsx.content
    ppq = client.get(base + "/packages/" + fp + "/ppq505_preparation_json", headers=headers)
    assert ppq.status_code == 200
    assert ppq.json()["schema_version"] == "ppq505-preparation-v2"
    manifest = client.get(base + "/packages/" + fp + "/manifest", headers=headers)
    assert manifest.status_code == 200
    assert manifest.json()["case"]["documents"][0]["sha256"] == "a" * 64
    assert client.get(base + "/packages/" + fp + "/lawgs_xml",
                      headers={"X-Demo-Tenant": "12"}).status_code == 404


def test_blocked_final_output_provides_only_watermarked_draft():
    case = authorize(replace(make_case(), exceptions=({
        "id": "ex-pending", "field_name": "species", "status": "OPEN",
        "blocking": True, "line_reference": "LINE-1",
    },)))
    client, _ = make_client(case)
    base = f"/assurance-v2/operations/{OPERATION_ID}"
    headers = {"X-Demo-Tenant": "11"}
    response = client.post(base + "/issue", json={"csrf_token": "fixture-csrf"}, headers=headers)
    assert response.status_code == 409
    assert response.json()["status"] == "BLOCKED"
    draft = client.get(base + "/draft.xlsx", headers=headers)
    assert draft.status_code == 200
    assert draft.headers["X-Assurance-Status"] == "DRAFT"
    assert "INCOMPLETE_DRAFT" in draft.headers["content-disposition"]



def test_production_router_cannot_issue_ready_case_without_atomic_gateway():
    client, _ = make_client(test_issue_enabled=False)
    response = client.post(
        f"/assurance-v2/operations/{OPERATION_ID}/issue",
        json={"csrf_token": "fixture-csrf"},
        headers={"X-Demo-Tenant": "11"},
    )
    assert response.status_code == 503
    assert "Atomic Assurance V2 issuance is not enabled" in response.text



def test_export_authorization_requires_authenticated_explicit_human_action():
    case = make_case(approved=False)
    client, _ = make_client(case, test_authorization_enabled=True)
    base = f"/assurance-v2/operations/{OPERATION_ID}"
    headers = {"X-Demo-Tenant": "11"}
    html = client.get(base, headers=headers)
    assert html.status_code == 200
    assert 'id="authorize-export-form"' in html.text
    assert 'name="explicit_confirmation"' in html.text
    assert 'name="reason"' in html.text
    assert client.post(base + "/authorize-export", json={
        "csrf_token": "fixture-csrf", "reason": "Reviewed source documents",
        "explicit_confirmation": True,
    }).status_code == 401
    assert client.post(base + "/authorize-export", json={
        "csrf_token": "wrong", "reason": "Reviewed source documents",
        "explicit_confirmation": True,
    }, headers=headers).status_code == 403
    assert client.post(base + "/authorize-export", json={
        "csrf_token": "fixture-csrf", "reason": "Reviewed source documents",
    }, headers=headers).status_code == 422
    assert client.post(base + "/authorize-export", json={
        "csrf_token": "fixture-csrf", "reason": "",
        "explicit_confirmation": True,
    }, headers=headers).status_code == 422
    answer = client.post(base + "/authorize-export", json={
        "csrf_token": "fixture-csrf", "reason": "Reviewed source documents",
        "explicit_confirmation": True,
    }, headers=headers)
    assert answer.status_code == 200
    assert answer.json()["status"] == "AUTHORIZED"
    updated = client.get(base, headers=headers)
    assert updated.status_code == 200
    assert 'id="issue-package"' in updated.text
    assert 'id="authorize-export-form"' not in updated.text


def test_no_explicit_authorization_gateway_never_grants_export():
    client, _ = make_client(make_case(approved=False))
    headers = {"X-Demo-Tenant": "11"}
    resp = client.post(f"/assurance-v2/operations/{OPERATION_ID}/authorize-export",
                       json={"csrf_token": "fixture-csrf",
                             "reason": "Review complete", "explicit_confirmation": True},
                       headers=headers)
    assert resp.status_code == 503
    assert "not enabled" in resp.text


def test_authorization_gateway_cannot_override_missing_required_review():
    case = make_case(approved=False)
    fields = list(case.fields)
    fields[0] = {**fields[0], "authority": "CANDIDATE"}
    client, _ = make_client(
        replace(case, fields=tuple(fields)), test_authorization_enabled=True,
    )
    resp = client.post(f"/assurance-v2/operations/{OPERATION_ID}/authorize-export",
                       json={"csrf_token": "fixture-csrf",
                             "reason": "Review complete", "explicit_confirmation": True},
                       headers={"X-Demo-Tenant": "11"})
    assert resp.status_code == 409
    assert any(b["code"] == "UNSUPPORTED_CLAIM" for b in resp.json()["blockers"])
