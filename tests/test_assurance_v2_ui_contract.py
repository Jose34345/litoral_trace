"""Standalone UI contract: only authority gateway owns decisions."""
from __future__ import annotations

from dataclasses import replace

from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from litoral_trace.us_lacey.exporters.assurance_v2_package import MemoryPackageStore
from litoral_trace.web.us_lacey_assurance_v2_views import (
    WorkspacePrincipal, create_assurance_v2_router,
)
from test_assurance_v2_outputs_core import OPERATION_ID, make_case, authorize


def make_client(case=None):
    case = case or make_case()
    receipts: list[dict[str, str]] = []
    store = MemoryPackageStore()
    app = FastAPI()

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

    app.include_router(create_assurance_v2_router(
        principal_for=principal,
        case_for=read,
        decision_gateway=gateway,
        csrf_verify=lambda principal, token: token == principal.csrf_token,
        package_store=store,
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
