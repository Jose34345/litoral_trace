"""Portal route wiring for opt-in V2 commands; persistence is tested separately."""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient

from litoral_trace.web import us_lacey_assurance_v2_runtime as runtime
from test_assurance_v2_outputs_core import OPERATION_ID, authorize, make_case


def _client(monkeypatch, *, commands: bool, case=None):
    case = case or make_case(approved=False)
    calls = []
    monkeypatch.setattr(runtime, "resolve_us_lacey_session", lambda token: SimpleNamespace(
        organization_id=case.organization_id, user_id=42, email="reviewer@example.test",
    ))
    monkeypatch.setattr(runtime, "require_us_lacey_operational_access", lambda **kw: calls.append(
        ("entitlement", kw["require_mutation_access"])
    ))
    monkeypatch.setattr(runtime, "read_tenant_assurance_case", lambda **kw: case)
    monkeypatch.setattr(runtime, "review_journal_enabled", lambda: True)
    monkeypatch.setattr(runtime, "review_us_lacey_field", lambda **kw: (
        calls.append(("review", kw)) or SimpleNamespace(field_id=1, field_status="MATCHED")
    ))
    monkeypatch.setattr(runtime, "explicitly_authorize_assurance_export", lambda **kw: (
        calls.append(("authorize", kw)) or {"status": "AUTHORIZED"}
    ))
    monkeypatch.setattr(runtime, "issue_locked_assurance_package", lambda **kw: (
        calls.append(("issue", kw)) or {"status": "FILING_READY", "submitted_to_agency": False}
    ))
    monkeypatch.setattr(runtime, "apply_authorized_assurance_v2_reuse", lambda **kw: (
        calls.append(("reuse", kw)) or {"status": "VERIFIED_REUSE"}
    ))
    app = FastAPI()
    static_dir = Path(__file__).resolve().parents[1] / "src/litoral_trace/static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    assert runtime.configure_assurance_v2_shadow(app, mode="SHADOW", enable_commands=commands)
    client = TestClient(app)
    client.cookies.set("us_lacey_session", "fixture-session")
    return client, calls, case


def test_runtime_commands_default_off_even_with_authenticated_case(monkeypatch):
    client, calls, case = _client(monkeypatch, commands=False)
    ex = {"id": "field:1", "field_name": "genus", "line_reference": "LINE-1",
          "status": "OPEN", "blocking": True, "candidates": []}
    monkeypatch.setattr(runtime, "read_tenant_assurance_case", lambda **kw: replace(
        case, exceptions=(ex,),
    ))
    csrf = runtime.us_lacey_csrf_token(
        session_token="fixture-session", purpose=f"assurance-v2:{OPERATION_ID}",
    )
    response = client.post(f"/assurance-v2/operations/{OPERATION_ID}/decisions", json={
        "csrf_token": csrf, "exception_id": "field:1", "action": "ACCEPT",
    })
    assert response.status_code == 503
    assert not any(kind == "review" for kind, _ in calls)


def test_runtime_review_uses_session_actor_and_mutation_entitlement(monkeypatch):
    case = make_case(approved=False)
    ex = {"id": "field:1", "field_name": "genus", "line_reference": "LINE-1",
          "status": "OPEN", "blocking": True,
          "candidates": [{"id": "legacy:1", "value": "Quercus"}]}
    client, calls, case = _client(monkeypatch, commands=True, case=replace(
        case, exceptions=(ex,),
    ))
    csrf = runtime.us_lacey_csrf_token(
        session_token="fixture-session", purpose=f"assurance-v2:{OPERATION_ID}",
    )
    response = client.post(f"/assurance-v2/operations/{OPERATION_ID}/decisions", json={
        "csrf_token": csrf, "exception_id": "field:1", "action": "ACCEPT",
        "candidate_id": "legacy:1",
    })
    assert response.status_code == 202, response.text
    assert ("entitlement", True) in calls
    sent = next(payload for kind, payload in calls if kind == "review")
    assert sent["organization_id"] == case.organization_id
    assert sent["user_id"] == 42
    assert sent["field_id"] == 1
    assert sent["expected_source_set_fingerprint"] == case.source_set_fingerprint
    assert sent["expected_case_fingerprint"] == case.authority_fingerprint()
    assert sent["candidate_id"] is None


def test_runtime_rejects_unsupported_review_actions_without_side_effects(monkeypatch):
    case = make_case(approved=False)
    ex = {"id": "field:1", "field_name": "genus", "line_reference": "LINE-1",
          "status": "OPEN", "blocking": True, "candidates": []}
    client, calls, _ = _client(monkeypatch, commands=True, case=replace(case, exceptions=(ex,)))
    csrf = runtime.us_lacey_csrf_token(
        session_token="fixture-session", purpose=f"assurance-v2:{OPERATION_ID}",
    )
    response = client.post(f"/assurance-v2/operations/{OPERATION_ID}/decisions", json={
        "csrf_token": csrf, "exception_id": "field:1", "action": "REJECT",
        "reason": "Unusable source",
    })
    assert response.status_code == 422
    assert not any(kind == "review" for kind, _ in calls)


def test_runtime_authorization_and_issuance_are_session_bound(monkeypatch):
    client, calls, case = _client(monkeypatch, commands=True)
    csrf = runtime.us_lacey_csrf_token(
        session_token="fixture-session", purpose=f"assurance-v2:{OPERATION_ID}",
    )
    base = f"/assurance-v2/operations/{OPERATION_ID}"
    assert client.post(base + "/authorize-export", json={
        "csrf_token": "wrong", "reason": "Reviewed", "explicit_confirmation": True,
    }).status_code == 403
    assert client.post(base + "/authorize-export", json={
        "csrf_token": csrf, "reason": "Reviewed", "explicit_confirmation": True,
    }).status_code == 200
    sent = next(payload for kind, payload in calls if kind == "authorize")
    assert sent["authenticated_user_id"] == 42
    assert sent["expected_case_fingerprint"] == case.authority_fingerprint()
    assert ("entitlement", True) in calls

    approved = authorize(case)
    monkeypatch.setattr(runtime, "read_tenant_assurance_case", lambda **kw: approved)
    issued = client.post(base + "/issue", json={"csrf_token": csrf})
    assert issued.status_code == 200, issued.text
    assert issued.json()["submitted_to_agency"] is False
    sent = next(payload for kind, payload in calls if kind == "issue")
    assert sent["authenticated_user_id"] == 42
    assert sent["expected_case_fingerprint"] == approved.authority_fingerprint()


def test_runtime_reuse_application_is_authenticated_and_off_by_default(monkeypatch):
    case = replace(make_case(approved=False), reuses=({
        "id": "memory-1", "status": "ELIGIBLE", "field_name": "species",
        "line_reference": "LINE-1", "value": "rubra",
    },))
    csrf = runtime.us_lacey_csrf_token(
        session_token="fixture-session", purpose=f"assurance-v2:{OPERATION_ID}",
    )
    command = {"csrf_token": csrf, "memory_id": "memory-1",
               "field_name": "species", "line_reference": "LINE-1",
               "reason": "I verified original source and identity"}
    base = f"/assurance-v2/operations/{OPERATION_ID}/reuses"
    disabled, calls, _ = _client(monkeypatch, commands=False, case=case)
    assert disabled.post(base, json=command).status_code == 503
    assert not any(kind == "reuse" for kind, _ in calls)
    enabled, calls, case = _client(monkeypatch, commands=True, case=case)
    response = enabled.post(base, json=command)
    assert response.status_code == 202, response.text
    sent = next(payload for kind, payload in calls if kind == "reuse")
    assert sent["organization_id"] == case.organization_id
    assert sent["authenticated_user_id"] == 42
    assert sent["expected_case_fingerprint"] == case.authority_fingerprint()


def test_runtime_source_link_downloads_only_verified_current_document(monkeypatch):
    case = make_case(approved=False)
    case = replace(case, documents=({
        "id": "7", "name": "supplier.pdf", "version": "2",
        "sha256": "a" * 64, "role": "SUPPLIER_DECLARATION",
        "raw_available": True,
    },))
    client, _, _ = _client(monkeypatch, commands=False, case=case)
    monkeypatch.setattr(runtime, "_resolve_current_source_document", lambda **kw: (
        {"vault_public_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
         "sha256": "a" * 64, "version": "2"}
        if kw["assurance_document_id"] == "7" else None
    ))

    class Verified:
        document = SimpleNamespace(
            sha256="a" * 64, content_type="application/pdf",
            size_bytes=8, filename="supplier.pdf",
        )

        def iter_chunks(self, *, chunk_size):
            yield b"%PDFtest"

        def close(self):
            pass

    monkeypatch.setattr(runtime, "VaultService", lambda: SimpleNamespace(
        materialize_verified_download=lambda **kw: Verified(),
    ))
    monkeypatch.setattr(runtime, "_audit_source_access", lambda **kw: None)
    base = f"/assurance-v2/operations/{OPERATION_ID}"
    page = client.get(base)
    assert page.status_code == 200
    assert f"{base}/documents/7" in page.text
    found = client.get(base + "/documents/7")
    assert found.status_code == 200
    assert found.content == b"%PDFtest"
    assert found.headers["Cache-Control"] == "private, no-store"
    assert client.get(base + "/documents/8").status_code == 404
