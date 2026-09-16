"""Authenticated PostgreSQL acceptance for the read-only Engine 2 dossier page."""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from litoral_trace.db.models import ReconciliationIssue, UsLaceyEngineDocumentRun, UsLaceyEngineShipmentRun, UsLaceyFieldCandidate, UsLaceyOperation, UsLaceyOperationField
from litoral_trace.lacey_engine.domain import DocumentType
from litoral_trace.services.vault import VaultService
from litoral_trace.us_lacey import lacey_engine_service as engine_module
from litoral_trace.us_lacey.lacey_engine_service import UsLaceyEngine2Service
from litoral_trace.us_lacey.operations import UsLaceyOperationService
from litoral_trace.web import us_lacey_pilot_app as portal
from tests.test_us_lacey_active_portal_postgres_e2e import _activate_account, _configure, _csrf_for, _organization_id_for_email
from tests.test_us_lacey_engine2_persistence import _resolution
from tests.us_lacey_engine2_postgres import FakeVault, add_test_document, create_test_graph, engine2_postgres_engine, engine2_postgres_session_factory, tenant_session


def _account_operation(monkeypatch):
    _configure(monkeypatch); suffix = uuid4().hex[:12]; email = f"dossier-{suffix}@example.test"; password = "dossier-http-password-123"
    delivered = {}; monkeypatch.setattr(portal, "send_us_lacey_verification_email", lambda **values: delivered.update(values))
    client = TestClient(portal.app, follow_redirects=False)
    assert client.post("/signup", data={"legal_name": f"Dossier {suffix} LLC", "business_type": "IMPORTER", "admin_name": "Dossier Admin", "admin_email": email, "password": password, "accept_terms": "yes", "accept_privacy": "yes", "accept_beta": "yes"}).status_code == 201
    assert client.get(f"/verify-email?token={delivered['verification_token']}").status_code == 303
    org = _organization_id_for_email(email); _activate_account(org)
    assert client.post("/login", data={"email": email, "password": password}).status_code == 303
    page = client.get("/operations/new"); created = client.post("/operations/new", data={"csrf_token": _csrf_for(page.text, "/operations/new"), "client_reference": f"DOSSIER-{suffix}", "importer_name": "Dossier Importer", "line_references": "1"})
    assert created.status_code == 303
    public_id = created.headers["location"].rsplit("/", 1)[-1]
    operation_id = UsLaceyOperationService().get_internal_id(organization_id=org, operation_public_id=public_id)
    return client, org, operation_id, public_id


def _persist_current(factory, monkeypatch, org, operation):
    link, assurance, _, sha = add_test_document(factory, organization_id=org, operation_id=operation, role="BILL_OF_LADING", filename="dossier.pdf", content=b"dossier")
    monkeypatch.setattr(engine_module, "process_document", lambda **_: _resolution("dossier.pdf", DocumentType.BILL_OF_LADING, {"bill_of_lading": "MAEU274342495", "country_of_harvest": "CHILE"}))
    assert UsLaceyEngine2Service(session_factory=factory, vault_service=FakeVault(b"dossier")).resolve_operation_with_engine2(organization_id=org, operation_id=operation).status == "SUCCEEDED"
    return link, assurance, sha


def _state(factory, org, operation):
    session = tenant_session(factory, org)
    rows = {}
    for model in (UsLaceyEngineDocumentRun, UsLaceyEngineShipmentRun, UsLaceyOperation, UsLaceyOperationField, UsLaceyFieldCandidate, ReconciliationIssue):
        rows[model.__tablename__] = [tuple(getattr(item, column.key) for column in item.__table__.columns) for item in session.scalars(select(model).where(model.organization_id == org)).all()]
    session.close(); return rows


def test_authenticated_get_is_read_only_and_uses_persisted_dossier(engine2_postgres_session_factory, monkeypatch):
    client, org, operation, public_id = _account_operation(monkeypatch); link, assurance, _ = _persist_current(engine2_postgres_session_factory, monkeypatch, org, operation)
    session = tenant_session(engine2_postgres_session_factory, org); field = session.scalar(select(UsLaceyOperationField).where(UsLaceyOperationField.operation_id == operation)); field.human_value = "BRAZIL"; field.reviewed_at = datetime(2026, 1, 2, tzinfo=timezone.utc); user_id = session.scalar(select(UsLaceyOperation.created_by_user_id).where(UsLaceyOperation.id == operation)); field.reviewed_by_user_id = user_id
    session.add_all((UsLaceyFieldCandidate(organization_id=org, operation_id=operation, operation_field_id=field.id, source_assurance_document_id=assurance, original_value="BRAZIL", normalized_value="BRAZIL", validation_status="VALID", confidence=1, fingerprint=hashlib.sha256(b"selected").hexdigest(), decision="SELECTED", decided_by_user_id=user_id, decided_at=field.reviewed_at), UsLaceyFieldCandidate(organization_id=org, operation_id=operation, operation_field_id=field.id, source_assurance_document_id=assurance, original_value="CHILE", normalized_value="CHILE", validation_status="VALID", confidence=1, fingerprint=hashlib.sha256(b"rejected").hexdigest(), decision="REJECTED", decided_by_user_id=user_id, decided_at=field.reviewed_at), ReconciliationIssue(organization_id=org, operation_reference=str(operation), fingerprint=hashlib.sha256(b"legacy").hexdigest(), rule_code="TEST", severity="WARNING", status="RESOLVED", left_document_id=assurance, left_source="test", explanation="test", resolution_justification="preserved", resolved_at=field.reviewed_at))); session.commit(); session.close()
    before = _state(engine2_postgres_session_factory, org, operation)
    monkeypatch.setenv("US_LACEY_ENGINE2_MODE", "SHADOW")
    monkeypatch.setattr(engine_module, "process_document", lambda **_: (_ for _ in ()).throw(AssertionError("GET must not execute process_document")))
    monkeypatch.setattr(engine_module, "process_shipment", lambda **_: (_ for _ in ()).throw(AssertionError("GET must not execute process_shipment")))
    monkeypatch.setattr(UsLaceyEngine2Service, "resolve_operation_with_engine2", lambda *_, **__: (_ for _ in ()).throw(AssertionError("GET must not resolve Engine2")))
    monkeypatch.setattr(VaultService, "materialize_verified_download", lambda *_, **__: (_ for _ in ()).throw(AssertionError("GET must not read Vault bytes")))
    response = client.get(f"/operations/{public_id}")
    assert response.status_code == 200 and 'data-engine2-availability="CURRENT"' in response.text and "MAEU274342495" in response.text
    assert _state(engine2_postgres_session_factory, org, operation) == before
    session = tenant_session(engine2_postgres_session_factory, org); preserved = session.get(UsLaceyOperationField, field.id); session.close()
    assert preserved.human_value == "BRAZIL" and preserved.reviewed_at == datetime(2026, 1, 2, tzinfo=timezone.utc)
    client.close()


def test_authenticated_foreign_operation_is_not_rendered(engine2_postgres_session_factory, monkeypatch):
    a_client, a_org, a_operation, a_public = _account_operation(monkeypatch); _persist_current(engine2_postgres_session_factory, monkeypatch, a_org, a_operation)
    b_client, _, _, _ = _account_operation(monkeypatch); monkeypatch.setenv("US_LACEY_ENGINE2_MODE", "SHADOW")
    response = b_client.get(f"/operations/{a_public}")
    assert response.status_code in {403, 404} and "MAEU274342495" not in response.text and "Dossier Importer" not in response.text
    a_client.close(); b_client.close()


def test_invalid_and_disabled_dossier_gets_fail_safe_http_200(engine2_postgres_session_factory, monkeypatch):
    client, org, operation, public_id = _account_operation(monkeypatch); _persist_current(engine2_postgres_session_factory, monkeypatch, org, operation)
    session = tenant_session(engine2_postgres_session_factory, org); snapshot = session.scalar(select(UsLaceyEngineShipmentRun).where(UsLaceyEngineShipmentRun.operation_id == operation)); payload = dict(snapshot.resolution_json); payload["documents"] = []; snapshot.resolution_json = payload; session.commit(); session.close()
    monkeypatch.setenv("US_LACEY_ENGINE2_MODE", "SHADOW")
    invalid = client.get(f"/operations/{public_id}"); assert invalid.status_code == 200 and 'data-engine2-availability="INVALID"' in invalid.text and "Traceback" not in invalid.text
    monkeypatch.setenv("US_LACEY_ENGINE2_MODE", "off")
    disabled = client.get(f"/operations/{public_id}"); assert disabled.status_code == 200 and 'data-engine2-availability="DISABLED"' in disabled.text
    client.close()
