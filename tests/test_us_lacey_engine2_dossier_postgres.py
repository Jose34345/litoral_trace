"""PostgreSQL acceptance for the read-only Engine 2 dossier selector."""
from __future__ import annotations

from copy import deepcopy
import pytest

from sqlalchemy import select
from litoral_trace.db.models import UsLaceyEngineDocumentRun, UsLaceyEngineShipmentRun, UsLaceyOperation, UsLaceyOperationDocument
from litoral_trace.lacey_engine.serialization import DOCUMENT_RESOLUTION_SCHEMA_VERSION, serialize_document_resolution
from litoral_trace.lacey_engine.domain import DocumentType
from litoral_trace.us_lacey import lacey_engine_dossier as dossier_module
from litoral_trace.us_lacey import lacey_engine_service as service_module
from litoral_trace.us_lacey.lacey_engine_dossier import Engine2DossierAvailability, UsLaceyEngineDossierService
from litoral_trace.us_lacey.lacey_engine_service import UsLaceyEngine2Service
from tests.test_us_lacey_engine2_persistence import _dossier
from tests.test_us_lacey_engine2_persistence import _resolution
from tests.us_lacey_engine2_postgres import FakeVault, add_test_document, create_test_graph, engine2_postgres_engine, engine2_postgres_session_factory, tenant_session


def _current(factory, monkeypatch):
    org, operation, _, _, resolutions = _dossier(factory)
    monkeypatch.setattr(service_module, "process_document", lambda **values: resolutions[values["filename"]])
    result = UsLaceyEngine2Service(session_factory=factory, vault_service=FakeVault(b"x")).resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    session = tenant_session(factory, org); snapshot = session.query(UsLaceyEngineShipmentRun).filter_by(id=result.shipment_run_id).one(); operation_public_id = session.scalar(select(UsLaceyOperation.public_id).where(UsLaceyOperation.id == operation)); session.expunge(snapshot); session.close()
    return org, operation, operation_public_id, snapshot


def _view(factory, monkeypatch, org, public_id):
    monkeypatch.setattr(dossier_module, "engine2_mode", lambda: "SHADOW")
    return UsLaceyEngineDossierService(session_factory=factory).get_dossier(organization_id=org, operation_public_id=public_id)


def _document_run(org, operation, identity, *, status, resolution=None):
    link, assurance, sha = identity
    return UsLaceyEngineDocumentRun(organization_id=org, operation_id=operation, operation_document_id=link, assurance_document_id=assurance, engine_version=service_module.ENGINE_VERSION, schema_version=DOCUMENT_RESOLUTION_SCHEMA_VERSION, source_sha256=sha, role_hint="BILL_OF_LADING" if identity is not None else "", status=status, resolution_json=serialize_document_resolution(resolution) if resolution else None, safe_error_code="TEST" if status == "FAILED" else None, safe_error_message="Test" if status == "FAILED" else None)


def test_dossier_uses_exact_current_identity_not_newest_snapshot(engine2_postgres_session_factory, monkeypatch):
    org, operation, public_id, snapshot = _current(engine2_postgres_session_factory, monkeypatch)
    session = tenant_session(engine2_postgres_session_factory, org)
    session.add(UsLaceyEngineShipmentRun(organization_id=org, operation_id=operation, engine_version=snapshot.engine_version, ruleset_version=snapshot.ruleset_version, schema_version=snapshot.schema_version, source_set_fingerprint="f" * 64, document_count=snapshot.document_count, readiness=snapshot.readiness, resolution_json=deepcopy(snapshot.resolution_json)))
    session.commit(); session.close()
    monkeypatch.setattr(dossier_module, "engine2_mode", lambda: "SHADOW")
    view = UsLaceyEngineDossierService(session_factory=engine2_postgres_session_factory).get_dossier(organization_id=org, operation_public_id=public_id)
    assert view.availability is Engine2DossierAvailability.CURRENT and view.document_count == 2


def test_dossier_marks_exact_identity_with_inconsistent_count_invalid(engine2_postgres_session_factory, monkeypatch):
    org, operation, public_id, snapshot = _current(engine2_postgres_session_factory, monkeypatch)
    session = tenant_session(engine2_postgres_session_factory, org); row = session.query(UsLaceyEngineShipmentRun).filter_by(id=snapshot.id).one(); row.document_count = 99; session.commit(); session.close()
    monkeypatch.setattr(dossier_module, "engine2_mode", lambda: "SHADOW")
    view = UsLaceyEngineDossierService(session_factory=engine2_postgres_session_factory).get_dossier(organization_id=org, operation_public_id=public_id)
    assert view.availability is Engine2DossierAvailability.INVALID and not view.fields


def test_dossier_marks_embedded_shipment_document_identity_mismatch_invalid(engine2_postgres_session_factory, monkeypatch):
    org, _, public_id, snapshot = _current(engine2_postgres_session_factory, monkeypatch)
    session = tenant_session(engine2_postgres_session_factory, org); row = session.query(UsLaceyEngineShipmentRun).filter_by(id=snapshot.id).one(); payload = deepcopy(row.resolution_json); payload["documents"][0]["document_id"] = "999999999"; row.resolution_json = payload; session.commit(); session.close()
    view = _view(engine2_postgres_session_factory, monkeypatch, org, public_id)
    assert view.availability is Engine2DossierAvailability.INVALID and not view.fields and not view.issues and view.readiness is None


def test_dossier_marks_unknown_evidence_document_invalid(engine2_postgres_session_factory, monkeypatch):
    org, _, public_id, snapshot = _current(engine2_postgres_session_factory, monkeypatch)
    session = tenant_session(engine2_postgres_session_factory, org); row = session.query(UsLaceyEngineShipmentRun).filter_by(id=snapshot.id).one(); payload = deepcopy(row.resolution_json)
    evidence = next(item for field in payload["canonical_fields"].values() for item in field["supporting_evidence"]); evidence["document_id"] = "999999999"; row.resolution_json = payload; session.commit(); session.close()
    view = _view(engine2_postgres_session_factory, monkeypatch, org, public_id)
    assert view.availability is Engine2DossierAvailability.INVALID and not view.fields and not view.issues and view.readiness is None


def test_dossier_marks_replaced_current_document_snapshot_stale(engine2_postgres_session_factory, monkeypatch):
    org, operation, public_id, snapshot = _current(engine2_postgres_session_factory, monkeypatch)
    session = tenant_session(engine2_postgres_session_factory, org)
    old_link = session.scalar(select(UsLaceyOperationDocument.id).where(UsLaceyOperationDocument.operation_id == operation, UsLaceyOperationDocument.document_role == "BILL_OF_LADING"))
    session.close()
    add_test_document(engine2_postgres_session_factory, organization_id=org, operation_id=operation, role="BILL_OF_LADING", filename="replacement.pdf", content=b"replacement", version_number=2, is_current=True)
    session = tenant_session(engine2_postgres_session_factory, org); session.query(UsLaceyOperationDocument).filter_by(id=old_link).update({"is_current": False}); session.commit(); session.close()
    monkeypatch.setattr(dossier_module, "engine2_mode", lambda: "SHADOW")
    view = UsLaceyEngineDossierService(session_factory=engine2_postgres_session_factory).get_dossier(organization_id=org, operation_public_id=public_id)
    assert view.availability is Engine2DossierAvailability.STALE and not view.fields


def test_dossier_reports_failed_only_for_unresolved_current_document(engine2_postgres_session_factory, monkeypatch):
    org, operation, link, assurance, _, sha = create_test_graph(engine2_postgres_session_factory, content=b"failed")
    session = tenant_session(engine2_postgres_session_factory, org); public_id = session.scalar(select(UsLaceyOperation.public_id).where(UsLaceyOperation.id == operation))
    session.add(UsLaceyEngineDocumentRun(organization_id=org, operation_id=operation, operation_document_id=link, assurance_document_id=assurance, engine_version=service_module.ENGINE_VERSION, schema_version=DOCUMENT_RESOLUTION_SCHEMA_VERSION, source_sha256=sha, role_hint="BILL_OF_LADING", status="FAILED", safe_error_code="TEST", safe_error_message="Test")); session.commit(); session.close()
    monkeypatch.setattr(dossier_module, "engine2_mode", lambda: "SHADOW")
    view = UsLaceyEngineDossierService(session_factory=engine2_postgres_session_factory).get_dossier(organization_id=org, operation_public_id=public_id)
    assert view.availability is Engine2DossierAvailability.FAILED and not view.fields


def test_dossier_reusable_success_neutralizes_historical_current_failure(engine2_postgres_session_factory, monkeypatch):
    org, operation, link, assurance, _, sha = create_test_graph(engine2_postgres_session_factory, content=b"retry")
    resolution = _resolution("bill.pdf", DocumentType.BILL_OF_LADING, {"bill_of_lading": "MAEU274342495"})
    session = tenant_session(engine2_postgres_session_factory, org); public_id = session.scalar(select(UsLaceyOperation.public_id).where(UsLaceyOperation.id == operation))
    for status, payload in (("FAILED", None), ("SUCCEEDED", serialize_document_resolution(resolution))): session.add(UsLaceyEngineDocumentRun(organization_id=org, operation_id=operation, operation_document_id=link, assurance_document_id=assurance, engine_version=service_module.ENGINE_VERSION, schema_version=DOCUMENT_RESOLUTION_SCHEMA_VERSION, source_sha256=sha, role_hint="BILL_OF_LADING", status=status, resolution_json=payload, safe_error_code="TEST" if status == "FAILED" else None, safe_error_message="Test" if status == "FAILED" else None))
    session.commit(); assert session.query(UsLaceyEngineDocumentRun).filter_by(assurance_document_id=assurance, status="FAILED").count() == 1 and session.query(UsLaceyEngineDocumentRun).filter_by(assurance_document_id=assurance, status="SUCCEEDED").count() == 1; session.close()
    view = _view(engine2_postgres_session_factory, monkeypatch, org, public_id)
    assert view.availability is Engine2DossierAvailability.NOT_AVAILABLE and not view.fields


def test_dossier_marks_partial_current_source_set_failed(engine2_postgres_session_factory, monkeypatch):
    org, operation, bill, supplier, resolutions = _dossier(engine2_postgres_session_factory)
    session = tenant_session(engine2_postgres_session_factory, org); public_id = session.scalar(select(UsLaceyOperation.public_id).where(UsLaceyOperation.id == operation))
    success = _document_run(org, operation, bill, status="SUCCEEDED", resolution=resolutions["bill.pdf"]); success.role_hint = "BILL_OF_LADING"
    failed = _document_run(org, operation, supplier, status="FAILED"); failed.role_hint = "SUPPLIER_DECLARATION"
    session.add_all((success, failed)); session.commit(); session.close()
    view = _view(engine2_postgres_session_factory, monkeypatch, org, public_id)
    assert view.availability is Engine2DossierAvailability.FAILED and not view.fields and not view.issues and view.readiness is None


def test_dossier_ignores_superseded_document_failure(engine2_postgres_session_factory, monkeypatch):
    org, operation, old_link, old_assurance, _, old_sha = create_test_graph(engine2_postgres_session_factory, content=b"v1")
    new_link, new_assurance, _, new_sha = add_test_document(engine2_postgres_session_factory, organization_id=org, operation_id=operation, role="BILL_OF_LADING", filename="v2.pdf", content=b"v2", version_number=2, is_current=True)
    session = tenant_session(engine2_postgres_session_factory, org); session.query(UsLaceyOperationDocument).filter_by(id=old_link).update({"is_current": False}); public_id = session.scalar(select(UsLaceyOperation.public_id).where(UsLaceyOperation.id == operation))
    session.add_all((UsLaceyEngineDocumentRun(organization_id=org, operation_id=operation, operation_document_id=old_link, assurance_document_id=old_assurance, engine_version=service_module.ENGINE_VERSION, schema_version=DOCUMENT_RESOLUTION_SCHEMA_VERSION, source_sha256=old_sha, role_hint="BILL_OF_LADING", status="FAILED", safe_error_code="TEST", safe_error_message="Test"), UsLaceyEngineDocumentRun(organization_id=org, operation_id=operation, operation_document_id=new_link, assurance_document_id=new_assurance, engine_version=service_module.ENGINE_VERSION, schema_version=DOCUMENT_RESOLUTION_SCHEMA_VERSION, source_sha256=new_sha, role_hint="BILL_OF_LADING", status="SUCCEEDED", resolution_json=serialize_document_resolution(_resolution("v2.pdf", DocumentType.BILL_OF_LADING, {"bill_of_lading": "MAEU274342495"})))))
    session.commit(); session.close()
    view = _view(engine2_postgres_session_factory, monkeypatch, org, public_id)
    assert view.availability is Engine2DossierAvailability.NOT_AVAILABLE and not view.fields


def test_dossier_current_shipment_dominates_historical_failures_regardless_of_row_order(engine2_postgres_session_factory, monkeypatch):
    org, operation, public_id, _ = _current(engine2_postgres_session_factory, monkeypatch)
    old_link, old_assurance, _, old_sha = add_test_document(engine2_postgres_session_factory, organization_id=org, operation_id=operation, role="COMMERCIAL_INVOICE", filename="old.pdf", content=b"old", version_number=1, is_current=False)
    session = tenant_session(engine2_postgres_session_factory, org); session.add(UsLaceyEngineDocumentRun(organization_id=org, operation_id=operation, operation_document_id=old_link, assurance_document_id=old_assurance, engine_version=service_module.ENGINE_VERSION, schema_version=DOCUMENT_RESOLUTION_SCHEMA_VERSION, source_sha256=old_sha, role_hint="COMMERCIAL_INVOICE", status="FAILED", safe_error_code="TEST", safe_error_message="Test")); session.commit(); session.close()
    view = _view(engine2_postgres_session_factory, monkeypatch, org, public_id)
    assert view.availability is Engine2DossierAvailability.CURRENT and view.fields and view.readiness is not None


def test_dossier_service_hides_foreign_tenant_current_snapshot(engine2_postgres_session_factory, monkeypatch):
    a_org, _, public_id, _ = _current(engine2_postgres_session_factory, monkeypatch)
    b_org, _, _, _, _, _ = create_test_graph(engine2_postgres_session_factory, content=b"tenant-b")
    monkeypatch.setattr(dossier_module, "engine2_mode", lambda: "SHADOW")
    with pytest.raises(LookupError):
        UsLaceyEngineDossierService(session_factory=engine2_postgres_session_factory).get_dossier(organization_id=b_org, operation_public_id=public_id)


@pytest.mark.parametrize("attribute, old_value", [("engine_version", "engine-old"), ("ruleset_version", "rules-old"), ("schema_version", "shipment-schema-old")])
def test_dossier_marks_version_contract_mismatches_stale(engine2_postgres_session_factory, monkeypatch, attribute, old_value):
    org, operation, public_id, snapshot = _current(engine2_postgres_session_factory, monkeypatch)
    session = tenant_session(engine2_postgres_session_factory, org); row = session.query(UsLaceyEngineShipmentRun).filter_by(id=snapshot.id).one(); setattr(row, attribute, old_value); session.commit(); session.close()
    monkeypatch.setattr(dossier_module, "engine2_mode", lambda: "SHADOW")
    view = UsLaceyEngineDossierService(session_factory=engine2_postgres_session_factory).get_dossier(organization_id=org, operation_public_id=public_id)
    assert view.availability is Engine2DossierAvailability.STALE and not view.fields and not view.issues
