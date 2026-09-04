"""PostgreSQL RLS and composite-tenant-FK acceptance for Engine 2 tables."""
from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

from litoral_trace.db.models import UsLaceyEngineDocumentRun, UsLaceyEngineShipmentRun
from litoral_trace.lacey_engine.domain import DocumentResolution, DocumentType, ParsedLayout
from litoral_trace.lacey_engine.serialization import DOCUMENT_RESOLUTION_SCHEMA_VERSION
from litoral_trace.us_lacey import lacey_engine_service as service_module
from litoral_trace.us_lacey.lacey_engine_service import UsLaceyEngine2Service
from tests.us_lacey_engine2_postgres import FakeVault, create_test_graph, engine2_postgres_engine, engine2_postgres_session_factory, tenant_session

RUNTIME_ROLE = "litoral_trace_app"


def _resolution(filename="bill.pdf"):
    return DocumentResolution(filename, "test", DocumentType.BILL_OF_LADING, 1.0, ParsedLayout((), 1), (), {})


def _seed_snapshot(factory, monkeypatch, content=b"shared"):
    org, operation, link, assurance, _, sha = create_test_graph(factory, content=content)
    monkeypatch.setattr(service_module, "process_document", lambda **values: _resolution(values["filename"]))
    result = UsLaceyEngine2Service(session_factory=factory, vault_service=FakeVault(content)).resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    session = tenant_session(factory, org)
    document = session.query(UsLaceyEngineDocumentRun).filter_by(operation_document_id=link).one()
    shipment = session.query(UsLaceyEngineShipmentRun).filter_by(id=result.shipment_run_id).one()
    result = (org, operation, link, assurance, sha, document.id, shipment.id)
    session.close()
    return result


def _runtime_ids(engine, organization_id, table):
    """Use a fresh application-role connection; RLS must filter an unqualified SELECT."""
    with engine.begin() as connection:
        connection.execute(text(f"SET ROLE {RUNTIME_ROLE}"))
        connection.execute(text("SELECT set_config('app.current_organization_id', :org, true)"), {"org": str(organization_id)})
        return {row[0] for row in connection.execute(text(f"SELECT id FROM public.{table}"))}


def test_engine2_rls_select_hides_document_and_shipment_rows(engine2_postgres_engine, engine2_postgres_session_factory, monkeypatch):
    a = _seed_snapshot(engine2_postgres_session_factory, monkeypatch, b"tenant-a")
    b = _seed_snapshot(engine2_postgres_session_factory, monkeypatch, b"tenant-b")
    assert a[5] in _runtime_ids(engine2_postgres_engine, a[0], "us_lacey_engine_document_runs")
    assert a[6] in _runtime_ids(engine2_postgres_engine, a[0], "us_lacey_engine_shipment_runs")
    assert a[5] not in _runtime_ids(engine2_postgres_engine, b[0], "us_lacey_engine_document_runs")
    assert a[6] not in _runtime_ids(engine2_postgres_engine, b[0], "us_lacey_engine_shipment_runs")


def test_engine2_rls_rejects_cross_tenant_write(engine2_postgres_engine, engine2_postgres_session_factory, monkeypatch):
    a = _seed_snapshot(engine2_postgres_session_factory, monkeypatch, b"tenant-a")
    b = _seed_snapshot(engine2_postgres_session_factory, monkeypatch, b"tenant-b")
    with pytest.raises(DBAPIError):
        with engine2_postgres_engine.begin() as connection:
            connection.execute(text(f"SET ROLE {RUNTIME_ROLE}"))
            connection.execute(text("SELECT set_config('app.current_organization_id', :org, true)"), {"org": str(b[0])})
            connection.execute(text("""INSERT INTO public.us_lacey_engine_document_runs
                (organization_id, operation_id, operation_document_id, assurance_document_id, engine_version,
                 schema_version, source_sha256, role_hint, status)
                VALUES (:org, :operation, :link, :assurance, 'rls-write', :schema, :sha, 'BILL_OF_LADING', 'FAILED')"""),
                {"org": a[0], "operation": a[1], "link": a[2], "assurance": a[3], "schema": DOCUMENT_RESOLUTION_SCHEMA_VERSION, "sha": a[4]})
    session = tenant_session(engine2_postgres_session_factory, a[0])
    assert session.query(UsLaceyEngineDocumentRun).filter_by(id=a[5]).one().status == "SUCCEEDED"
    session.close()


def test_engine2_composite_tenant_foreign_keys_reject_cross_tenant_rows(engine2_postgres_session_factory, monkeypatch):
    a = _seed_snapshot(engine2_postgres_session_factory, monkeypatch, b"tenant-a")
    b = _seed_snapshot(engine2_postgres_session_factory, monkeypatch, b"tenant-b")
    session = tenant_session(engine2_postgres_session_factory, b[0])
    session.add(UsLaceyEngineDocumentRun(organization_id=b[0], operation_id=a[1], operation_document_id=a[2], assurance_document_id=a[3], engine_version="fk-test", schema_version=DOCUMENT_RESOLUTION_SCHEMA_VERSION, source_sha256=a[4], role_hint="BILL_OF_LADING", status="FAILED", safe_error_code="TEST", safe_error_message="Test"))
    with pytest.raises(IntegrityError): session.commit()
    session.rollback()
    session.add(UsLaceyEngineShipmentRun(organization_id=b[0], operation_id=a[1], engine_version="fk-test", ruleset_version="rules", schema_version="test", source_set_fingerprint="f" * 64, document_count=0, readiness="BLOCKED", resolution_json={"test": True}))
    with pytest.raises(IntegrityError): session.commit()
    session.rollback(); session.close()
    session = tenant_session(engine2_postgres_session_factory, b[0])
    assert session.query(UsLaceyEngineDocumentRun).filter_by(engine_version="fk-test").count() == 0
    assert session.query(UsLaceyEngineShipmentRun).filter_by(engine_version="fk-test").count() == 0
    session.close()
