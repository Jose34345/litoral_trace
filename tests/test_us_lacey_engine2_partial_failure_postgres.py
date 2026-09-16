from __future__ import annotations

from litoral_trace.db.models import UsLaceyEngineDocumentRun, UsLaceyEngineShipmentRun
from litoral_trace.lacey_engine.domain import DocumentResolution, DocumentType, ParsedLayout
from litoral_trace.lacey_engine.serialization import DOCUMENT_RESOLUTION_SCHEMA_VERSION
from litoral_trace.us_lacey import lacey_engine_service as service_module
from litoral_trace.us_lacey.lacey_engine_service import UsLaceyEngine2Service
from tests.us_lacey_engine2_postgres import (
    FakeVault,
    add_test_document,
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


def _empty_resolution(filename: str) -> DocumentResolution:
    document_type = (
        DocumentType.BILL_OF_LADING
        if filename == "bill.pdf"
        else DocumentType.OTHER
    )
    return DocumentResolution(
        filename,
        service_module.ENGINE_VERSION,
        document_type,
        1.0,
        ParsedLayout((), 1),
        (),
        {},
    )


def test_partial_failure_persists_successful_siblings_and_never_snapshots_incomplete_source_set(
    engine2_postgres_session_factory,
    monkeypatch,
):
    factory = engine2_postgres_session_factory
    org, operation, bill_link, _, _, _ = create_test_graph(factory, content=b"broken")
    invoice_link, _, _, _ = add_test_document(
        factory,
        organization_id=org,
        operation_id=operation,
        role="COMMERCIAL_INVOICE",
        filename="invoice.pdf",
        content=b"invoice",
    )
    packing_link, _, _, _ = add_test_document(
        factory,
        organization_id=org,
        operation_id=operation,
        role="PACKING_LIST",
        filename="packing-list.pdf",
        content=b"packing",
    )
    calls: list[str] = []

    def process_document(**values):
        filename = values["filename"]
        calls.append(filename)
        if filename == "bill.pdf":
            raise RuntimeError("synthetic unreadable source")
        return _empty_resolution(filename)

    monkeypatch.setattr(service_module, "process_document", process_document)
    service = UsLaceyEngine2Service(
        session_factory=factory,
        vault_service=FakeVault(b"source"),
    )

    result = service.resolve_operation_with_engine2(
        organization_id=org,
        operation_id=operation,
    )

    assert result.status == "BLOCKED_PARTIAL"
    assert result.shipment_run_id is None
    assert result.succeeded_document_count == 2
    assert result.failed_document_count == 1
    assert calls == ["bill.pdf", "invoice.pdf", "packing-list.pdf"]

    session = tenant_session(factory, org)
    runs = (
        session.query(UsLaceyEngineDocumentRun)
        .filter_by(
            organization_id=org,
            operation_id=operation,
            engine_version=service_module.ENGINE_VERSION,
            schema_version=DOCUMENT_RESOLUTION_SCHEMA_VERSION,
        )
        .all()
    )
    by_link = {row.operation_document_id: row.status for row in runs}
    assert by_link == {
        bill_link: "FAILED",
        invoice_link: "SUCCEEDED",
        packing_link: "SUCCEEDED",
    }
    assert (
        session.query(UsLaceyEngineShipmentRun)
        .filter_by(organization_id=org, operation_id=operation)
        .count()
        == 0
    )
    session.close()


def test_repeated_partial_failure_reuses_one_failed_run_without_unique_violation(
    engine2_postgres_session_factory,
    monkeypatch,
):
    factory = engine2_postgres_session_factory
    org, operation, bill_link, _, _, _ = create_test_graph(factory, content=b"broken-retry")
    invoice_link, _, _, _ = add_test_document(
        factory,
        organization_id=org,
        operation_id=operation,
        role="COMMERCIAL_INVOICE",
        filename="invoice.pdf",
        content=b"invoice-retry",
    )
    calls: list[str] = []

    def process_document(**values):
        filename = values["filename"]
        calls.append(filename)
        if filename == "bill.pdf":
            raise RuntimeError("persistent synthetic failure")
        return _empty_resolution(filename)

    monkeypatch.setattr(service_module, "process_document", process_document)
    service = UsLaceyEngine2Service(
        session_factory=factory,
        vault_service=FakeVault(b"source"),
    )

    first = service.resolve_operation_with_engine2(
        organization_id=org,
        operation_id=operation,
    )
    second = service.resolve_operation_with_engine2(
        organization_id=org,
        operation_id=operation,
    )

    assert first.status == second.status == "BLOCKED_PARTIAL"
    assert first.succeeded_document_count == second.succeeded_document_count == 1
    assert first.failed_document_count == second.failed_document_count == 1
    # The failing document is retried; the successful sibling is reused from its
    # immutable SUCCEEDED run instead of being reprocessed.
    assert calls == ["bill.pdf", "invoice.pdf", "bill.pdf"]

    session = tenant_session(factory, org)
    assert (
        session.query(UsLaceyEngineDocumentRun)
        .filter_by(
            organization_id=org,
            operation_id=operation,
            operation_document_id=bill_link,
            engine_version=service_module.ENGINE_VERSION,
            schema_version=DOCUMENT_RESOLUTION_SCHEMA_VERSION,
            status="FAILED",
        )
        .count()
        == 1
    )
    assert (
        session.query(UsLaceyEngineDocumentRun)
        .filter_by(
            organization_id=org,
            operation_id=operation,
            operation_document_id=invoice_link,
            engine_version=service_module.ENGINE_VERSION,
            schema_version=DOCUMENT_RESOLUTION_SCHEMA_VERSION,
            status="SUCCEEDED",
        )
        .count()
        == 1
    )
    assert (
        session.query(UsLaceyEngineShipmentRun)
        .filter_by(organization_id=org, operation_id=operation)
        .count()
        == 0
    )
    session.close()


def test_all_failed_source_set_remains_failed_not_partial(
    engine2_postgres_session_factory,
    monkeypatch,
):
    factory = engine2_postgres_session_factory
    org, operation, _, _, _, _ = create_test_graph(factory, content=b"all-broken")

    def process_document(**_values):
        raise RuntimeError("synthetic total source failure")

    monkeypatch.setattr(service_module, "process_document", process_document)
    service = UsLaceyEngine2Service(
        session_factory=factory,
        vault_service=FakeVault(b"source"),
    )

    result = service.resolve_operation_with_engine2(
        organization_id=org,
        operation_id=operation,
    )

    assert result.status == "FAILED"
    assert result.shipment_run_id is None
    assert result.succeeded_document_count == 0
    assert result.failed_document_count == 1

    session = tenant_session(factory, org)
    assert (
        session.query(UsLaceyEngineShipmentRun)
        .filter_by(organization_id=org, operation_id=operation)
        .count()
        == 0
    )
    session.close()
