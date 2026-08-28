from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from decimal import Decimal
import hashlib

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from litoral_trace.assurance.domain import ReconciliationSeverity
from litoral_trace.assurance.ingestion import AssuranceIngestionService
from litoral_trace.assurance.market_ready_inventory import (
    InventoryStockCandidate,
    MarketReadyTarget,
    build_market_ready_matrix,
)
from litoral_trace.assurance.metrics_service import AssuranceMetricsService
from litoral_trace.assurance.operational_exceptions import AssuranceOperationalExceptionService
from litoral_trace.assurance.preflight import (
    PreflightDocument,
    PreflightInput,
    PreflightSignalState,
    PreflightStatus,
)
from litoral_trace.assurance.preflight_service import AssurancePreflightService
from litoral_trace.assurance.processing import AssuranceProcessingService
from litoral_trace.assurance.reconciliation import ReconciliationFinding
from litoral_trace.assurance.reconciliation_service import AssuranceReconciliationService
from litoral_trace.assurance.suppliers import AssuranceSupplierService
from litoral_trace.config.settings import StorageSettings
from litoral_trace.db.models import (
    AssuranceDocument,
    AssuranceSupplier,
    AuditLog,
    DocumentEntityLink,
    DocumentExtractionRun,
    ExtractedDocumentField,
    Lote,
    OperationalException,
    Organization,
    ReconciliationIssue,
    Shipment,
    ShipmentItem,
    TraceabilityBatch,
    VaultDocument,
)
from litoral_trace.storage import ObjectDeleteResult, ObjectHead, ObjectWriteResult


class _FakeObjectStorage:
    def __init__(self) -> None:
        self.objects: dict[str, dict[str, object]] = {}

    def put_object(self, *, key, body, content_type, content_length, metadata=None):
        payload = body if isinstance(body, bytes) else body.read()
        assert len(payload) == content_length
        self.objects[key] = {
            "body": payload,
            "content_type": content_type,
            "metadata": dict(metadata or {}),
        }
        return ObjectWriteResult(etag=hashlib.sha256(payload).hexdigest(), version_id=None)

    def delete_object(self, *, key, version_id=None):
        self.objects.pop(key, None)
        return ObjectDeleteResult(delete_marker=False, version_id=version_id)

    def head_object(self, *, key, version_id=None):
        item = self.objects[key]
        return ObjectHead(
            size_bytes=len(item["body"]),
            content_type=str(item["content_type"]),
            etag="fixture",
            version_id=version_id,
            metadata=dict(item["metadata"]),
        )

    def object_exists(self, *, key, version_id=None):
        del version_id
        return key in self.objects

    def health_check(self):
        return True

    def get_object_stream(self, *, key, version_id=None):
        raise NotImplementedError


class _VerifiedDownload:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def iter_chunks(self, *, chunk_size: int):
        for start in range(0, len(self._payload), chunk_size):
            yield self._payload[start : start + chunk_size]


class _FakeVaultService:
    def __init__(self, factory, storage: _FakeObjectStorage) -> None:
        self._factory = factory
        self._storage = storage

    @contextmanager
    def materialize_verified_download(self, *, organization_id: int, document_id):
        session: Session = self._factory()
        try:
            row = session.scalar(
                select(VaultDocument).where(
                    VaultDocument.organization_id == organization_id,
                    VaultDocument.public_id == document_id,
                )
            )
            assert row is not None
            payload = bytes(self._storage.objects[row.object_key]["body"])
            assert hashlib.sha256(payload).hexdigest() == row.sha256
            yield _VerifiedDownload(payload)
        finally:
            session.close()


def _database():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    for table in (
        Organization.__table__,
        Lote.__table__,
        TraceabilityBatch.__table__,
        Shipment.__table__,
        ShipmentItem.__table__,
        VaultDocument.__table__,
        AssuranceDocument.__table__,
        DocumentExtractionRun.__table__,
        ExtractedDocumentField.__table__,
        DocumentEntityLink.__table__,
        AssuranceSupplier.__table__,
        ReconciliationIssue.__table__,
        OperationalException.__table__,
        AuditLog.__table__,
    ):
        table.create(engine, checkfirst=True)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    return engine, factory


def _csv(*, quantity: str, document_number: str) -> bytes:
    return (
        "Numero;Fecha;CUIT proveedor;Proveedor;Producto;Cantidad;Unidad;Lote;"
        "Codigo despacho;Destino\n"
        f"{document_number};28/08/2026;30-70832310-8;Forestal Norte S.A.;"
        f"Madera aserrada de pino;{quantity};M3;LOT-E2E-001;SHIP-E2E-001;BR\n"
    ).encode("utf-8")


def _blocking_finding(issue: ReconciliationIssue) -> ReconciliationFinding:
    return ReconciliationFinding(
        rule_code=issue.rule_code,
        severity=ReconciliationSeverity.BLOCKING,
        field_name=issue.field_name,
        left_source=issue.left_source,
        left_value=issue.left_value,
        right_source=issue.right_source,
        right_value=issue.right_value,
        explanation=issue.explanation,
        evidence=(),
        fingerprint=issue.fingerprint,
        left_document_id=issue.left_document_id,
        right_document_id=issue.right_document_id,
        delta_numeric=issue.delta_numeric,
    )


def test_files_to_supplier_exception_preflight_matrix_and_metrics_e2e():
    engine, factory = _database()
    org_id: int
    shipment_public_id: str
    with factory() as seed:
        org = Organization(name="E2E Assurance", slug="e2e-assurance", tier="pro", is_active=True)
        seed.add(org)
        seed.flush()
        org_id = org.id
        lote = Lote(
            organization_id=org_id,
            identificador="LOT-E2E-001",
            productor_id="30708323108",
            producto_forestal="Madera aserrada de pino",
            hectareas=10,
            latitud=-27.45,
            longitud=-58.98,
            estatus="Activo",
            volumen_ingresado_ton=100,
            volumen_exportar_ton=100,
        )
        shipment = Shipment(
            organization_id=org_id,
            shipment_code="SHIP-E2E-001",
            sale_reference="SALE-E2E-001",
            buyer_reference="BUYER-BR-001",
            destination_country="BR",
            status="DRAFT",
        )
        seed.add_all([lote, shipment])
        seed.commit()
        shipment_public_id = str(shipment.public_id)

    storage = _FakeObjectStorage()
    storage_settings = StorageSettings(
        backend="s3",
        bucket_name="assurance-e2e-private",
        max_upload_bytes=5 * 1024 * 1024,
    )
    ingestion = AssuranceIngestionService(
        storage_settings=storage_settings,
        storage=storage,
        session_factory=factory,
    )
    invoice = ingestion.ingest(
        organization_id=org_id,
        created_by_user_id=None,
        filename="factura_operacion.csv",
        content_type="text/csv",
        content=_csv(quantity="100", document_number="FAC-0001"),
    )
    delivery = ingestion.ingest(
        organization_id=org_id,
        created_by_user_id=None,
        filename="remito_operacion.csv",
        content_type="text/csv",
        content=_csv(quantity="90", document_number="REM-0001"),
    )
    assert invoice.duplicate is False
    assert delivery.duplicate is False

    processing = AssuranceProcessingService(
        session_factory=factory,
        vault_service=_FakeVaultService(factory, storage),
    )
    supplier_service = AssuranceSupplierService(session_factory=factory)
    reconciliation = AssuranceReconciliationService(session_factory=factory)

    # 138–140: real file bytes are parsed, linked to known lot/shipment, and the
    # supplier identity is created/reused without a manual supplier form.
    for result in (invoice, delivery):
        assert processing.process(
            organization_id=org_id,
            assurance_public_id=result.assurance_public_id,
        ) == "EXTRACTED"
        supplier_service.resolve_document(
            organization_id=org_id,
            assurance_public_id=result.assurance_public_id,
        )
        reconciliation.reconcile_document(
            organization_id=org_id,
            assurance_public_id=result.assurance_public_id,
        )

    operation_reference = f"shipment:{shipment_public_id}"
    with factory() as inspect:
        suppliers = inspect.scalars(select(AssuranceSupplier)).all()
        assert len(suppliers) == 1
        assert suppliers[0].cuit == "30708323108"
        links = inspect.scalars(select(DocumentEntityLink)).all()
        by_document: dict[int, set[str]] = {}
        for link in links:
            by_document.setdefault(link.assurance_document_id, set()).add(link.entity_type)
        assert len(by_document) == 2
        assert all({"LOT", "SHIPMENT", "SUPPLIER"}.issubset(types) for types in by_document.values())

        open_issues = inspect.scalars(
            select(ReconciliationIssue).where(
                ReconciliationIssue.organization_id == org_id,
                ReconciliationIssue.operation_reference == operation_reference,
                ReconciliationIssue.status == "OPEN",
            )
        ).all()
        assert open_issues
        assert any(issue.rule_code == "INVOICE_DELIVERY_QUANTITY_MISMATCH" for issue in open_issues)
        blocking_findings = tuple(_blocking_finding(issue) for issue in open_issues)

    # 141–144: the intentional 100 vs 90 contradiction becomes an actionable
    # exception and blocks the persisted-operation Preflight.
    exceptions = AssuranceOperationalExceptionService(session_factory=factory)
    sync = exceptions.sync_reconciliation(organization_id=org_id)
    assert sync.created_count == len(open_issues)
    attention = exceptions.list_attention(
        organization_id=org_id,
        operation_reference=operation_reference,
    )
    assert attention
    assert all(row.recommended_action for row in attention)
    assert all(row.impact == "BLOCKING" for row in attention)

    payload = PreflightInput(
        customer_reference="BUYER-BR-001",
        market="BR",
        product="Madera aserrada de pino",
        quantity=Decimal("100"),
        commitment_date=date(2026, 9, 30),
        stock_available=Decimal("100"),
        documents=(
            PreflightDocument(document_type="INVOICE", reference="factura_operacion.csv"),
            PreflightDocument(document_type="DELIVERY_NOTE", reference="remito_operacion.csv"),
        ),
        required_document_types=("INVOICE", "DELIVERY_NOTE"),
        origin_state=PreflightSignalState.READY,
        genealogy_state=PreflightSignalState.READY,
        phytosanitary_state=PreflightSignalState.READY,
        eudr_state=PreflightSignalState.NOT_APPLICABLE,
    )
    preflight_service = AssurancePreflightService(session_factory=factory)
    blocked = preflight_service.evaluate(
        organization_id=org_id,
        operation_reference=operation_reference,
        payload=payload,
    )
    assert blocked.result.status == PreflightStatus.BLOCKED
    assert "RECONCILIATION_BLOCKING" in blocked.result.reason_codes

    target = MarketReadyTarget(
        reference="TARGET-BR-001",
        customer_reference="BUYER-BR-001",
        market="BR",
        product="Madera aserrada de pino",
        requested_quantity=Decimal("100"),
        unit="M3",
        commitment_date=date(2026, 9, 30),
        required_document_types=("INVOICE", "DELIVERY_NOTE"),
        phytosanitary_state=PreflightSignalState.READY,
        eudr_state=PreflightSignalState.NOT_APPLICABLE,
    )
    matrix_before = build_market_ready_matrix(
        stocks=(
            InventoryStockCandidate(
                reference="LOT-E2E-001",
                product="Madera aserrada de pino",
                available=Decimal("100"),
                unit="M3",
                documents=payload.documents,
                origin_state=PreflightSignalState.READY,
                genealogy_state=PreflightSignalState.READY,
                reconciliation_findings=blocking_findings,
            ),
        ),
        targets=(target,),
    )
    assert matrix_before.cells[0].status == PreflightStatus.BLOCKED

    # 145–146: resolving every reconciliation exception with an explicit human
    # justification recomputes Preflight on the final resolution; the same stock
    # projection then moves from BLOCKED to READY.
    final_resolution = None
    reconciliation_attention = [
        row for row in attention if row.source_type == "RECONCILIATION"
    ]
    for index, row in enumerate(reconciliation_attention):
        final_resolution = exceptions.resolve(
            organization_id=org_id,
            exception_public_id=row.public_id,
            resolved_by_user_id=None,
            resolution_note="Diferencia revisada y aceptada para la prueba integral.",
            preflight_payload=(payload if index == len(reconciliation_attention) - 1 else None),
        )
    assert final_resolution is not None
    assert final_resolution.preflight is not None
    assert final_resolution.preflight.result.status == PreflightStatus.READY

    matrix_after = build_market_ready_matrix(
        stocks=(
            InventoryStockCandidate(
                reference="LOT-E2E-001",
                product="Madera aserrada de pino",
                available=Decimal("100"),
                unit="M3",
                documents=payload.documents,
                origin_state=PreflightSignalState.READY,
                genealogy_state=PreflightSignalState.READY,
                reconciliation_findings=(),
            ),
        ),
        targets=(target,),
    )
    assert matrix_after.cells[0].status == PreflightStatus.READY
    assert matrix_after.totals[0].ready_quantity == Decimal("100")

    # 147: the actual extraction runs, not a hard-coded fixture percentage,
    # determine how much data entered without manual field loading.
    metrics = AssuranceMetricsService(session_factory=factory).snapshot(
        organization_id=org_id
    )
    assert metrics.metrics.fields_detected > 0
    assert metrics.metrics.fields_manually_reviewed == 0
    assert metrics.metrics.fields_manually_changed == 0
    assert metrics.metrics.automatic_data_percentage >= 70.0
    assert metrics.zero_friction_target_met is True

    engine.dispose()
