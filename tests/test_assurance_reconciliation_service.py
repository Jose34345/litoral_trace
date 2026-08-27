from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from litoral_trace.assurance.domain import (
    AssuranceDocumentType,
    DocumentProcessingStatus,
    ExtractionRunStatus,
    ReconciliationIssueStatus,
)
from litoral_trace.assurance.reconciliation import (
    RULE_DOCUMENT_OPERATION_QUANTITY,
    RULE_INVOICE_DELIVERY_QUANTITY,
)
from litoral_trace.assurance.reconciliation_service import (
    AssuranceReconciliationService,
    _canonical_operation_target,
)
from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentEntityLink,
    DocumentExtractionRun,
    ExtractedDocumentField,
    ReconciliationIssue,
    Shipment,
    ShipmentItem,
    TraceabilityBatch,
    VaultDocument,
)


def _session_factory() -> tuple[sessionmaker[Session], Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    tables = (
        VaultDocument.__table__,
        AssuranceDocument.__table__,
        DocumentExtractionRun.__table__,
        ExtractedDocumentField.__table__,
        DocumentEntityLink.__table__,
        ReconciliationIssue.__table__,
        TraceabilityBatch.__table__,
        Shipment.__table__,
        ShipmentItem.__table__,
    )
    for table in tables:
        table.create(engine, checkfirst=True)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    return factory, factory()


def _vault(*, organization_id: int, filename: str, suffix: str) -> VaultDocument:
    return VaultDocument(
        organization_id=organization_id,
        original_filename=filename,
        content_type="application/pdf",
        size_bytes=100,
        sha256=(suffix * 64)[:64],
        object_key=f"assurance/{suffix}/{filename}",
        storage_backend="s3",
        storage_bucket="test-private",
        document_type="OTHER_EVIDENCE",
        status="available",
    )


def _accepted_field(
    *,
    organization_id: int,
    document_id: int,
    run_id: int,
    field_name: str,
    value: str,
    auto_accepted: bool = True,
) -> ExtractedDocumentField:
    return ExtractedDocumentField(
        organization_id=organization_id,
        assurance_document_id=document_id,
        extraction_run_id=run_id,
        field_name=field_name,
        original_value=value,
        normalized_value=value,
        value_type="number" if field_name == "quantity" else "text",
        confidence=0.98 if auto_accepted else 0.70,
        confidence_level="HIGH" if auto_accepted else "MEDIUM",
        source_page=1,
        source_locator=f"page:1;field:{field_name}",
        auto_accepted=auto_accepted,
        needs_review=not auto_accepted,
    )


def test_order_and_shipment_links_share_one_canonical_operation():
    shipment_id = uuid4()
    shipment_target = _canonical_operation_target("SHIPMENT", f"shipment:{shipment_id}")
    order_target = _canonical_operation_target("ORDER", f"order:{shipment_id}")

    assert shipment_target is not None
    assert order_target is not None
    assert shipment_target == order_target
    assert shipment_target[0] == f"shipment:{shipment_id}"


def test_reconciliation_persists_idempotently_and_resolves_after_source_correction():
    factory, seed = _session_factory()
    org_id = 42
    shipment_public_id = uuid4()

    batch = TraceabilityBatch(
        organization_id=org_id,
        code="LOT-442",
        product_name="Madera aserrada de pino",
        stage="FINISHED_GOOD",
        unit="M3",
        status="ACTIVE",
    )
    shipment = Shipment(
        public_id=shipment_public_id,
        organization_id=org_id,
        shipment_code="SHIP-EU-2841",
        sale_reference="EU-2841",
        destination_country="DE",
        shipped_at=datetime(2026, 9, 15, tzinfo=timezone.utc),
        status="DRAFT",
    )
    seed.add_all([batch, shipment])
    seed.flush()
    seed.add(
        ShipmentItem(
            organization_id=org_id,
            shipment_id=shipment.id,
            batch_id=batch.id,
            quantity=80,
            unit="M3",
        )
    )

    invoice_vault = _vault(
        organization_id=org_id,
        filename="factura_e_EU2841.pdf",
        suffix="a",
    )
    delivery_vault = _vault(
        organization_id=org_id,
        filename="remito_EU2841.pdf",
        suffix="b",
    )
    seed.add_all([invoice_vault, delivery_vault])
    seed.flush()

    invoice = AssuranceDocument(
        organization_id=org_id,
        vault_document_id=invoice_vault.id,
        semantic_document_type=AssuranceDocumentType.INVOICE.value,
        type_confidence=0.99,
        processing_status=DocumentProcessingStatus.EXTRACTED.value,
    )
    delivery = AssuranceDocument(
        organization_id=org_id,
        vault_document_id=delivery_vault.id,
        semantic_document_type=AssuranceDocumentType.DELIVERY_NOTE.value,
        type_confidence=0.99,
        processing_status=DocumentProcessingStatus.EXTRACTED.value,
    )
    seed.add_all([invoice, delivery])
    seed.flush()

    invoice_run = DocumentExtractionRun(
        organization_id=org_id,
        assurance_document_id=invoice.id,
        engine="test",
        engine_version="1",
        status=ExtractionRunStatus.SUCCEEDED.value,
    )
    delivery_run = DocumentExtractionRun(
        organization_id=org_id,
        assurance_document_id=delivery.id,
        engine="test",
        engine_version="1",
        status=ExtractionRunStatus.SUCCEEDED.value,
    )
    seed.add_all([invoice_run, delivery_run])
    seed.flush()

    invoice_quantity = _accepted_field(
        organization_id=org_id,
        document_id=invoice.id,
        run_id=invoice_run.id,
        field_name="quantity",
        value="80",
    )
    delivery_quantity = _accepted_field(
        organization_id=org_id,
        document_id=delivery.id,
        run_id=delivery_run.id,
        field_name="quantity",
        value="75",
    )
    seed.add_all(
        [
            invoice_quantity,
            delivery_quantity,
            _accepted_field(
                organization_id=org_id,
                document_id=invoice.id,
                run_id=invoice_run.id,
                field_name="destination",
                value="DE",
            ),
            _accepted_field(
                organization_id=org_id,
                document_id=delivery.id,
                run_id=delivery_run.id,
                field_name="destination",
                value="DE",
            ),
        ]
    )
    # One document matches the shipment identifier and the other the order
    # identifier. They must still reconcile as the same commercial operation.
    seed.add_all(
        [
            DocumentEntityLink(
                organization_id=org_id,
                assurance_document_id=invoice.id,
                entity_type="SHIPMENT",
                entity_reference=f"shipment:{shipment_public_id}",
                link_confidence=0.99,
                link_method="EXACT_IDENTIFIER",
                human_confirmed=False,
            ),
            DocumentEntityLink(
                organization_id=org_id,
                assurance_document_id=delivery.id,
                entity_type="ORDER",
                entity_reference=f"order:{shipment_public_id}",
                link_confidence=0.99,
                link_method="EXACT_IDENTIFIER",
                human_confirmed=False,
            ),
        ]
    )
    seed.commit()

    service = AssuranceReconciliationService(session_factory=factory)
    first = service.reconcile_document(
        organization_id=org_id,
        assurance_public_id=invoice.public_id,
    )
    assert first.operation_count == 1
    assert first.created_count >= 2

    inspect = factory()
    issues = inspect.scalars(
        select(ReconciliationIssue)
        .where(ReconciliationIssue.organization_id == org_id)
        .order_by(ReconciliationIssue.id.asc())
    ).all()
    rule_codes = {issue.rule_code for issue in issues}
    assert RULE_INVOICE_DELIVERY_QUANTITY in rule_codes
    assert RULE_DOCUMENT_OPERATION_QUANTITY in rule_codes
    assert all(issue.operation_reference == f"shipment:{shipment_public_id}" for issue in issues)
    quantity_issue = next(
        issue for issue in issues if issue.rule_code == RULE_INVOICE_DELIVERY_QUANTITY
    )
    assert "factura_e_EU2841.pdf" in quantity_issue.left_source
    assert "remito_EU2841.pdf" in (quantity_issue.right_source or "")
    assert quantity_issue.evidence_json and len(quantity_issue.evidence_json["sources"]) == 2
    initial_issue_count = len(issues)
    inspect.close()

    second = service.reconcile_document(
        organization_id=org_id,
        assurance_public_id=delivery.public_id,
    )
    assert second.created_count == 0
    assert second.refreshed_count == initial_issue_count

    correct = factory()
    persisted_delivery_quantity = correct.scalar(
        select(ExtractedDocumentField).where(
            ExtractedDocumentField.organization_id == org_id,
            ExtractedDocumentField.id == delivery_quantity.id,
        )
    )
    assert persisted_delivery_quantity is not None
    persisted_delivery_quantity.original_value = "80"
    persisted_delivery_quantity.normalized_value = "80"
    correct.commit()
    correct.close()

    third = service.reconcile_document(
        organization_id=org_id,
        assurance_public_id=delivery.public_id,
    )
    assert third.created_count == 0
    assert third.auto_resolved_count >= 2

    final_session = factory()
    final_issues = final_session.scalars(
        select(ReconciliationIssue).where(ReconciliationIssue.organization_id == org_id)
    ).all()
    assert len(final_issues) == initial_issue_count
    assert all(issue.status == ReconciliationIssueStatus.RESOLVED.value for issue in final_issues)
    assert all(issue.resolved_at is not None for issue in final_issues)
    final_session.close()


def test_fields_pending_human_review_do_not_create_automatic_quantity_blocking():
    factory, seed = _session_factory()
    org_id = 7
    shipment_public_id = uuid4()

    shipment = Shipment(
        public_id=shipment_public_id,
        organization_id=org_id,
        shipment_code="SHIP-REVIEW",
        destination_country="DE",
        status="DRAFT",
    )
    seed.add(shipment)
    seed.flush()

    vault = _vault(organization_id=org_id, filename="remito_review.pdf", suffix="c")
    seed.add(vault)
    seed.flush()
    document = AssuranceDocument(
        organization_id=org_id,
        vault_document_id=vault.id,
        semantic_document_type=AssuranceDocumentType.DELIVERY_NOTE.value,
        type_confidence=0.90,
        processing_status=DocumentProcessingStatus.NEEDS_REVIEW.value,
    )
    seed.add(document)
    seed.flush()
    run = DocumentExtractionRun(
        organization_id=org_id,
        assurance_document_id=document.id,
        engine="test",
        engine_version="1",
        status=ExtractionRunStatus.NEEDS_REVIEW.value,
    )
    seed.add(run)
    seed.flush()
    seed.add(
        _accepted_field(
            organization_id=org_id,
            document_id=document.id,
            run_id=run.id,
            field_name="quantity",
            value="999",
            auto_accepted=False,
        )
    )
    seed.add(
        DocumentEntityLink(
            organization_id=org_id,
            assurance_document_id=document.id,
            entity_type="SHIPMENT",
            entity_reference=f"shipment:{shipment_public_id}",
            link_confidence=0.99,
            link_method="EXACT_IDENTIFIER",
            human_confirmed=False,
        )
    )
    seed.commit()

    result = AssuranceReconciliationService(session_factory=factory).reconcile_document(
        organization_id=org_id,
        assurance_public_id=document.public_id,
    )
    assert result.finding_count == 0
    assert result.created_count == 0
