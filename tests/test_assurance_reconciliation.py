from __future__ import annotations

from datetime import date

from litoral_trace.assurance.domain import (
    AssuranceDocumentType,
    ReconciliationSeverity,
)
from litoral_trace.assurance.reconciliation import (
    RULE_CONTRADICTORY_VERSION,
    RULE_DATE_AFTER_SHIPMENT,
    RULE_DESTINATION,
    RULE_DOCUMENT_OPERATION_QUANTITY,
    RULE_EXPIRED,
    RULE_INVOICE_DELIVERY_QUANTITY,
    RULE_LOT,
    RULE_PRODUCT,
    RULE_REQUIRED_MISSING,
    RULE_SUPPLIER,
    DocumentSnapshot,
    OperationSnapshot,
    reconcile_operation,
)


def _document(
    reference: str,
    document_type: AssuranceDocumentType,
    **fields,
) -> DocumentSnapshot:
    return DocumentSnapshot(
        reference=reference,
        document_type=document_type,
        fields=fields,
        source_locators={name: f"field:{name}" for name in fields},
    )


def _rule_codes(operation: OperationSnapshot) -> set[str]:
    return {finding.rule_code for finding in reconcile_operation(operation)}


def test_consistent_operation_has_no_reconciliation_findings():
    operation = OperationSnapshot(
        operation_reference="EU-2841",
        documents=(
            _document(
                "factura.pdf",
                AssuranceDocumentType.INVOICE,
                quantity="80.0",
                product="Madera aserrada de pino",
                issuer_cuit="30712345678",
                destination="DE",
                lot_id="LOT-442",
                document_date="2026-08-20",
            ),
            _document(
                "remito.pdf",
                AssuranceDocumentType.DELIVERY_NOTE,
                quantity="80.0",
                product="Madera aserrada de pino",
                issuer_cuit="30712345678",
                destination="DE",
                lot_id="LOT-442",
                document_date="2026-08-21",
            ),
        ),
        system_values={
            "quantity": "80.0",
            "supplier_cuit": "30712345678",
            "destination": "DE",
        },
        allocated_lots=("LOT-442",),
        shipment_date="2026-09-15",
        required_document_types=(
            AssuranceDocumentType.INVOICE,
            AssuranceDocumentType.DELIVERY_NOTE,
        ),
    )
    assert reconcile_operation(operation) == ()


def test_invoice_vs_delivery_quantity_mismatch_is_blocking_and_has_sources():
    operation = OperationSnapshot(
        operation_reference="OP-1",
        documents=(
            _document("factura.pdf", AssuranceDocumentType.INVOICE, quantity="80"),
            _document("remito.pdf", AssuranceDocumentType.DELIVERY_NOTE, quantity="75"),
        ),
    )
    finding = next(
        item
        for item in reconcile_operation(operation)
        if item.rule_code == RULE_INVOICE_DELIVERY_QUANTITY
    )
    assert finding.severity == ReconciliationSeverity.BLOCKING
    assert finding.delta_numeric is not None
    assert finding.delta_numeric == 5
    assert "factura.pdf" in finding.left_source
    assert "remito.pdf" in (finding.right_source or "")
    assert len(finding.evidence) == 2
    assert len(finding.fingerprint) == 64


def test_document_quantity_is_compared_with_operation_stock_or_shipment_quantity():
    operation = OperationSnapshot(
        operation_reference="OP-2",
        documents=(
            _document("factura.pdf", AssuranceDocumentType.INVOICE, quantity="79.5"),
        ),
        system_values={"quantity": "80"},
    )
    finding = next(
        item
        for item in reconcile_operation(operation)
        if item.rule_code == RULE_DOCUMENT_OPERATION_QUANTITY
    )
    assert finding.severity == ReconciliationSeverity.BLOCKING
    assert finding.right_source == "operation.quantity"


def test_product_mismatch_across_documents_is_warning():
    operation = OperationSnapshot(
        operation_reference="OP-3",
        documents=(
            _document("a.pdf", AssuranceDocumentType.INVOICE, product="Pino aserrado"),
            _document("b.pdf", AssuranceDocumentType.DELIVERY_NOTE, product="Eucalipto"),
        ),
    )
    finding = next(item for item in reconcile_operation(operation) if item.rule_code == RULE_PRODUCT)
    assert finding.severity == ReconciliationSeverity.WARNING


def test_supplier_mismatch_against_documents_and_system_is_blocking():
    operation = OperationSnapshot(
        operation_reference="OP-4",
        documents=(
            _document("a.pdf", AssuranceDocumentType.INVOICE, issuer_cuit="30712345678"),
            _document("b.pdf", AssuranceDocumentType.DELIVERY_NOTE, issuer_cuit="30999999999"),
        ),
        system_values={"supplier_cuit": "30712345678"},
    )
    findings = [item for item in reconcile_operation(operation) if item.rule_code == RULE_SUPPLIER]
    assert findings
    assert all(item.severity == ReconciliationSeverity.BLOCKING for item in findings)
    assert any(item.right_source == "operation.supplier_cuit" for item in findings)


def test_unallocated_document_lot_is_blocking():
    operation = OperationSnapshot(
        operation_reference="OP-5",
        documents=(
            _document("guia.pdf", AssuranceDocumentType.FOREST_GUIDE, lot_id="LOT-391"),
        ),
        allocated_lots=("LOT-442",),
    )
    finding = next(item for item in reconcile_operation(operation) if item.rule_code == RULE_LOT)
    assert finding.severity == ReconciliationSeverity.BLOCKING
    assert finding.right_value == "LOT-442"


def test_document_date_after_shipment_is_warning():
    operation = OperationSnapshot(
        operation_reference="OP-6",
        documents=(
            _document(
                "remito.pdf",
                AssuranceDocumentType.DELIVERY_NOTE,
                document_date="20/09/2026",
            ),
        ),
        shipment_date=date(2026, 9, 15),
    )
    finding = next(
        item for item in reconcile_operation(operation) if item.rule_code == RULE_DATE_AFTER_SHIPMENT
    )
    assert finding.severity == ReconciliationSeverity.WARNING


def test_destination_mismatch_with_operation_is_blocking():
    operation = OperationSnapshot(
        operation_reference="OP-7",
        documents=(
            _document("factura.pdf", AssuranceDocumentType.INVOICE, destination="BR"),
        ),
        system_values={"market": "DE"},
    )
    finding = next(
        item for item in reconcile_operation(operation) if item.rule_code == RULE_DESTINATION
    )
    assert finding.severity == ReconciliationSeverity.BLOCKING
    assert finding.right_value == "DE"


def test_expired_document_before_shipment_is_blocking():
    document = DocumentSnapshot(
        reference="certificado.pdf",
        document_type=AssuranceDocumentType.PHYTOSANITARY_CERTIFICATE,
        fields={},
        valid_until="2026-09-11",
    )
    operation = OperationSnapshot(
        operation_reference="OP-8",
        documents=(document,),
        shipment_date="2026-09-15",
    )
    finding = next(item for item in reconcile_operation(operation) if item.rule_code == RULE_EXPIRED)
    assert finding.severity == ReconciliationSeverity.BLOCKING
    assert finding.left_value == "2026-09-11"


def test_required_document_absence_is_explicit_blocking_issue():
    operation = OperationSnapshot(
        operation_reference="OP-9",
        documents=(
            _document("factura.pdf", AssuranceDocumentType.INVOICE, quantity="80"),
        ),
        required_document_types=(
            AssuranceDocumentType.INVOICE,
            AssuranceDocumentType.PHYTOSANITARY_CERTIFICATE,
        ),
    )
    finding = next(
        item for item in reconcile_operation(operation) if item.rule_code == RULE_REQUIRED_MISSING
    )
    assert finding.severity == ReconciliationSeverity.BLOCKING
    assert finding.left_value == AssuranceDocumentType.PHYTOSANITARY_CERTIFICATE.value


def test_contradictory_versions_are_detected_per_field():
    operation = OperationSnapshot(
        operation_reference="OP-10",
        documents=(
            _document(
                "factura_v1.pdf",
                AssuranceDocumentType.INVOICE,
                document_number="E-001",
                quantity="80",
            ),
            _document(
                "factura_v2.pdf",
                AssuranceDocumentType.INVOICE,
                document_number="E-001",
                quantity="75",
            ),
        ),
    )
    findings = [
        item
        for item in reconcile_operation(operation)
        if item.rule_code == RULE_CONTRADICTORY_VERSION
    ]
    assert len(findings) == 1
    assert findings[0].field_name == "quantity"
    assert findings[0].severity == ReconciliationSeverity.BLOCKING


def test_reconciliation_is_deterministic_and_fingerprints_are_stable():
    operation = OperationSnapshot(
        operation_reference="OP-11",
        documents=(
            _document("a.pdf", AssuranceDocumentType.INVOICE, quantity="80"),
            _document("b.pdf", AssuranceDocumentType.DELIVERY_NOTE, quantity="70"),
        ),
    )
    first = reconcile_operation(operation)
    second = reconcile_operation(operation)
    assert first == second
    assert [item.fingerprint for item in first] == [item.fingerprint for item in second]


def test_operation_reference_is_required():
    operation = OperationSnapshot(operation_reference="   ")
    try:
        reconcile_operation(operation)
    except ValueError as exc:
        assert "operation_reference" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected ValueError")
