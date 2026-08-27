"""Deterministic cross-document and operation reconciliation for Assurance v1.

The engine only compares supplied evidence. It never fabricates missing facts and
it always returns the concrete sources behind a discrepancy so downstream
preflight decisions can remain explainable and auditable.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from typing import Iterable, Mapping, Sequence

from litoral_trace.assurance.domain import (
    AssuranceDocumentType,
    ReconciliationSeverity,
)
from litoral_trace.assurance.normalization import NormalizationError, normalize_date


RULE_INVOICE_DELIVERY_QUANTITY = "INVOICE_DELIVERY_QUANTITY_MISMATCH"
RULE_DOCUMENT_OPERATION_QUANTITY = "DOCUMENT_OPERATION_QUANTITY_MISMATCH"
RULE_PRODUCT = "PRODUCT_MISMATCH"
RULE_SUPPLIER = "SUPPLIER_MISMATCH"
RULE_LOT = "LOT_IDENTIFIER_MISMATCH"
RULE_DATE_AFTER_SHIPMENT = "DOCUMENT_DATE_AFTER_SHIPMENT"
RULE_DESTINATION = "DESTINATION_MISMATCH"
RULE_EXPIRED = "DOCUMENT_EXPIRED_BEFORE_SHIPMENT"
RULE_REQUIRED_MISSING = "REQUIRED_DOCUMENT_MISSING"
RULE_CONTRADICTORY_VERSION = "CONTRADICTORY_DOCUMENT_VERSION"

_QUANTITY_TOLERANCE = Decimal("0.000001")
_BLOCKING_VERSION_FIELDS = frozenset(
    {"quantity", "issuer_cuit", "lot_id", "destination", "receiver_cuit"}
)


@dataclass(frozen=True, slots=True)
class DocumentSnapshot:
    """Accepted evidence from one document, with field-level provenance."""

    reference: str
    document_type: AssuranceDocumentType | str
    fields: Mapping[str, object] = field(default_factory=dict)
    source_locators: Mapping[str, str] = field(default_factory=dict)
    assurance_document_id: int | None = None
    valid_from: date | datetime | str | None = None
    valid_until: date | datetime | str | None = None

    @property
    def semantic_type(self) -> AssuranceDocumentType:
        if isinstance(self.document_type, AssuranceDocumentType):
            return self.document_type
        try:
            return AssuranceDocumentType(str(self.document_type))
        except ValueError:
            return AssuranceDocumentType.UNKNOWN


@dataclass(frozen=True, slots=True)
class OperationSnapshot:
    """Minimum operation facts against which documents can be reconciled."""

    operation_reference: str
    documents: tuple[DocumentSnapshot, ...] = ()
    system_values: Mapping[str, object] = field(default_factory=dict)
    required_document_types: tuple[AssuranceDocumentType | str, ...] = ()
    shipment_date: date | datetime | str | None = None
    allocated_lots: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ReconciliationFinding:
    rule_code: str
    severity: ReconciliationSeverity
    field_name: str | None
    left_source: str
    left_value: str | None
    right_source: str | None
    right_value: str | None
    explanation: str
    evidence: tuple[dict[str, object], ...]
    left_document_id: int | None = None
    right_document_id: int | None = None
    delta_numeric: Decimal | None = None
    fingerprint: str = ""


def _text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _fold(value: object) -> str:
    return _text(value).casefold()


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return None
    raw = _text(value)
    if not raw:
        return None
    try:
        return Decimal(raw)
    except InvalidOperation:
        # Extracted normalized values should already use decimal dots. This small
        # fallback only handles obvious Argentine-formatted evidence without
        # silently guessing more complex units/currencies.
        compact = raw.replace(" ", "")
        if "," in compact and "." in compact:
            if compact.rfind(",") > compact.rfind("."):
                compact = compact.replace(".", "").replace(",", ".")
            else:
                compact = compact.replace(",", "")
        elif "," in compact:
            compact = compact.replace(".", "").replace(",", ".")
        try:
            return Decimal(compact)
        except InvalidOperation:
            return None


def _date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = _text(value)
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        try:
            return normalize_date(raw)
        except NormalizationError:
            return None


def _source(document: DocumentSnapshot, field_name: str | None = None) -> str:
    if field_name:
        locator = _text(document.source_locators.get(field_name))
        if locator:
            return f"{document.reference} [{locator}]"
    return document.reference


def _evidence(
    document: DocumentSnapshot | None,
    *,
    source: str,
    field_name: str | None,
    value: object,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "source": source,
        "field_name": field_name,
        "value": None if value is None else _text(value),
    }
    if document is not None:
        payload["document_type"] = document.semantic_type.value
        payload["assurance_document_id"] = document.assurance_document_id
    return payload


def _fingerprint(
    operation_reference: str,
    *,
    rule_code: str,
    field_name: str | None,
    left_source: str,
    left_value: str | None,
    right_source: str | None,
    right_value: str | None,
) -> str:
    payload = {
        "operation_reference": operation_reference,
        "rule_code": rule_code,
        "field_name": field_name,
        "left_source": left_source,
        "left_value": left_value,
        "right_source": right_source,
        "right_value": right_value,
    }
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _finding(
    operation: OperationSnapshot,
    *,
    rule_code: str,
    severity: ReconciliationSeverity,
    field_name: str | None,
    left_source: str,
    left_value: object,
    right_source: str | None,
    right_value: object,
    explanation: str,
    evidence: Iterable[dict[str, object]],
    left_document_id: int | None = None,
    right_document_id: int | None = None,
    delta_numeric: Decimal | None = None,
) -> ReconciliationFinding:
    left = None if left_value is None else _text(left_value)
    right = None if right_value is None else _text(right_value)
    return ReconciliationFinding(
        rule_code=rule_code,
        severity=severity,
        field_name=field_name,
        left_source=left_source,
        left_value=left,
        right_source=right_source,
        right_value=right,
        explanation=explanation,
        evidence=tuple(evidence),
        left_document_id=left_document_id,
        right_document_id=right_document_id,
        delta_numeric=delta_numeric,
        fingerprint=_fingerprint(
            operation.operation_reference,
            rule_code=rule_code,
            field_name=field_name,
            left_source=left_source,
            left_value=left,
            right_source=right_source,
            right_value=right,
        ),
    )


def _documents_of_type(
    documents: Sequence[DocumentSnapshot],
    document_type: AssuranceDocumentType,
) -> tuple[DocumentSnapshot, ...]:
    return tuple(document for document in documents if document.semantic_type == document_type)


def _compare_invoice_delivery_quantity(
    operation: OperationSnapshot,
) -> list[ReconciliationFinding]:
    findings: list[ReconciliationFinding] = []
    invoices = _documents_of_type(operation.documents, AssuranceDocumentType.INVOICE)
    delivery_notes = _documents_of_type(operation.documents, AssuranceDocumentType.DELIVERY_NOTE)
    for invoice in invoices:
        left = _decimal(invoice.fields.get("quantity"))
        if left is None:
            continue
        for delivery in delivery_notes:
            right = _decimal(delivery.fields.get("quantity"))
            if right is None or abs(left - right) <= _QUANTITY_TOLERANCE:
                continue
            left_source = _source(invoice, "quantity")
            right_source = _source(delivery, "quantity")
            findings.append(
                _finding(
                    operation,
                    rule_code=RULE_INVOICE_DELIVERY_QUANTITY,
                    severity=ReconciliationSeverity.BLOCKING,
                    field_name="quantity",
                    left_source=left_source,
                    left_value=left,
                    right_source=right_source,
                    right_value=right,
                    delta_numeric=left - right,
                    explanation=(
                        "La cantidad de la factura no coincide con la cantidad del remito."
                    ),
                    evidence=(
                        _evidence(invoice, source=left_source, field_name="quantity", value=left),
                        _evidence(delivery, source=right_source, field_name="quantity", value=right),
                    ),
                    left_document_id=invoice.assurance_document_id,
                    right_document_id=delivery.assurance_document_id,
                )
            )
    return findings


def _compare_document_operation_quantity(
    operation: OperationSnapshot,
) -> list[ReconciliationFinding]:
    expected = _decimal(operation.system_values.get("quantity"))
    if expected is None:
        return []
    findings: list[ReconciliationFinding] = []
    for document in operation.documents:
        actual = _decimal(document.fields.get("quantity"))
        if actual is None or abs(actual - expected) <= _QUANTITY_TOLERANCE:
            continue
        source = _source(document, "quantity")
        findings.append(
            _finding(
                operation,
                rule_code=RULE_DOCUMENT_OPERATION_QUANTITY,
                severity=ReconciliationSeverity.BLOCKING,
                field_name="quantity",
                left_source=source,
                left_value=actual,
                right_source="operation.quantity",
                right_value=expected,
                delta_numeric=actual - expected,
                explanation=(
                    "La cantidad documentada no coincide con la cantidad registrada para la operación."
                ),
                evidence=(
                    _evidence(document, source=source, field_name="quantity", value=actual),
                    _evidence(
                        None,
                        source="operation.quantity",
                        field_name="quantity",
                        value=expected,
                    ),
                ),
                left_document_id=document.assurance_document_id,
            )
        )
    return findings


def _compare_text_field_across_documents(
    operation: OperationSnapshot,
    *,
    field_name: str,
    rule_code: str,
    severity: ReconciliationSeverity,
    explanation: str,
) -> list[ReconciliationFinding]:
    evidence_values: list[tuple[DocumentSnapshot, str]] = []
    for document in operation.documents:
        value = _text(document.fields.get(field_name))
        if value:
            evidence_values.append((document, value))
    if len({_fold(value) for _, value in evidence_values}) <= 1:
        return []

    baseline_document, baseline = evidence_values[0]
    findings: list[ReconciliationFinding] = []
    for document, value in evidence_values[1:]:
        if _fold(value) == _fold(baseline):
            continue
        left_source = _source(baseline_document, field_name)
        right_source = _source(document, field_name)
        findings.append(
            _finding(
                operation,
                rule_code=rule_code,
                severity=severity,
                field_name=field_name,
                left_source=left_source,
                left_value=baseline,
                right_source=right_source,
                right_value=value,
                explanation=explanation,
                evidence=(
                    _evidence(
                        baseline_document,
                        source=left_source,
                        field_name=field_name,
                        value=baseline,
                    ),
                    _evidence(
                        document,
                        source=right_source,
                        field_name=field_name,
                        value=value,
                    ),
                ),
                left_document_id=baseline_document.assurance_document_id,
                right_document_id=document.assurance_document_id,
            )
        )
    return findings


def _compare_supplier(operation: OperationSnapshot) -> list[ReconciliationFinding]:
    findings = _compare_text_field_across_documents(
        operation,
        field_name="issuer_cuit",
        rule_code=RULE_SUPPLIER,
        severity=ReconciliationSeverity.BLOCKING,
        explanation="Los documentos identifican proveedores/emisores distintos.",
    )
    expected = _text(operation.system_values.get("supplier_cuit"))
    if not expected:
        return findings
    for document in operation.documents:
        actual = _text(document.fields.get("issuer_cuit"))
        if not actual or _fold(actual) == _fold(expected):
            continue
        source = _source(document, "issuer_cuit")
        findings.append(
            _finding(
                operation,
                rule_code=RULE_SUPPLIER,
                severity=ReconciliationSeverity.BLOCKING,
                field_name="issuer_cuit",
                left_source=source,
                left_value=actual,
                right_source="operation.supplier_cuit",
                right_value=expected,
                explanation="El proveedor del documento no coincide con el proveedor de la operación.",
                evidence=(
                    _evidence(document, source=source, field_name="issuer_cuit", value=actual),
                    _evidence(
                        None,
                        source="operation.supplier_cuit",
                        field_name="supplier_cuit",
                        value=expected,
                    ),
                ),
                left_document_id=document.assurance_document_id,
            )
        )
    return findings


def _compare_lots(operation: OperationSnapshot) -> list[ReconciliationFinding]:
    expected_lots = {_fold(value): _text(value) for value in operation.allocated_lots if _text(value)}
    if not expected_lots:
        raw = operation.system_values.get("lot_ids")
        if isinstance(raw, (list, tuple, set)):
            expected_lots = {_fold(value): _text(value) for value in raw if _text(value)}
    if not expected_lots:
        return []

    findings: list[ReconciliationFinding] = []
    for document in operation.documents:
        actual = _text(document.fields.get("lot_id"))
        if not actual or _fold(actual) in expected_lots:
            continue
        source = _source(document, "lot_id")
        expected_display = ", ".join(sorted(expected_lots.values()))
        findings.append(
            _finding(
                operation,
                rule_code=RULE_LOT,
                severity=ReconciliationSeverity.BLOCKING,
                field_name="lot_id",
                left_source=source,
                left_value=actual,
                right_source="operation.allocated_lots",
                right_value=expected_display,
                explanation="El lote citado por el documento no está asignado a la operación.",
                evidence=(
                    _evidence(document, source=source, field_name="lot_id", value=actual),
                    _evidence(
                        None,
                        source="operation.allocated_lots",
                        field_name="lot_ids",
                        value=expected_display,
                    ),
                ),
                left_document_id=document.assurance_document_id,
            )
        )
    return findings


def _compare_dates(operation: OperationSnapshot) -> list[ReconciliationFinding]:
    shipment_date = _date(operation.shipment_date or operation.system_values.get("shipment_date"))
    if shipment_date is None:
        return []
    findings: list[ReconciliationFinding] = []
    for document in operation.documents:
        document_date = _date(document.fields.get("document_date"))
        if document_date is not None and document_date > shipment_date:
            source = _source(document, "document_date")
            findings.append(
                _finding(
                    operation,
                    rule_code=RULE_DATE_AFTER_SHIPMENT,
                    severity=ReconciliationSeverity.WARNING,
                    field_name="document_date",
                    left_source=source,
                    left_value=document_date.isoformat(),
                    right_source="operation.shipment_date",
                    right_value=shipment_date.isoformat(),
                    explanation="La fecha del documento es posterior a la fecha de despacho.",
                    evidence=(
                        _evidence(
                            document,
                            source=source,
                            field_name="document_date",
                            value=document_date.isoformat(),
                        ),
                        _evidence(
                            None,
                            source="operation.shipment_date",
                            field_name="shipment_date",
                            value=shipment_date.isoformat(),
                        ),
                    ),
                    left_document_id=document.assurance_document_id,
                )
            )

        valid_until = _date(document.valid_until or document.fields.get("valid_until"))
        if valid_until is not None and valid_until < shipment_date:
            source = _source(document, "valid_until")
            findings.append(
                _finding(
                    operation,
                    rule_code=RULE_EXPIRED,
                    severity=ReconciliationSeverity.BLOCKING,
                    field_name="valid_until",
                    left_source=source,
                    left_value=valid_until.isoformat(),
                    right_source="operation.shipment_date",
                    right_value=shipment_date.isoformat(),
                    explanation="El documento vence antes de la fecha de despacho.",
                    evidence=(
                        _evidence(
                            document,
                            source=source,
                            field_name="valid_until",
                            value=valid_until.isoformat(),
                        ),
                        _evidence(
                            None,
                            source="operation.shipment_date",
                            field_name="shipment_date",
                            value=shipment_date.isoformat(),
                        ),
                    ),
                    left_document_id=document.assurance_document_id,
                )
            )
    return findings


def _compare_destination(operation: OperationSnapshot) -> list[ReconciliationFinding]:
    findings = _compare_text_field_across_documents(
        operation,
        field_name="destination",
        rule_code=RULE_DESTINATION,
        severity=ReconciliationSeverity.BLOCKING,
        explanation="Los documentos indican destinos o mercados distintos.",
    )
    expected = _text(
        operation.system_values.get("destination")
        or operation.system_values.get("market")
        or operation.system_values.get("destination_country")
    )
    if not expected:
        return findings
    for document in operation.documents:
        actual = _text(document.fields.get("destination"))
        if not actual or _fold(actual) == _fold(expected):
            continue
        source = _source(document, "destination")
        findings.append(
            _finding(
                operation,
                rule_code=RULE_DESTINATION,
                severity=ReconciliationSeverity.BLOCKING,
                field_name="destination",
                left_source=source,
                left_value=actual,
                right_source="operation.destination",
                right_value=expected,
                explanation="El destino del documento no coincide con el mercado de la operación.",
                evidence=(
                    _evidence(document, source=source, field_name="destination", value=actual),
                    _evidence(
                        None,
                        source="operation.destination",
                        field_name="destination",
                        value=expected,
                    ),
                ),
                left_document_id=document.assurance_document_id,
            )
        )
    return findings


def _missing_required_documents(operation: OperationSnapshot) -> list[ReconciliationFinding]:
    present = {document.semantic_type for document in operation.documents}
    findings: list[ReconciliationFinding] = []
    for raw_required in operation.required_document_types:
        required = (
            raw_required
            if isinstance(raw_required, AssuranceDocumentType)
            else AssuranceDocumentType(str(raw_required))
        )
        if required in present:
            continue
        findings.append(
            _finding(
                operation,
                rule_code=RULE_REQUIRED_MISSING,
                severity=ReconciliationSeverity.BLOCKING,
                field_name="document_type",
                left_source="operation.required_documents",
                left_value=required.value,
                right_source=None,
                right_value=None,
                explanation=f"Falta el documento requerido {required.value}.",
                evidence=(
                    _evidence(
                        None,
                        source="operation.required_documents",
                        field_name="document_type",
                        value=required.value,
                    ),
                ),
            )
        )
    return findings


def _contradictory_versions(operation: OperationSnapshot) -> list[ReconciliationFinding]:
    findings: list[ReconciliationFinding] = []
    by_type: dict[AssuranceDocumentType, list[DocumentSnapshot]] = {}
    for document in operation.documents:
        by_type.setdefault(document.semantic_type, []).append(document)

    for document_type, documents in by_type.items():
        if len(documents) < 2:
            continue
        field_names = set().union(*(document.fields.keys() for document in documents))
        for field_name in sorted(field_names):
            values = [
                (document, _text(document.fields.get(field_name)))
                for document in documents
                if _text(document.fields.get(field_name))
            ]
            if len({_fold(value) for _, value in values}) <= 1:
                continue
            baseline_document, baseline = values[0]
            for document, value in values[1:]:
                if _fold(value) == _fold(baseline):
                    continue
                left_source = _source(baseline_document, field_name)
                right_source = _source(document, field_name)
                severity = (
                    ReconciliationSeverity.BLOCKING
                    if field_name in _BLOCKING_VERSION_FIELDS
                    else ReconciliationSeverity.WARNING
                )
                findings.append(
                    _finding(
                        operation,
                        rule_code=RULE_CONTRADICTORY_VERSION,
                        severity=severity,
                        field_name=field_name,
                        left_source=left_source,
                        left_value=baseline,
                        right_source=right_source,
                        right_value=value,
                        explanation=(
                            f"Dos versiones {document_type.value} contienen valores contradictorios "
                            f"para {field_name}."
                        ),
                        evidence=(
                            _evidence(
                                baseline_document,
                                source=left_source,
                                field_name=field_name,
                                value=baseline,
                            ),
                            _evidence(
                                document,
                                source=right_source,
                                field_name=field_name,
                                value=value,
                            ),
                        ),
                        left_document_id=baseline_document.assurance_document_id,
                        right_document_id=document.assurance_document_id,
                    )
                )
    return findings


def reconcile_operation(operation: OperationSnapshot) -> tuple[ReconciliationFinding, ...]:
    """Run all deterministic reconciliation rules over one operation snapshot."""
    if not _text(operation.operation_reference):
        raise ValueError("operation_reference es obligatorio para conciliar.")

    findings: list[ReconciliationFinding] = []
    findings.extend(_compare_invoice_delivery_quantity(operation))
    findings.extend(_compare_document_operation_quantity(operation))
    findings.extend(
        _compare_text_field_across_documents(
            operation,
            field_name="product",
            rule_code=RULE_PRODUCT,
            severity=ReconciliationSeverity.WARNING,
            explanation="Los documentos describen productos distintos.",
        )
    )
    findings.extend(_compare_supplier(operation))
    findings.extend(_compare_lots(operation))
    findings.extend(_compare_dates(operation))
    findings.extend(_compare_destination(operation))
    findings.extend(_missing_required_documents(operation))
    findings.extend(_contradictory_versions(operation))

    # A single discrepancy can be detected by a specialized rule and the
    # generic contradictory-version rule. Fingerprints remain rule-specific;
    # exact duplicate emissions from the same rule are collapsed here.
    unique: dict[str, ReconciliationFinding] = {}
    for finding in findings:
        unique.setdefault(finding.fingerprint, finding)
    return tuple(
        sorted(
            unique.values(),
            key=lambda finding: (
                {"BLOCKING": 0, "WARNING": 1, "INFO": 2}[finding.severity.value],
                finding.rule_code,
                finding.fingerprint,
            ),
        )
    )
