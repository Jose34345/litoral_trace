"""Deterministic Assurance v1 preflight decision engine.

Preflight is an operational readiness projection, not a legal opinion. It
combines explicit business inputs with already-computed origin, documentary,
phytosanitary, EUDR and reconciliation signals and returns one explainable
READY / CONDITIONAL / BLOCKED result.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Mapping, Sequence

from litoral_trace.assurance.domain import ReconciliationSeverity
from litoral_trace.assurance.reconciliation import ReconciliationFinding
from litoral_trace.services.eudr_release_control import EU_EUDR_DESTINATION_CODES


class PreflightStatus(StrEnum):
    READY = "READY"
    CONDITIONAL = "CONDITIONAL"
    BLOCKED = "BLOCKED"


class PreflightSignalState(StrEnum):
    READY = "READY"
    PENDING = "PENDING"
    BLOCKED = "BLOCKED"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    UNASSESSED = "UNASSESSED"


@dataclass(frozen=True, slots=True)
class ReasonDefinition:
    code: str
    category: str
    status: PreflightStatus
    explanation: str
    action: str


@dataclass(frozen=True, slots=True)
class PreflightDocument:
    document_type: str
    reference: str
    valid_until: date | datetime | None = None


@dataclass(frozen=True, slots=True)
class PreflightInput:
    customer_reference: str
    market: str
    product: str
    quantity: Decimal
    commitment_date: date
    stock_available: Decimal
    documents: tuple[PreflightDocument, ...] = ()
    required_document_types: tuple[str, ...] = ()
    origin_state: PreflightSignalState = PreflightSignalState.UNASSESSED
    genealogy_state: PreflightSignalState = PreflightSignalState.UNASSESSED
    phytosanitary_state: PreflightSignalState = PreflightSignalState.UNASSESSED
    eudr_state: PreflightSignalState = PreflightSignalState.UNASSESSED
    reconciliation_findings: tuple[ReconciliationFinding, ...] = ()


@dataclass(frozen=True, slots=True)
class PreflightReason:
    code: str
    category: str
    status: PreflightStatus
    explanation: str
    action: str
    source: str | None = None


@dataclass(frozen=True, slots=True)
class PreflightResult:
    status: PreflightStatus
    reasons: tuple[PreflightReason, ...]
    requires_human_action: bool

    @property
    def reason_codes(self) -> tuple[str, ...]:
        return tuple(reason.code for reason in self.reasons)


REASON_CATALOG: Mapping[str, ReasonDefinition] = {
    "INVALID_MINIMUM_INPUT": ReasonDefinition(
        "INVALID_MINIMUM_INPUT", "INPUT", PreflightStatus.BLOCKED,
        "Faltan datos mínimos confiables para evaluar la operación.",
        "Completar cliente, mercado, producto, cantidad, fecha y stock.",
    ),
    "INSUFFICIENT_STOCK": ReasonDefinition(
        "INSUFFICIENT_STOCK", "STOCK", PreflightStatus.BLOCKED,
        "El stock disponible no alcanza para cubrir la cantidad comprometida.",
        "Asignar stock suficiente o reducir la cantidad de la operación.",
    ),
    "ORIGIN_UNASSESSED": ReasonDefinition(
        "ORIGIN_UNASSESSED", "ORIGIN", PreflightStatus.BLOCKED,
        "El origen todavía no fue evaluado para esta operación.",
        "Completar la evaluación del origen antes de avanzar.",
    ),
    "ORIGIN_BLOCKED": ReasonDefinition(
        "ORIGIN_BLOCKED", "ORIGIN", PreflightStatus.BLOCKED,
        "El control de origen tiene un bloqueo vigente.",
        "Resolver el bloqueo de origen y volver a ejecutar el preflight.",
    ),
    "ORIGIN_PENDING": ReasonDefinition(
        "ORIGIN_PENDING", "ORIGIN", PreflightStatus.CONDITIONAL,
        "El origen tiene una condición pendiente que todavía puede resolverse.",
        "Completar la condición pendiente de origen.",
    ),
    "GENEALOGY_UNASSESSED": ReasonDefinition(
        "GENEALOGY_UNASSESSED", "GENEALOGY", PreflightStatus.BLOCKED,
        "La genealogía de la operación todavía no fue evaluada.",
        "Reconstruir y validar la genealogía del volumen asignado.",
    ),
    "GENEALOGY_BLOCKED": ReasonDefinition(
        "GENEALOGY_BLOCKED", "GENEALOGY", PreflightStatus.BLOCKED,
        "La genealogía contiene volumen sin resolver o una ruptura de cadena.",
        "Corregir la genealogía antes de liberar la operación.",
    ),
    "GENEALOGY_PENDING": ReasonDefinition(
        "GENEALOGY_PENDING", "GENEALOGY", PreflightStatus.CONDITIONAL,
        "La genealogía es utilizable pero conserva una condición pendiente.",
        "Completar la evidencia o dato pendiente de genealogía.",
    ),
    "REQUIRED_DOCUMENT_MISSING": ReasonDefinition(
        "REQUIRED_DOCUMENT_MISSING", "DOCUMENTS", PreflightStatus.CONDITIONAL,
        "Falta al menos un documento requerido para cerrar la operación.",
        "Adjuntar o vincular el documento requerido y volver a evaluar.",
    ),
    "DOCUMENT_EXPIRED_BEFORE_COMMITMENT": ReasonDefinition(
        "DOCUMENT_EXPIRED_BEFORE_COMMITMENT", "DOCUMENTS", PreflightStatus.CONDITIONAL,
        "Un documento perderá vigencia antes de la fecha comprometida.",
        "Renovar o reemplazar el documento antes del compromiso.",
    ),
    "CERTIFICATE_EXPIRING_BEFORE_COMMITMENT": ReasonDefinition(
        "CERTIFICATE_EXPIRING_BEFORE_COMMITMENT", "PHYTOSANITARY", PreflightStatus.CONDITIONAL,
        "El certificado fitosanitario vence antes de la fecha comprometida.",
        "Renovar o emitir un certificado vigente para la operación.",
    ),
    "PHYTOSANITARY_UNASSESSED": ReasonDefinition(
        "PHYTOSANITARY_UNASSESSED", "PHYTOSANITARY", PreflightStatus.BLOCKED,
        "No existe una evaluación fitosanitaria suficiente para la operación.",
        "Evaluar si el destino exige certificación fitosanitaria.",
    ),
    "PHYTOSANITARY_BLOCKED": ReasonDefinition(
        "PHYTOSANITARY_BLOCKED", "PHYTOSANITARY", PreflightStatus.BLOCKED,
        "La evaluación fitosanitaria tiene un bloqueo vigente.",
        "Completar los requisitos fitosanitarios antes de avanzar.",
    ),
    "PHYTOSANITARY_PENDING": ReasonDefinition(
        "PHYTOSANITARY_PENDING", "PHYTOSANITARY", PreflightStatus.CONDITIONAL,
        "La condición fitosanitaria es solucionable pero todavía está pendiente.",
        "Completar el requisito fitosanitario pendiente.",
    ),
    "EUDR_UNASSESSED": ReasonDefinition(
        "EUDR_UNASSESSED", "EUDR", PreflightStatus.BLOCKED,
        "La operación con destino UE no tiene evaluación EUDR local suficiente.",
        "Preparar y revisar el candidato DDS EUDR correspondiente.",
    ),
    "EUDR_BLOCKED": ReasonDefinition(
        "EUDR_BLOCKED", "EUDR", PreflightStatus.BLOCKED,
        "El control EUDR local tiene requisitos pendientes que impiden avanzar.",
        "Resolver los faltantes EUDR antes de liberar la operación.",
    ),
    "EUDR_PENDING": ReasonDefinition(
        "EUDR_PENDING", "EUDR", PreflightStatus.CONDITIONAL,
        "La evaluación EUDR conserva una condición solucionable.",
        "Completar la condición EUDR pendiente.",
    ),
    "RECONCILIATION_BLOCKING": ReasonDefinition(
        "RECONCILIATION_BLOCKING", "RECONCILIATION", PreflightStatus.BLOCKED,
        "La conciliación detectó una contradicción material no resuelta.",
        "Resolver la discrepancia entre documentos o contra el sistema.",
    ),
    "RECONCILIATION_WARNING": ReasonDefinition(
        "RECONCILIATION_WARNING", "RECONCILIATION", PreflightStatus.CONDITIONAL,
        "La conciliación detectó una diferencia que requiere revisión humana.",
        "Revisar la discrepancia antes del compromiso final.",
    ),
}

_STATUS_PRIORITY = {
    PreflightStatus.READY: 0,
    PreflightStatus.CONDITIONAL: 1,
    PreflightStatus.BLOCKED: 2,
}


def _try_decimal(value: object) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _date(value: date | datetime) -> date:
    return value.date() if isinstance(value, datetime) else value


def _reason(
    code: str,
    *,
    explanation: str | None = None,
    action: str | None = None,
    source: str | None = None,
) -> PreflightReason:
    definition = REASON_CATALOG[code]
    return PreflightReason(
        code=definition.code,
        category=definition.category,
        status=definition.status,
        explanation=explanation or definition.explanation,
        action=action or definition.action,
        source=source,
    )


def _signal_reasons(prefix: str, state: PreflightSignalState) -> tuple[PreflightReason, ...]:
    if state in {PreflightSignalState.READY, PreflightSignalState.NOT_APPLICABLE}:
        return ()
    if state == PreflightSignalState.UNASSESSED:
        return (_reason(f"{prefix}_UNASSESSED"),)
    if state == PreflightSignalState.BLOCKED:
        return (_reason(f"{prefix}_BLOCKED"),)
    if state == PreflightSignalState.PENDING:
        return (_reason(f"{prefix}_PENDING"),)
    return ()


def _document_reasons(payload: PreflightInput) -> tuple[PreflightReason, ...]:
    if not isinstance(payload.commitment_date, date):
        return ()
    reasons: list[PreflightReason] = []
    present_types = {document.document_type.strip().upper() for document in payload.documents}
    for required_type in payload.required_document_types:
        normalized = str(required_type or "").strip().upper()
        if normalized and normalized not in present_types:
            reasons.append(
                _reason(
                    "REQUIRED_DOCUMENT_MISSING",
                    explanation=f"Falta el documento requerido {normalized}.",
                    source=f"required_document:{normalized}",
                )
            )

    commitment = payload.commitment_date
    for document in payload.documents:
        if document.valid_until is None:
            continue
        valid_until = _date(document.valid_until)
        if valid_until >= commitment:
            continue
        normalized_type = document.document_type.strip().upper()
        if normalized_type == "PHYTOSANITARY_CERTIFICATE":
            reasons.append(
                _reason(
                    "CERTIFICATE_EXPIRING_BEFORE_COMMITMENT",
                    explanation=(
                        f"{document.reference} vence el {valid_until.isoformat()} antes del "
                        f"compromiso {commitment.isoformat()}."
                    ),
                    source=document.reference,
                )
            )
        else:
            reasons.append(
                _reason(
                    "DOCUMENT_EXPIRED_BEFORE_COMMITMENT",
                    explanation=(
                        f"{document.reference} vence el {valid_until.isoformat()} antes del "
                        f"compromiso {commitment.isoformat()}."
                    ),
                    source=document.reference,
                )
            )
    return tuple(reasons)


def _reconciliation_reasons(
    findings: Sequence[ReconciliationFinding],
) -> tuple[PreflightReason, ...]:
    reasons: list[PreflightReason] = []
    for finding in findings:
        if finding.severity == ReconciliationSeverity.BLOCKING:
            reasons.append(
                _reason(
                    "RECONCILIATION_BLOCKING",
                    explanation=f"{finding.rule_code}: {finding.explanation}",
                    source=finding.left_source,
                )
            )
        elif finding.severity == ReconciliationSeverity.WARNING:
            reasons.append(
                _reason(
                    "RECONCILIATION_WARNING",
                    explanation=f"{finding.rule_code}: {finding.explanation}",
                    source=finding.left_source,
                )
            )
    return tuple(reasons)


def validate_preflight_input(payload: PreflightInput) -> tuple[str, ...]:
    invalid: list[str] = []
    if not str(payload.customer_reference or "").strip():
        invalid.append("customer_reference")
    if not str(payload.market or "").strip():
        invalid.append("market")
    if not str(payload.product or "").strip():
        invalid.append("product")

    quantity = _try_decimal(payload.quantity)
    stock = _try_decimal(payload.stock_available)
    if quantity is None or quantity <= 0:
        invalid.append("quantity")
    if stock is None or stock < 0:
        invalid.append("stock_available")
    if not isinstance(payload.commitment_date, date):
        invalid.append("commitment_date")
    return tuple(invalid)


def evaluate_preflight(payload: PreflightInput) -> PreflightResult:
    """Evaluate one operation with fail-closed, explainable rules."""
    reasons: list[PreflightReason] = []
    invalid = validate_preflight_input(payload)
    if invalid:
        reasons.append(
            _reason(
                "INVALID_MINIMUM_INPUT",
                explanation="Faltan o son inválidos: " + ", ".join(invalid) + ".",
                source="preflight_input",
            )
        )

    quantity = _try_decimal(payload.quantity)
    stock = _try_decimal(payload.stock_available)
    if quantity is not None and quantity > 0 and stock is not None and stock >= 0 and stock < quantity:
        reasons.append(
            _reason(
                "INSUFFICIENT_STOCK",
                explanation=f"Stock {stock} menor que cantidad comprometida {quantity}.",
                source="stock_available",
            )
        )

    reasons.extend(_signal_reasons("ORIGIN", payload.origin_state))
    reasons.extend(_signal_reasons("GENEALOGY", payload.genealogy_state))
    reasons.extend(_document_reasons(payload))
    reasons.extend(_signal_reasons("PHYTOSANITARY", payload.phytosanitary_state))

    destination = str(payload.market or "").strip().upper()
    if destination in EU_EUDR_DESTINATION_CODES:
        if payload.eudr_state == PreflightSignalState.NOT_APPLICABLE:
            reasons.append(
                _reason(
                    "EUDR_UNASSESSED",
                    explanation=(
                        "El destino está dentro del alcance geográfico UE y no puede "
                        "tratarse como EUDR no aplicable sin una evaluación concreta."
                    ),
                    source="eudr_state",
                )
            )
        else:
            reasons.extend(_signal_reasons("EUDR", payload.eudr_state))

    reasons.extend(_reconciliation_reasons(payload.reconciliation_findings))

    deduplicated: dict[tuple[str, str | None], PreflightReason] = {}
    for reason in reasons:
        deduplicated.setdefault((reason.code, reason.source), reason)
    ordered = tuple(
        sorted(
            deduplicated.values(),
            key=lambda item: (
                -_STATUS_PRIORITY[item.status],
                item.category,
                item.code,
                item.source or "",
            ),
        )
    )

    if any(reason.status == PreflightStatus.BLOCKED for reason in ordered):
        status = PreflightStatus.BLOCKED
    elif any(reason.status == PreflightStatus.CONDITIONAL for reason in ordered):
        status = PreflightStatus.CONDITIONAL
    else:
        status = PreflightStatus.READY

    return PreflightResult(
        status=status,
        reasons=ordered,
        requires_human_action=status != PreflightStatus.READY,
    )


def reason_catalog_payload() -> tuple[dict[str, str], ...]:
    """Stable serializable catalog for API/UI consumers."""
    return tuple(
        {
            "code": definition.code,
            "category": definition.category,
            "default_status": definition.status.value,
            "explanation": definition.explanation,
            "action": definition.action,
        }
        for definition in sorted(REASON_CATALOG.values(), key=lambda item: item.code)
    )
