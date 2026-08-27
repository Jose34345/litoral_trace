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
from typing import Iterable, Mapping, Sequence

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
        code="INVALID_MINIMUM_INPUT",
        category="INPUT",
        status=PreflightStatus.BLOCKED,
        explanation="Faltan datos mínimos confiables para evaluar la operación.",
        action="Completar cliente, mercado, producto, cantidad, fecha y stock.",
    ),
    "INSUFFICIENT_STOCK": ReasonDefinition(
        code="INSUFFICIENT_STOCK",
        category="STOCK",
        status=PreflightStatus.BLOCKED,
        explanation="El stock disponible no alcanza para cubrir la cantidad comprometida.",
        action="Asignar stock suficiente o reducir la cantidad de la operación.",
    ),
    "ORIGIN_UNASSESSED": ReasonDefinition(
        code="ORIGIN_UNASSESSED",
        category="ORIGIN",
        status=PreflightStatus.BLOCKED,
        explanation="El origen todavía no fue evaluado para esta operación.",
        action="Completar la evaluación del origen antes de avanzar.",
    ),
    "ORIGIN_BLOCKED": ReasonDefinition(
        code="ORIGIN_BLOCKED",
        category="ORIGIN",
        status=PreflightStatus.BLOCKED,
        explanation="El control de origen tiene un bloqueo vigente.",
        action="Resolver el bloqueo de origen y volver a ejecutar el preflight.",
    ),
    "ORIGIN_PENDING": ReasonDefinition(
        code="ORIGIN_PENDING",
        category="ORIGIN",
        status=PreflightStatus.CONDITIONAL,
        explanation="El origen tiene una condición pendiente que todavía puede resolverse.",
        action="Completar la condición pendiente de origen.",
    ),
    "GENEALOGY_UNASSESSED": ReasonDefinition(
        code="GENEALOGY_UNASSESSED",
        category="GENEALOGY",
        status=PreflightStatus.BLOCKED,
        explanation="La genealogía de la operación todavía no fue evaluada.",
        action="Reconstruir y validar la genealogía del volumen asignado.",
    ),
    "GENEALOGY_BLOCKED": ReasonDefinition(
        code="GENEALOGY_BLOCKED",
        category="GENEALOGY",
        status=PreflightStatus.BLOCKED,
        explanation="La genealogía contiene volumen sin resolver o una ruptura de cadena.",
        action="Corregir la genealogía antes de liberar la operación.",
    ),
    "GENEALOGY_PENDING": ReasonDefinition(
        code="GENEALOGY_PENDING",
        category="GENEALOGY",
        status=PreflightStatus.CONDITIONAL,
        explanation="La genealogía es utilizable pero conserva una condición pendiente.",
        action="Completar la evidencia o dato pendiente de genealogía.",
    ),
    "REQUIRED_DOCUMENT_MISSING": ReasonDefinition(
        code="REQUIRED_DOCUMENT_MISSING",
        category="DOCUMENTS",
        status=PreflightStatus.CONDITIONAL,
        explanation="Falta al menos un documento requerido para cerrar la operación.",
        action="Adjuntar o vincular el documento requerido y volver a evaluar.",
    ),
    "DOCUMENT_EXPIRED_BEFORE_COMMITMENT": ReasonDefinition(
        code="DOCUMENT_EXPIRED_BEFORE_COMMITMENT",
        category="DOCUMENTS",
        status=PreflightStatus.CONDITIONAL,
        explanation="Un documento perderá vigencia antes de la fecha comprometida.",
        action="Renovar o reemplazar el documento antes del compromiso.",
    ),
    "CERTIFICATE_EXPIRING_BEFORE_COMMITMENT": ReasonDefinition(
        code="CERTIFICATE_EXPIRING_BEFORE_COMMITMENT",
        category="PHYTOSANITARY",
        status=PreflightStatus.CONDITIONAL,
        explanation="El certificado fitosanitario vence antes de la fecha comprometida.",
        action="Renovar o emitir un certificado vigente para la operación.",
    ),
    "PHYTOSANITARY_UNASSESSED": ReasonDefinition(
        code="PHYTOSANITARY_UNASSESSED",
        category="PHYTOSANITARY",
        status=PreflightStatus.BLOCKED,
        explanation="No existe una evaluación fitosanitaria suficiente para la operación.",
        action="Evaluar si el destino exige certificación fitosanitaria.",
    ),
    "PHYTOSANITARY_BLOCKED": ReasonDefinition(
        code="PHYTOSANITARY_BLOCKED",
        category="PHYTOSANITARY",
        status=PreflightStatus.BLOCKED,
        explanation="La evaluación fitosanitaria tiene un bloqueo vigente.",
        action="Completar los requisitos fitosanitarios antes de avanzar.",
    ),
    "PHYTOSANITARY_PENDING": ReasonDefinition(
        code="PHYTOSANITARY_PENDING",
        category="PHYTOSANITARY",
        status=PreflightStatus.CONDITIONAL,
        explanation="La condición fitosanitaria es solucionable pero todavía está pendiente.",
        action="Completar el requisito fitosanitario pendiente.",
    ),
    "EUDR_UNASSESSED": ReasonDefinition(
        code="EUDR_UNASSESSED",
        category="EUDR",
        status=PreflightStatus.BLOCKED,
        explanation="La operación con destino UE no tiene evaluación EUDR local suficiente.",
        action="Preparar y revisar el candidato DDS EUDR correspondiente.",
    ),
    "EUDR_BLOCKED": ReasonDefinition(
        code="EUDR_BLOCKED",
        category="EUDR",
        status=PreflightStatus.BLOCKED,
        explanation="El control EUDR local tiene requisitos pendientes que impiden avanzar.",
        action="Resolver los faltantes EUDR antes de liberar la operación.",
    ),
    "EUDR_PENDING": ReasonDefinition(
        code="EUDR_PENDING",
        category="EUDR",
        status=PreflightStatus.CONDITIONAL,
        explanation="La evaluación EUDR conserva una condición solucionable.",
        action="Completar la condición EUDR pendiente.",
    ),
    "RECONCILIATION_BLOCKING": ReasonDefinition(
        code="RECONCILIATION_BLOCKING",
        category="RECONCILIATION",
        status=PreflightStatus.BLOCKED,
        explanation="La conciliación detectó una contradicción material no resuelta.",
        action="Resolver la discrepancia entre documentos o contra el sistema.",
    ),
    "RECONCILIATION_WARNING": ReasonDefinition(
        code="RECONCILIATION_WARNING",
        category="RECONCILIATION",
        status=PreflightStatus.CONDITIONAL,
        explanation="La conciliación detectó una diferencia que requiere revisión humana.",
        action="Revisar la discrepancia antes del compromiso final.",
    ),
}


_STATUS_PRIORITY = {
    PreflightStatus.READY: 0,
    PreflightStatus.CONDITIONAL: 1,
    PreflightStatus.BLOCKED: 2,
}


def _decimal(value: object) -> Decimal:
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("Cantidad o stock inválido para preflight.") from exc


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
    missing: list[str] = []
    if not payload.customer_reference.strip():
        missing.append("customer_reference")
    if not payload.market.strip():
        missing.append("market")
    if not payload.product.strip():
        missing.append("product")
    if _decimal(payload.quantity) <= 0:
        missing.append("quantity")
    if _decimal(payload.stock_available) < 0:
        missing.append("stock_available")
    if not isinstance(payload.commitment_date, date):
        missing.append("commitment_date")
    return tuple(missing)


def evaluate_preflight(payload: PreflightInput) -> PreflightResult:
    """Evaluate one operation with fail-closed, explainable rules."""
    reasons: list[PreflightReason] = []
    missing = validate_preflight_input(payload)
    if missing:
        reasons.append(
            _reason(
                "INVALID_MINIMUM_INPUT",
                explanation="Faltan o son inválidos: " + ", ".join(missing) + ".",
                source="preflight_input",
            )
        )

    quantity = _decimal(payload.quantity)
    stock = _decimal(payload.stock_available)
    if quantity > 0 and stock >= 0 and stock < quantity:
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

    destination = payload.market.strip().upper()
    if destination in EU_EUDR_DESTINATION_CODES:
        reasons.extend(_signal_reasons("EUDR", payload.eudr_state))

    reasons.extend(_reconciliation_reasons(payload.reconciliation_findings))

    # Stable de-duplication: a reason code + source represents one actionable fact.
    deduplicated: dict[tuple[str, str | None], PreflightReason] = {}
    for reason in reasons:
        deduplicated.setdefault((reason.code, reason.source), reason)
    ordered = tuple(
        sorted(
            deduplicated.values(),
            key=lambda item: (-_STATUS_PRIORITY[item.status], item.category, item.code, item.source or ""),
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
