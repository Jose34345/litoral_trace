"""Deterministic first-customer pilot readiness derived from tenant data.

No readiness flag is persisted. The service projects existing source-of-truth
objects so the checklist cannot drift from the real operational state.

The caller supplies the already-authenticated organization display name. This
service deliberately does not re-read control-plane organization/user/license
tables: authentication has already validated active user + active tenant, and
pilot readiness must remain compatible with a least-privilege runtime role.

Once a dispatched shipment exists, the origin/receipt/transformation milestones
are anchored to that shipment's reverse genealogy. A tenant can therefore only
reach ``PILOT_READY`` with one demonstrable end-to-end chain; unrelated tenant
activity cannot be combined to manufacture readiness.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from litoral_trace.db.models import Lote, Shipment, TraceabilityEvent, VaultDocument
from litoral_trace.services.eudr_dds_candidate import EudrDdsCandidateService
from litoral_trace.services.eudr_release_control import is_eudr_destination
from litoral_trace.services.shipment_export_case import ShipmentExportCaseService
from litoral_trace.services.shipment_phytosanitary_case import (
    ShipmentPhytosanitaryCaseService,
)
from litoral_trace.services.traceability_lineage import (
    TraceabilityLineageError,
    TraceabilityLineageService,
)


PILOT_NOT_STARTED = "NOT_STARTED"
PILOT_IN_PROGRESS = "IN_PROGRESS"
PILOT_READY = "PILOT_READY"


class PilotReadinessError(RuntimeError):
    """Safe base error for first-customer readiness projection."""


class PilotReadinessPersistenceError(PilotReadinessError):
    pass


@dataclass(frozen=True)
class PilotStepView:
    key: str
    label: str
    completed: bool
    detail: str
    action_label: str
    action_href: str


@dataclass(frozen=True)
class PilotReadinessView:
    organization_id: int
    organization_name: str
    state: str
    completed_steps: int
    total_steps: int
    shipment_code: str | None
    steps: tuple[PilotStepView, ...]
    counts: dict[str, int]

    @property
    def ready(self) -> bool:
        return self.state == PILOT_READY


@dataclass(frozen=True)
class _ShipmentPilotProjection:
    shipment: Shipment
    lineage_complete: bool
    source_lotes: int
    posted_receipts: int
    posted_transformations: int
    export_ready: bool
    phytosanitary_ready: bool
    eudr_required: bool
    eudr_ready: bool

    @property
    def chain_ready(self) -> bool:
        return bool(
            self.lineage_complete
            and self.source_lotes > 0
            and self.posted_receipts > 0
            and self.posted_transformations > 0
        )

    @property
    def release_ready(self) -> bool:
        return bool(
            self.export_ready
            and self.phytosanitary_ready
            and self.eudr_ready
        )

    @property
    def qualifies(self) -> bool:
        return self.chain_ready and self.release_ready


def _step(
    key: str,
    label: str,
    completed: bool,
    detail: str,
    action_label: str,
    action_href: str,
) -> PilotStepView:
    return PilotStepView(
        key=key,
        label=label,
        completed=bool(completed),
        detail=detail,
        action_label=action_label,
        action_href=action_href,
    )


class PilotReadinessService:
    """Project a tenant's ability to execute one commercially useful pilot."""

    def __init__(
        self,
        *,
        session: Session,
        organization_id: int,
        organization_name: str = "Organización activa",
    ) -> None:
        self._session = session
        self._organization_id = int(organization_id)
        self._organization_name = str(organization_name or "Organización activa").strip()

    def _count(self, model: Any, *criteria: Any) -> int:
        return int(
            self._session.scalar(
                select(func.count()).select_from(model).where(*criteria)
            )
            or 0
        )

    def _dispatched_international_shipments(self) -> list[Shipment]:
        return list(
            self._session.execute(
                select(Shipment)
                .where(
                    Shipment.organization_id == self._organization_id,
                    Shipment.status == "DISPATCHED",
                    Shipment.destination_country.is_not(None),
                )
                .order_by(
                    Shipment.shipped_at.desc().nullslast(),
                    Shipment.created_at.desc(),
                    Shipment.id.desc(),
                )
            ).scalars().all()
        )

    def _project_shipment(self, shipment: Shipment) -> _ShipmentPilotProjection:
        lineage_complete = False
        source_lotes = 0
        posted_receipts = 0
        posted_transformations = 0
        try:
            lineage = TraceabilityLineageService(
                session=self._session,
                organization_id=self._organization_id,
            ).trace_shipment(shipment.shipment_code)
            lineage_complete = bool(lineage.get("complete"))
            source_lotes = len(lineage.get("source_lotes") or ())
            events = tuple(lineage.get("events") or ())
            posted_receipts = sum(
                1
                for event in events
                if event.get("status") == "POSTED"
                and event.get("event_type") == "RECEIPT"
            )
            posted_transformations = sum(
                1
                for event in events
                if event.get("status") == "POSTED"
                and event.get("event_type") == "TRANSFORMATION"
            )
        except TraceabilityLineageError:
            # A broken lineage is a BLOCKED pilot candidate, not a reason to
            # expose internal traceability errors or abort the whole checklist.
            pass

        export_readiness = ShipmentExportCaseService(
            session=self._session,
            organization_id=self._organization_id,
        ).readiness(shipment.shipment_code)
        phytosanitary_readiness = ShipmentPhytosanitaryCaseService(
            session=self._session,
            organization_id=self._organization_id,
        ).readiness(shipment.shipment_code)

        eudr_required = is_eudr_destination(
            {
                "shipment": {
                    "destination_country": shipment.destination_country,
                    "shipment_code": shipment.shipment_code,
                }
            }
        )
        eudr_ready = True
        if eudr_required:
            eudr_ready = EudrDdsCandidateService(
                session=self._session,
                organization_id=self._organization_id,
            ).conformance(shipment.shipment_code).ready

        return _ShipmentPilotProjection(
            shipment=shipment,
            lineage_complete=lineage_complete,
            source_lotes=source_lotes,
            posted_receipts=posted_receipts,
            posted_transformations=posted_transformations,
            export_ready=bool(export_readiness.ready),
            phytosanitary_ready=bool(phytosanitary_readiness.ready),
            eudr_required=bool(eudr_required),
            eudr_ready=bool(eudr_ready),
        )

    def _select_shipment_projection(
        self,
        shipments: list[Shipment],
    ) -> _ShipmentPilotProjection | None:
        fallback: _ShipmentPilotProjection | None = None
        for shipment in shipments:
            projection = self._project_shipment(shipment)
            if fallback is None:
                fallback = projection
            if projection.qualifies:
                return projection
        return fallback

    def evaluate(self) -> PilotReadinessView:
        try:
            tenant_lotes = self._count(
                Lote,
                Lote.organization_id == self._organization_id,
            )
            vault_documents = self._count(
                VaultDocument,
                VaultDocument.organization_id == self._organization_id,
                VaultDocument.status == "available",
            )
            tenant_posted_receipts = self._count(
                TraceabilityEvent,
                TraceabilityEvent.organization_id == self._organization_id,
                TraceabilityEvent.event_type == "RECEIPT",
                TraceabilityEvent.status == "POSTED",
            )
            tenant_posted_transformations = self._count(
                TraceabilityEvent,
                TraceabilityEvent.organization_id == self._organization_id,
                TraceabilityEvent.event_type == "TRANSFORMATION",
                TraceabilityEvent.status == "POSTED",
            )
            shipments = self._dispatched_international_shipments()
            projection = self._select_shipment_projection(shipments)

            if projection is None:
                origin_count = tenant_lotes
                receipt_count = tenant_posted_receipts
                transformation_count = tenant_posted_transformations
                origin_detail = (
                    f"{origin_count} lote(s) de origen disponible(s)."
                    if origin_count
                    else "Cargá al menos un lote real de origen."
                )
                receipt_detail = (
                    f"{receipt_count} recepción(es) POSTED."
                    if receipt_count
                    else "Registrá y publicá una recepción vinculada al origen."
                )
                transformation_detail = (
                    f"{transformation_count} transformación(es) POSTED."
                    if transformation_count
                    else "Publicá una transformación para demostrar genealogía industrial."
                )
            else:
                origin_count = projection.source_lotes if projection.lineage_complete else 0
                receipt_count = projection.posted_receipts if projection.lineage_complete else 0
                transformation_count = (
                    projection.posted_transformations if projection.lineage_complete else 0
                )
                origin_detail = (
                    f"{origin_count} lote(s) de origen reconstruido(s) para {projection.shipment.shipment_code}."
                    if origin_count
                    else f"El despacho {projection.shipment.shipment_code} no tiene origen completo reconstruible."
                )
                receipt_detail = (
                    f"{receipt_count} recepción(es) POSTED dentro de la genealogía de {projection.shipment.shipment_code}."
                    if receipt_count
                    else f"La genealogía de {projection.shipment.shipment_code} no contiene una recepción POSTED válida."
                )
                transformation_detail = (
                    f"{transformation_count} transformación(es) POSTED dentro de la genealogía de {projection.shipment.shipment_code}."
                    if transformation_count
                    else f"La genealogía de {projection.shipment.shipment_code} no contiene una transformación POSTED."
                )

            steps: list[PilotStepView] = [
                _step(
                    "TENANT",
                    "Organización y acceso",
                    True,
                    "Sesión tenant activa, usuario vigente y permiso de lectura verificado por autenticación.",
                    "Abrir configuración",
                    "/settings",
                ),
                _step(
                    "ORIGIN",
                    "Origen y lotes",
                    origin_count > 0,
                    origin_detail,
                    "Cargar lotes",
                    "/imports",
                ),
                _step(
                    "EVIDENCE",
                    "Evidencia documental",
                    vault_documents > 0,
                    (
                        f"{vault_documents} documento(s) disponible(s) en Vault."
                        if vault_documents
                        else "Subí al menos una evidencia documental al Vault."
                    ),
                    "Abrir evidencias",
                    "/evidence",
                ),
                _step(
                    "RECEIPT",
                    "Recepción trazable",
                    receipt_count > 0,
                    receipt_detail,
                    "Abrir operaciones",
                    "/operations",
                ),
                _step(
                    "TRANSFORMATION",
                    "Transformación / cadena de custodia",
                    transformation_count > 0,
                    transformation_detail,
                    "Abrir operaciones",
                    "/operations",
                ),
            ]

            shipment = projection.shipment if projection is not None else None
            shipment_code = shipment.shipment_code if shipment is not None else None
            steps.append(
                _step(
                    "SHIPMENT",
                    "Despacho internacional",
                    shipment is not None,
                    (
                        f"Despacho {shipment.shipment_code} DISPATCHED hacia {shipment.destination_country}."
                        if shipment is not None
                        else "Creá y despachá una salida internacional con material trazado."
                    ),
                    "Abrir operaciones",
                    "/operations",
                )
            )

            compliance_ready = bool(projection and projection.release_ready)
            compliance_detail = "Primero necesitás un despacho internacional DISPATCHED."
            compliance_href = "/release-control"
            if projection is not None:
                query = urlencode({"shipment_code": projection.shipment.shipment_code})
                compliance_href = f"/release-control?{query}"
                parts = [
                    f"expediente {'READY' if projection.export_ready else 'BLOCKED'}",
                    f"fitosanitario {'READY' if projection.phytosanitary_ready else 'BLOCKED'}",
                ]
                if projection.eudr_required:
                    parts.append(
                        f"EUDR {'CONFORMANCE_READY' if projection.eudr_ready else 'BLOCKED'}"
                    )
                compliance_detail = " · ".join(parts) + "."

            steps.append(
                _step(
                    "RELEASE",
                    "Expediente y Control de Salida",
                    compliance_ready,
                    compliance_detail,
                    "Abrir Control de Salida",
                    compliance_href,
                )
            )

            completed_steps = sum(step.completed for step in steps)
            operational_started = any(step.completed for step in steps[1:])
            selected_chain_ready = bool(projection and projection.chain_ready)
            if (
                completed_steps == len(steps)
                and selected_chain_ready
                and compliance_ready
            ):
                state = PILOT_READY
            elif operational_started:
                state = PILOT_IN_PROGRESS
            else:
                state = PILOT_NOT_STARTED

            return PilotReadinessView(
                organization_id=self._organization_id,
                organization_name=self._organization_name,
                state=state,
                completed_steps=completed_steps,
                total_steps=len(steps),
                shipment_code=shipment_code,
                steps=tuple(steps),
                counts={
                    "authenticated_tenant": 1,
                    "lotes": origin_count,
                    "vault_documents": vault_documents,
                    "posted_receipts": receipt_count,
                    "posted_transformations": transformation_count,
                    "dispatched_shipments": len(shipments),
                },
            )
        except PilotReadinessError:
            raise
        except SQLAlchemyError as exc:
            raise PilotReadinessPersistenceError(
                "No fue posible calcular el estado del piloto."
            ) from exc
