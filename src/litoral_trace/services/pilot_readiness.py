"""Deterministic first-customer pilot readiness derived from tenant data.

No readiness flag is persisted. The service projects existing source-of-truth
objects so the checklist cannot drift from the real operational state.

The caller supplies the already-authenticated organization display name. This
service deliberately does not re-read control-plane organization/user/license
tables: authentication has already validated active user + active tenant, and
pilot readiness must remain compatible with a least-privilege runtime role.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

from sqlalchemy import and_, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    Lote,
    Shipment,
    ShipmentItem,
    TraceabilityEvent,
    VaultDocument,
)
from litoral_trace.services.eudr_dds_candidate import EudrDdsCandidateService
from litoral_trace.services.eudr_release_control import is_eudr_destination
from litoral_trace.services.shipment_export_case import ShipmentExportCaseService
from litoral_trace.services.shipment_phytosanitary_case import (
    ShipmentPhytosanitaryCaseService,
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

    def _latest_international_dispatched_shipment(self) -> Shipment | None:
        return self._session.scalar(
            select(Shipment)
            .join(
                ShipmentItem,
                and_(
                    ShipmentItem.shipment_id == Shipment.id,
                    ShipmentItem.organization_id == Shipment.organization_id,
                ),
            )
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
            .limit(1)
        )

    def evaluate(self) -> PilotReadinessView:
        try:
            lotes = self._count(
                Lote,
                Lote.organization_id == self._organization_id,
            )
            vault_documents = self._count(
                VaultDocument,
                VaultDocument.organization_id == self._organization_id,
                VaultDocument.status == "available",
            )
            posted_receipts = self._count(
                TraceabilityEvent,
                TraceabilityEvent.organization_id == self._organization_id,
                TraceabilityEvent.event_type == "RECEIPT",
                TraceabilityEvent.status == "POSTED",
            )
            posted_transformations = self._count(
                TraceabilityEvent,
                TraceabilityEvent.organization_id == self._organization_id,
                TraceabilityEvent.event_type == "TRANSFORMATION",
                TraceabilityEvent.status == "POSTED",
            )
            dispatched_shipments = self._count(
                Shipment,
                Shipment.organization_id == self._organization_id,
                Shipment.status == "DISPATCHED",
                Shipment.destination_country.is_not(None),
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
                    lotes > 0,
                    (
                        f"{lotes} lote(s) de origen disponible(s)."
                        if lotes
                        else "Cargá al menos un lote real de origen."
                    ),
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
                    posted_receipts > 0,
                    (
                        f"{posted_receipts} recepción(es) POSTED."
                        if posted_receipts
                        else "Registrá y publicá una recepción vinculada al origen."
                    ),
                    "Abrir operaciones",
                    "/operations",
                ),
                _step(
                    "TRANSFORMATION",
                    "Transformación / cadena de custodia",
                    posted_transformations > 0,
                    (
                        f"{posted_transformations} transformación(es) POSTED."
                        if posted_transformations
                        else "Publicá una transformación para demostrar genealogía industrial."
                    ),
                    "Abrir operaciones",
                    "/operations",
                ),
            ]

            shipment = self._latest_international_dispatched_shipment()
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

            compliance_ready = False
            compliance_detail = "Primero necesitás un despacho internacional DISPATCHED."
            compliance_href = "/release-control"

            if shipment is not None:
                query = urlencode({"shipment_code": shipment.shipment_code})
                compliance_href = f"/release-control?{query}"
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

                compliance_ready = bool(
                    export_readiness.ready
                    and phytosanitary_readiness.ready
                    and eudr_ready
                )
                parts = [
                    f"expediente {'READY' if export_readiness.ready else 'BLOCKED'}",
                    f"fitosanitario {'READY' if phytosanitary_readiness.ready else 'BLOCKED'}",
                ]
                if eudr_required:
                    parts.append(
                        f"EUDR {'CONFORMANCE_READY' if eudr_ready else 'BLOCKED'}"
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
            if completed_steps == len(steps):
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
                    "lotes": lotes,
                    "vault_documents": vault_documents,
                    "posted_receipts": posted_receipts,
                    "posted_transformations": posted_transformations,
                    "dispatched_shipments": dispatched_shipments,
                },
            )
        except PilotReadinessError:
            raise
        except SQLAlchemyError as exc:
            raise PilotReadinessPersistenceError(
                "No fue posible calcular el estado del piloto."
            ) from exc
