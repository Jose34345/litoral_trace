"""Corrientes + ARCA export-case metadata and deterministic readiness.

The service never copies binary evidence out of Vault and never mutates the
traceability ledger. It evaluates only tenant-scoped shipment metadata,
structured external references and active/available Vault evidence links.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import and_, func, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    Shipment,
    ShipmentExportCase,
    TraceabilityEvidenceLink,
    VaultDocument,
)
from litoral_trace.db.models.shipment_export_case import EXPORT_ORIGIN_PROFILES


class ShipmentExportCaseError(RuntimeError):
    """Base safe error for export-case workflows."""


class ShipmentExportCaseNotFoundError(ShipmentExportCaseError):
    pass


class ShipmentExportCaseValidationError(ShipmentExportCaseError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail)


class ShipmentExportCasePersistenceError(ShipmentExportCaseError):
    pass


@dataclass(frozen=True)
class ShipmentExportCaseView:
    public_id: UUID
    shipment_public_id: UUID
    shipment_code: str
    origin_profile: str
    export_invoice_number: str | None
    export_invoice_cae: str | None
    customs_destination_id: str | None
    customs_subregime: str | None
    customs_officialized_at: datetime | None
    notes: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ExportRequirementView:
    key: str
    label: str
    satisfied: bool
    source: str


@dataclass(frozen=True)
class ShipmentExportReadinessView:
    shipment_public_id: UUID
    shipment_code: str
    state: str
    origin_profile: str | None
    requirements: tuple[ExportRequirementView, ...]
    missing: tuple[str, ...]
    evidence_types: tuple[str, ...]
    export_case: ShipmentExportCaseView | None

    @property
    def ready(self) -> bool:
        return self.state == "READY"


def _optional_text(value: Any, *, maximum: int, uppercase: bool = False) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    if len(normalized) > maximum:
        raise ShipmentExportCaseValidationError(
            "TEXT_TOO_LONG",
            f"El valor supera el máximo de {maximum} caracteres.",
        )
    return normalized.upper() if uppercase else normalized


def _normalize_profile(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in EXPORT_ORIGIN_PROFILES:
        raise ShipmentExportCaseValidationError(
            "INVALID_ORIGIN_PROFILE",
            "El perfil forestal debe ser CULTIVATED o NATIVE.",
        )
    return normalized


class ShipmentExportCaseService:
    """Manage one structured export dossier per shipment without ledger writes."""

    def __init__(self, *, session: Session, organization_id: int) -> None:
        self._session = session
        self._organization_id = int(organization_id)

    def _shipment(self, shipment_code: str) -> Shipment:
        code = str(shipment_code or "").strip()
        if not code or len(code) > 120:
            raise ShipmentExportCaseValidationError(
                "INVALID_SHIPMENT_CODE",
                "Debe informar un código de despacho válido.",
            )
        shipment = self._session.scalar(
            select(Shipment).where(
                Shipment.organization_id == self._organization_id,
                func.lower(Shipment.shipment_code) == code.lower(),
            )
        )
        if shipment is None:
            raise ShipmentExportCaseNotFoundError(
                "El despacho no existe en la organización activa."
            )
        return shipment

    @staticmethod
    def _view(case: ShipmentExportCase, shipment: Shipment) -> ShipmentExportCaseView:
        return ShipmentExportCaseView(
            public_id=case.public_id,
            shipment_public_id=shipment.public_id,
            shipment_code=shipment.shipment_code,
            origin_profile=case.origin_profile,
            export_invoice_number=case.export_invoice_number,
            export_invoice_cae=case.export_invoice_cae,
            customs_destination_id=case.customs_destination_id,
            customs_subregime=case.customs_subregime,
            customs_officialized_at=case.customs_officialized_at,
            notes=case.notes,
            created_at=case.created_at,
            updated_at=case.updated_at,
        )

    def get_case(self, shipment_code: str) -> ShipmentExportCaseView | None:
        shipment = self._shipment(shipment_code)
        case = self._session.scalar(
            select(ShipmentExportCase).where(
                ShipmentExportCase.organization_id == self._organization_id,
                ShipmentExportCase.shipment_id == shipment.id,
            )
        )
        return self._view(case, shipment) if case is not None else None

    def upsert_case(
        self,
        *,
        shipment_code: str,
        origin_profile: str,
        export_invoice_number: Any = None,
        export_invoice_cae: Any = None,
        customs_destination_id: Any = None,
        customs_subregime: Any = None,
        customs_officialized_at: datetime | None = None,
        notes: Any = None,
        actor_user_id: int | None = None,
    ) -> ShipmentExportCaseView:
        shipment = self._shipment(shipment_code)
        profile = _normalize_profile(origin_profile)
        invoice_number = _optional_text(export_invoice_number, maximum=80)
        invoice_cae = _optional_text(export_invoice_cae, maximum=32)
        destination_id = _optional_text(customs_destination_id, maximum=64, uppercase=True)
        subregime = _optional_text(customs_subregime, maximum=16, uppercase=True)
        normalized_notes = _optional_text(notes, maximum=2000)

        case = self._session.scalar(
            select(ShipmentExportCase).where(
                ShipmentExportCase.organization_id == self._organization_id,
                ShipmentExportCase.shipment_id == shipment.id,
            )
        )
        if case is None:
            case = ShipmentExportCase(
                organization_id=self._organization_id,
                shipment_id=int(shipment.id),
                origin_profile=profile,
                created_by_user_id=actor_user_id,
                updated_by_user_id=actor_user_id,
            )
            self._session.add(case)
        else:
            case.origin_profile = profile
            case.updated_by_user_id = actor_user_id

        case.export_invoice_number = invoice_number
        case.export_invoice_cae = invoice_cae
        case.customs_destination_id = destination_id
        case.customs_subregime = subregime
        case.customs_officialized_at = customs_officialized_at
        case.notes = normalized_notes

        try:
            self._session.flush()
            result = self._view(case, shipment)
            self._session.commit()
            return result
        except IntegrityError as exc:
            self._session.rollback()
            raise ShipmentExportCaseValidationError(
                "EXPORT_CASE_CONFLICT",
                "No fue posible guardar el expediente porque sus referencias entran en conflicto.",
            ) from exc
        except SQLAlchemyError as exc:
            self._session.rollback()
            raise ShipmentExportCasePersistenceError(
                "No fue posible guardar el expediente exportador."
            ) from exc

    def _active_evidence_types(self, shipment: Shipment) -> tuple[str, ...]:
        rows = self._session.scalars(
            select(TraceabilityEvidenceLink.evidence_type)
            .join(
                VaultDocument,
                and_(
                    VaultDocument.id == TraceabilityEvidenceLink.vault_document_id,
                    VaultDocument.organization_id == TraceabilityEvidenceLink.organization_id,
                ),
            )
            .where(
                TraceabilityEvidenceLink.organization_id == self._organization_id,
                TraceabilityEvidenceLink.shipment_id == shipment.id,
                TraceabilityEvidenceLink.unlinked_at.is_(None),
                VaultDocument.status == "available",
            )
            .distinct()
        ).all()
        return tuple(sorted(str(value) for value in rows))

    @staticmethod
    def _requirement(
        key: str,
        label: str,
        satisfied: bool,
        source: str,
    ) -> ExportRequirementView:
        return ExportRequirementView(
            key=key,
            label=label,
            satisfied=bool(satisfied),
            source=source,
        )

    def readiness(self, shipment_code: str) -> ShipmentExportReadinessView:
        shipment = self._shipment(shipment_code)
        case_row = self._session.scalar(
            select(ShipmentExportCase).where(
                ShipmentExportCase.organization_id == self._organization_id,
                ShipmentExportCase.shipment_id == shipment.id,
            )
        )
        evidence_types = self._active_evidence_types(shipment)
        evidence = set(evidence_types)
        requirements: list[ExportRequirementView] = []

        if case_row is None:
            requirements.append(
                self._requirement(
                    "EXPORT_CASE",
                    "Definir perfil forestal del expediente",
                    False,
                    "Expediente",
                )
            )
            profile = None
            case_view = None
        else:
            profile = case_row.origin_profile
            case_view = self._view(case_row, shipment)
            if profile == "CULTIVATED":
                requirements.extend(
                    [
                        self._requirement(
                            "CULTIVATED_INVOICE_OR_REMITO",
                            "Factura o Remito oficial del traslado",
                            bool({"INVOICE", "REMITO"} & evidence),
                            "Vault",
                        ),
                        self._requirement(
                            "FRUIT_GUIDE",
                            "Guía de Frutos de Corrientes",
                            "FRUIT_GUIDE" in evidence,
                            "Vault",
                        ),
                    ]
                )
            else:
                requirements.extend(
                    [
                        self._requirement(
                            "FOREST_GUIDE",
                            "Guía de Productos Forestales Nativos",
                            "FOREST_GUIDE" in evidence,
                            "Vault",
                        ),
                        self._requirement(
                            "FOREST_TRANSPORT_VOUCHER",
                            "Vale de Transporte forestal",
                            "TRANSPORT" in evidence,
                            "Vault",
                        ),
                    ]
                )

            requirements.extend(
                [
                    self._requirement(
                        "EXPORT_INVOICE_E",
                        "Factura E de exportación",
                        bool(case_row.export_invoice_number),
                        "ARCA",
                    ),
                    self._requirement(
                        "SIM_DESTINATION",
                        "Identificador de destinación aduanera SIM",
                        bool(case_row.customs_destination_id),
                        "SIM",
                    ),
                    self._requirement(
                        "SIM_SUBREGIME",
                        "Subrégimen de la destinación SIM",
                        bool(case_row.customs_subregime),
                        "SIM",
                    ),
                ]
            )

        missing = tuple(item.key for item in requirements if not item.satisfied)
        state = "READY" if requirements and not missing else "BLOCKED"
        return ShipmentExportReadinessView(
            shipment_public_id=shipment.public_id,
            shipment_code=shipment.shipment_code,
            state=state,
            origin_profile=profile,
            requirements=tuple(requirements),
            missing=missing,
            evidence_types=evidence_types,
            export_case=case_view,
        )
