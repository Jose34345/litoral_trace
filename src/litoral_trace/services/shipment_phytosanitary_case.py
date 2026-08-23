"""SENASA/CERT-POV/ePhyto metadata and deterministic shipment readiness."""
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
    ShipmentPhytosanitaryCase,
    TraceabilityEvidenceLink,
    VaultDocument,
)
from litoral_trace.db.models.shipment_phytosanitary_case import (
    PHYTOSANITARY_CERTIFICATION_MODES,
)


class ShipmentPhytosanitaryCaseError(RuntimeError):
    """Base safe error for phytosanitary workflows."""


class ShipmentPhytosanitaryCaseNotFoundError(ShipmentPhytosanitaryCaseError):
    pass


class ShipmentPhytosanitaryCaseValidationError(ShipmentPhytosanitaryCaseError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail)


class ShipmentPhytosanitaryCasePersistenceError(ShipmentPhytosanitaryCaseError):
    pass


@dataclass(frozen=True)
class ShipmentPhytosanitaryCaseView:
    public_id: UUID
    shipment_public_id: UUID
    shipment_code: str
    certification_mode: str
    requirements_reference: str | None
    requirements_checked_at: datetime | None
    cert_pov_reference: str | None
    certificate_number: str | None
    ephyto_reference: str | None
    notes: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class PhytosanitaryRequirementView:
    key: str
    label: str
    satisfied: bool
    source: str


@dataclass(frozen=True)
class ShipmentPhytosanitaryReadinessView:
    shipment_public_id: UUID
    shipment_code: str
    state: str
    certification_mode: str | None
    requirements: tuple[PhytosanitaryRequirementView, ...]
    missing: tuple[str, ...]
    evidence_types: tuple[str, ...]
    phytosanitary_case: ShipmentPhytosanitaryCaseView | None

    @property
    def ready(self) -> bool:
        return self.state == "READY"


def _optional_text(value: Any, *, maximum: int, uppercase: bool = False) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    if len(normalized) > maximum:
        raise ShipmentPhytosanitaryCaseValidationError(
            "TEXT_TOO_LONG",
            f"El valor supera el máximo de {maximum} caracteres.",
        )
    return normalized.upper() if uppercase else normalized


def _normalize_mode(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in PHYTOSANITARY_CERTIFICATION_MODES:
        raise ShipmentPhytosanitaryCaseValidationError(
            "INVALID_CERTIFICATION_MODE",
            "El modo debe ser UNASSESSED, NOT_REQUIRED, PAPER o EPHYTO.",
        )
    return normalized


class ShipmentPhytosanitaryCaseService:
    """Manage one phytosanitary case per shipment without issuing certificates."""

    def __init__(self, *, session: Session, organization_id: int) -> None:
        self._session = session
        self._organization_id = int(organization_id)

    def _shipment(self, shipment_code: str) -> Shipment:
        code = str(shipment_code or "").strip()
        if not code or len(code) > 120:
            raise ShipmentPhytosanitaryCaseValidationError(
                "INVALID_SHIPMENT_CODE", "Debe informar un código de despacho válido."
            )
        shipment = self._session.scalar(
            select(Shipment).where(
                Shipment.organization_id == self._organization_id,
                func.lower(Shipment.shipment_code) == code.lower(),
            )
        )
        if shipment is None:
            raise ShipmentPhytosanitaryCaseNotFoundError(
                "El despacho no existe en la organización activa."
            )
        return shipment

    @staticmethod
    def _view(
        case: ShipmentPhytosanitaryCase, shipment: Shipment
    ) -> ShipmentPhytosanitaryCaseView:
        return ShipmentPhytosanitaryCaseView(
            public_id=case.public_id,
            shipment_public_id=shipment.public_id,
            shipment_code=shipment.shipment_code,
            certification_mode=case.certification_mode,
            requirements_reference=case.requirements_reference,
            requirements_checked_at=case.requirements_checked_at,
            cert_pov_reference=case.cert_pov_reference,
            certificate_number=case.certificate_number,
            ephyto_reference=case.ephyto_reference,
            notes=case.notes,
            created_at=case.created_at,
            updated_at=case.updated_at,
        )

    def upsert_case(
        self,
        *,
        shipment_code: str,
        certification_mode: str,
        requirements_reference: Any = None,
        requirements_checked_at: datetime | None = None,
        cert_pov_reference: Any = None,
        certificate_number: Any = None,
        ephyto_reference: Any = None,
        notes: Any = None,
        actor_user_id: int | None = None,
    ) -> ShipmentPhytosanitaryCaseView:
        shipment = self._shipment(shipment_code)
        mode = _normalize_mode(certification_mode)
        case = self._session.scalar(
            select(ShipmentPhytosanitaryCase).where(
                ShipmentPhytosanitaryCase.organization_id == self._organization_id,
                ShipmentPhytosanitaryCase.shipment_id == shipment.id,
            )
        )
        if case is None:
            case = ShipmentPhytosanitaryCase(
                organization_id=self._organization_id,
                shipment_id=int(shipment.id),
                certification_mode=mode,
                created_by_user_id=actor_user_id,
                updated_by_user_id=actor_user_id,
            )
            self._session.add(case)
        else:
            case.certification_mode = mode
            case.updated_by_user_id = actor_user_id

        case.requirements_reference = _optional_text(
            requirements_reference, maximum=500
        )
        case.requirements_checked_at = requirements_checked_at
        case.cert_pov_reference = _optional_text(cert_pov_reference, maximum=120)
        case.certificate_number = _optional_text(certificate_number, maximum=120)
        case.ephyto_reference = _optional_text(ephyto_reference, maximum=160)
        case.notes = _optional_text(notes, maximum=2000)

        try:
            self._session.flush()
            result = self._view(case, shipment)
            self._session.commit()
            return result
        except IntegrityError as exc:
            self._session.rollback()
            raise ShipmentPhytosanitaryCaseValidationError(
                "PHYTOSANITARY_CASE_CONFLICT",
                "No fue posible guardar la evaluación fitosanitaria por un conflicto de referencias.",
            ) from exc
        except SQLAlchemyError as exc:
            self._session.rollback()
            raise ShipmentPhytosanitaryCasePersistenceError(
                "No fue posible guardar la evaluación fitosanitaria."
            ) from exc

    def _active_evidence_types(self, shipment: Shipment) -> tuple[str, ...]:
        rows = self._session.scalars(
            select(TraceabilityEvidenceLink.evidence_type)
            .join(
                VaultDocument,
                and_(
                    VaultDocument.id == TraceabilityEvidenceLink.vault_document_id,
                    VaultDocument.organization_id
                    == TraceabilityEvidenceLink.organization_id,
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
        key: str, label: str, satisfied: bool, source: str
    ) -> PhytosanitaryRequirementView:
        return PhytosanitaryRequirementView(
            key=key, label=label, satisfied=bool(satisfied), source=source
        )

    def readiness(self, shipment_code: str) -> ShipmentPhytosanitaryReadinessView:
        shipment = self._shipment(shipment_code)
        case_row = self._session.scalar(
            select(ShipmentPhytosanitaryCase).where(
                ShipmentPhytosanitaryCase.organization_id == self._organization_id,
                ShipmentPhytosanitaryCase.shipment_id == shipment.id,
            )
        )
        evidence_types = self._active_evidence_types(shipment)
        evidence = set(evidence_types)
        requirements: list[PhytosanitaryRequirementView] = []

        if case_row is None or case_row.certification_mode == "UNASSESSED":
            requirements.append(
                self._requirement(
                    "PHYTOSANITARY_ASSESSMENT",
                    "Evaluar requisito fitosanitario del destino",
                    False,
                    "SENASA / ONPF destino",
                )
            )
            mode = case_row.certification_mode if case_row is not None else None
            case_view = self._view(case_row, shipment) if case_row is not None else None
        else:
            mode = case_row.certification_mode
            case_view = self._view(case_row, shipment)
            requirements.extend(
                [
                    self._requirement(
                        "REQUIREMENTS_REFERENCE",
                        "Referencia oficial de requisitos del país de destino",
                        bool(case_row.requirements_reference),
                        "SENASA / ONPF destino",
                    ),
                    self._requirement(
                        "REQUIREMENTS_CHECKED_AT",
                        "Fecha de evaluación de requisitos",
                        case_row.requirements_checked_at is not None,
                        "Evaluación",
                    ),
                ]
            )

            if mode in {"PAPER", "EPHYTO"}:
                requirements.extend(
                    [
                        self._requirement(
                            "CERT_POV_REFERENCE",
                            "Referencia de trámite CERT-POV",
                            bool(case_row.cert_pov_reference),
                            "SENASA CERT-POV",
                        ),
                        self._requirement(
                            "PHYTOSANITARY_CERTIFICATE_NUMBER",
                            "Número del certificado fitosanitario",
                            bool(case_row.certificate_number),
                            "SENASA",
                        ),
                    ]
                )

            if mode == "PAPER":
                requirements.append(
                    self._requirement(
                        "PHYTOSANITARY_CERTIFICATE_EVIDENCE",
                        "Certificado fitosanitario disponible en Vault",
                        "PHYTOSANITARY_CERTIFICATE" in evidence,
                        "Vault",
                    )
                )
            elif mode == "EPHYTO":
                requirements.extend(
                    [
                        self._requirement(
                            "EPHYTO_REFERENCE",
                            "Referencia de intercambio ePhyto",
                            bool(case_row.ephyto_reference),
                            "ePhyto",
                        ),
                        self._requirement(
                            "EPHYTO_XML_EVIDENCE",
                            "XML ePhyto disponible en Vault",
                            "EPHYTO_XML" in evidence,
                            "Vault",
                        ),
                    ]
                )

        missing = tuple(item.key for item in requirements if not item.satisfied)
        state = "READY" if requirements and not missing else "BLOCKED"
        return ShipmentPhytosanitaryReadinessView(
            shipment_public_id=shipment.public_id,
            shipment_code=shipment.shipment_code,
            state=state,
            certification_mode=mode,
            requirements=tuple(requirements),
            missing=missing,
            evidence_types=evidence_types,
            phytosanitary_case=case_view,
        )
