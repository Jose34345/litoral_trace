"""Local EUDR API V3 DDS candidate and fail-closed conformance service.

The service deliberately stops before any legal submission.  It reconstructs
source plots from the current Shipment genealogy, validates the local Annex-II
profile, and produces a deterministic candidate payload/hash suitable for a
later ACCEPTANCE adapter.  ACCEPTANCE is a non-legal test environment; LIVE is
outside P1-D.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

from shapely.geometry import mapping
from shapely import wkt as shapely_wkt
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from litoral_trace.db.models import EudrDdsCandidate, Shipment
from litoral_trace.db.models.eudr_dds_candidate import (
    EUDR_ACTIVITY_TYPES,
    EUDR_COMMODITY_PROFILES,
    EUDR_RISK_CONCLUSIONS,
)
from litoral_trace.services.traceability_lineage import (
    TraceabilityLineageError,
    TraceabilityLineageService,
)


EUDR_SPEC_PROFILE = "EUDR_OPERATOR_API_V3_2026-06_LOCAL_CONFORMANCE"
EUDR_LOCAL_PAYLOAD_PROFILE = "LITORAL_TRACE_EUDR_DDS_CANDIDATE_V1"
EUDR_TARGET_ENVIRONMENT = "ACCEPTANCE"

# This fingerprint describes the reviewed local conformance profile, not a
# claim that Litoral Trace embeds or reproduces the Commission's XSD verbatim.
# If the official API reference changes, update this descriptor and candidates
# persisted under the previous fingerprint will fail closed as stale.
_EUDR_SPEC_DESCRIPTOR = {
    "source": "EU EUDR Information System operator API V3 reference, 2026-06",
    "environment": EUDR_TARGET_ENVIRONMENT,
    "legal_effect": "NONE",
    "required_local_sections": [
        "operator_identity",
        "activity",
        "hs_and_product_description",
        "quantity_net_mass_kg",
        "country_and_dates_of_production",
        "all_source_plots_geolocation",
        "wood_species_when_applicable",
        "previous_dds_when_relied_upon",
        "explicit_due_diligence_risk_conclusion",
    ],
    "geolocation_rule": "polygon_when_available; point_only_for_plot_area_ha_lte_4",
    "risk_ready_value": "NO_OR_NEGLIGIBLE_RISK",
}
EUDR_SPEC_FINGERPRINT_SHA256 = hashlib.sha256(
    json.dumps(
        _EUDR_SPEC_DESCRIPTOR,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
).hexdigest()

_HS_RE = re.compile(r"^[0-9]{4,10}$")
_COUNTRY_RE = re.compile(r"^[A-Z]{2}$")


class EudrDdsCandidateError(RuntimeError):
    """Base safe domain error."""


class EudrDdsCandidateNotFoundError(EudrDdsCandidateError):
    pass


class EudrDdsCandidateValidationError(EudrDdsCandidateError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail)


class EudrDdsCandidatePersistenceError(EudrDdsCandidateError):
    pass


@dataclass(frozen=True)
class EudrCandidateView:
    public_id: UUID
    shipment_public_id: UUID
    shipment_code: str
    activity_type: str
    commodity_profile: str
    operator_name: str | None
    operator_address: str | None
    operator_country_code: str | None
    operator_eori: str | None
    hs_code: str | None
    trade_name: str | None
    product_description: str | None
    common_species_name: str | None
    scientific_species_name: str | None
    net_mass_kg: str | None
    production_country_code: str | None
    production_date_from: date | None
    production_date_to: date | None
    relies_on_previous_dds: bool
    previous_dds_reference: str | None
    previous_dds_verification: str | None
    risk_conclusion: str
    risk_assessment_reference: str | None
    risk_assessed_at: datetime | None
    spec_profile: str
    spec_fingerprint_sha256: str
    notes: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class EudrRequirementView:
    key: str
    label: str
    satisfied: bool
    source: str
    detail: str | None = None


@dataclass(frozen=True)
class EudrDdsConformanceView:
    shipment_public_id: UUID
    shipment_code: str
    state: str
    ready: bool
    requirements: tuple[EudrRequirementView, ...]
    missing: tuple[str, ...]
    candidate: EudrCandidateView | None
    plots: tuple[dict[str, Any], ...]
    lineage_complete: bool
    payload: dict[str, Any] | None
    payload_sha256: str | None
    target_environment: str = EUDR_TARGET_ENVIRONMENT
    legal_effect: str = "NONE_LOCAL_OR_ACCEPTANCE_CANDIDATE"


def _optional_text(value: Any, *, maximum: int) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    if len(normalized) > maximum:
        raise EudrDdsCandidateValidationError(
            "TEXT_TOO_LONG",
            f"El texto supera el máximo de {maximum} caracteres.",
        )
    return normalized


def _enum(value: Any, allowed: frozenset[str], *, code: str, label: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in allowed:
        raise EudrDdsCandidateValidationError(
            code,
            f"{label} no permitido.",
        )
    return normalized


def _country(value: Any) -> str | None:
    normalized = _optional_text(value, maximum=2)
    if normalized is None:
        return None
    normalized = normalized.upper()
    if not _COUNTRY_RE.fullmatch(normalized):
        raise EudrDdsCandidateValidationError(
            "INVALID_COUNTRY_CODE",
            "El país debe informarse como código ISO alpha-2.",
        )
    return normalized


def _hs_code(value: Any) -> str | None:
    normalized = _optional_text(value, maximum=16)
    if normalized is None:
        return None
    normalized = re.sub(r"[\s.-]", "", normalized)
    if not _HS_RE.fullmatch(normalized):
        raise EudrDdsCandidateValidationError(
            "INVALID_HS_CODE",
            "El código HS/CN debe contener entre 4 y 10 dígitos.",
        )
    return normalized


def _positive_decimal(value: Any) -> Decimal | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        normalized = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise EudrDdsCandidateValidationError(
            "INVALID_NET_MASS",
            "La masa neta debe ser numérica.",
        ) from exc
    if not normalized.is_finite() or normalized <= 0:
        raise EudrDdsCandidateValidationError(
            "INVALID_NET_MASS",
            "La masa neta debe ser mayor que cero.",
        )
    return normalized.quantize(Decimal("0.001"))


def _round_coordinates(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return [_round_coordinates(item) for item in value]
    if isinstance(value, float):
        return round(value, 6)
    return value


def _plot_geojson(lote: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    polygon_wkt = str(lote.get("polygon_wkt") or "").strip()
    if polygon_wkt:
        try:
            geometry = shapely_wkt.loads(polygon_wkt)
        except Exception:
            return None, "INVALID_POLYGON_WKT"
        if geometry.is_empty or not geometry.is_valid:
            return None, "INVALID_POLYGON_GEOMETRY"
        if geometry.geom_type not in {"Polygon", "MultiPolygon"}:
            return None, "NON_AREA_GEOMETRY"
        geojson = mapping(geometry)
        return {
            "type": geojson["type"],
            "coordinates": _round_coordinates(geojson["coordinates"]),
        }, None

    try:
        hectares = float(lote.get("hectareas"))
        lat = float(lote.get("latitud"))
        lon = float(lote.get("longitud"))
    except (TypeError, ValueError):
        return None, "MISSING_GEOLOCATION"

    # EUDR geolocation permits a point for a production plot of no more than
    # four hectares; larger plots fail closed without polygon geometry.
    if hectares <= 4.0 and -90 <= lat <= 90 and -180 <= lon <= 180:
        return {
            "type": "Point",
            "coordinates": [round(lon, 6), round(lat, 6)],
        }, None
    return None, "POLYGON_REQUIRED_FOR_PLOT_OVER_4_HA"


def _canonicalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _canonicalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonicalize(item) for item in value]
    return value


def canonical_payload_sha256(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            _canonicalize(payload),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()


class EudrDdsCandidateService:
    """Create local candidates and recompute conformance from real genealogy."""

    def __init__(self, *, session: Session, organization_id: int) -> None:
        self._session = session
        self._organization_id = int(organization_id)

    def _shipment(self, shipment_code: str) -> Shipment:
        normalized = str(shipment_code or "").strip()
        if not normalized:
            raise EudrDdsCandidateValidationError(
                "SHIPMENT_CODE_REQUIRED",
                "El código de despacho es obligatorio.",
            )
        shipment = self._session.scalar(
            select(Shipment).where(
                Shipment.organization_id == self._organization_id,
                Shipment.shipment_code == normalized,
            )
        )
        if shipment is None:
            raise EudrDdsCandidateNotFoundError(
                "El despacho no existe en la organización activa."
            )
        return shipment

    @staticmethod
    def _view(candidate: EudrDdsCandidate, shipment: Shipment) -> EudrCandidateView:
        return EudrCandidateView(
            public_id=candidate.public_id,
            shipment_public_id=shipment.public_id,
            shipment_code=shipment.shipment_code,
            activity_type=candidate.activity_type,
            commodity_profile=candidate.commodity_profile,
            operator_name=candidate.operator_name,
            operator_address=candidate.operator_address,
            operator_country_code=candidate.operator_country_code,
            operator_eori=candidate.operator_eori,
            hs_code=candidate.hs_code,
            trade_name=candidate.trade_name,
            product_description=candidate.product_description,
            common_species_name=candidate.common_species_name,
            scientific_species_name=candidate.scientific_species_name,
            net_mass_kg=(
                format(candidate.net_mass_kg, "f")
                if candidate.net_mass_kg is not None
                else None
            ),
            production_country_code=candidate.production_country_code,
            production_date_from=candidate.production_date_from,
            production_date_to=candidate.production_date_to,
            relies_on_previous_dds=bool(candidate.relies_on_previous_dds),
            previous_dds_reference=candidate.previous_dds_reference,
            previous_dds_verification=candidate.previous_dds_verification,
            risk_conclusion=candidate.risk_conclusion,
            risk_assessment_reference=candidate.risk_assessment_reference,
            risk_assessed_at=candidate.risk_assessed_at,
            spec_profile=candidate.spec_profile,
            spec_fingerprint_sha256=candidate.spec_fingerprint_sha256,
            notes=candidate.notes,
            created_at=candidate.created_at,
            updated_at=candidate.updated_at,
        )

    def upsert_candidate(
        self,
        *,
        shipment_code: str,
        activity_type: str,
        commodity_profile: str,
        operator_name: Any = None,
        operator_address: Any = None,
        operator_country_code: Any = None,
        operator_eori: Any = None,
        hs_code: Any = None,
        trade_name: Any = None,
        product_description: Any = None,
        common_species_name: Any = None,
        scientific_species_name: Any = None,
        net_mass_kg: Any = None,
        production_country_code: Any = None,
        production_date_from: date | None = None,
        production_date_to: date | None = None,
        relies_on_previous_dds: bool = False,
        previous_dds_reference: Any = None,
        previous_dds_verification: Any = None,
        risk_conclusion: str = "UNASSESSED",
        risk_assessment_reference: Any = None,
        risk_assessed_at: datetime | None = None,
        notes: Any = None,
        actor_user_id: int | None = None,
    ) -> EudrCandidateView:
        shipment = self._shipment(shipment_code)
        activity = _enum(
            activity_type,
            EUDR_ACTIVITY_TYPES,
            code="INVALID_ACTIVITY_TYPE",
            label="Actividad EUDR",
        )
        commodity = _enum(
            commodity_profile,
            EUDR_COMMODITY_PROFILES,
            code="INVALID_COMMODITY_PROFILE",
            label="Perfil de commodity",
        )
        risk = _enum(
            risk_conclusion,
            EUDR_RISK_CONCLUSIONS,
            code="INVALID_RISK_CONCLUSION",
            label="Conclusión de riesgo",
        )
        if (
            production_date_from
            and production_date_to
            and production_date_to < production_date_from
        ):
            raise EudrDdsCandidateValidationError(
                "INVALID_PRODUCTION_DATE_RANGE",
                "La fecha final de producción no puede ser anterior a la inicial.",
            )

        candidate = self._session.scalar(
            select(EudrDdsCandidate).where(
                EudrDdsCandidate.organization_id == self._organization_id,
                EudrDdsCandidate.shipment_id == shipment.id,
            )
        )
        if candidate is None:
            candidate = EudrDdsCandidate(
                organization_id=self._organization_id,
                shipment_id=int(shipment.id),
                activity_type=activity,
                commodity_profile=commodity,
                spec_profile=EUDR_SPEC_PROFILE,
                spec_fingerprint_sha256=EUDR_SPEC_FINGERPRINT_SHA256,
                created_by_user_id=actor_user_id,
                updated_by_user_id=actor_user_id,
            )
            self._session.add(candidate)
        else:
            candidate.activity_type = activity
            candidate.commodity_profile = commodity
            candidate.spec_profile = EUDR_SPEC_PROFILE
            candidate.spec_fingerprint_sha256 = EUDR_SPEC_FINGERPRINT_SHA256
            candidate.updated_by_user_id = actor_user_id

        candidate.operator_name = _optional_text(operator_name, maximum=240)
        candidate.operator_address = _optional_text(operator_address, maximum=2000)
        candidate.operator_country_code = _country(operator_country_code)
        candidate.operator_eori = _optional_text(operator_eori, maximum=32)
        candidate.hs_code = _hs_code(hs_code)
        candidate.trade_name = _optional_text(trade_name, maximum=240)
        candidate.product_description = _optional_text(product_description, maximum=4000)
        candidate.common_species_name = _optional_text(common_species_name, maximum=240)
        candidate.scientific_species_name = _optional_text(
            scientific_species_name,
            maximum=240,
        )
        candidate.net_mass_kg = _positive_decimal(net_mass_kg)
        candidate.production_country_code = _country(production_country_code)
        candidate.production_date_from = production_date_from
        candidate.production_date_to = production_date_to
        candidate.relies_on_previous_dds = bool(relies_on_previous_dds)
        candidate.previous_dds_reference = _optional_text(
            previous_dds_reference,
            maximum=160,
        )
        candidate.previous_dds_verification = _optional_text(
            previous_dds_verification,
            maximum=160,
        )
        candidate.risk_conclusion = risk
        candidate.risk_assessment_reference = _optional_text(
            risk_assessment_reference,
            maximum=240,
        )
        candidate.risk_assessed_at = risk_assessed_at
        candidate.notes = _optional_text(notes, maximum=4000)

        try:
            self._session.flush()
            view = self._view(candidate, shipment)
            self._session.commit()
            return view
        except IntegrityError as exc:
            self._session.rollback()
            raise EudrDdsCandidateValidationError(
                "EUDR_CANDIDATE_CONFLICT",
                "No fue posible guardar el candidato DDS por un conflicto de datos.",
            ) from exc
        except SQLAlchemyError as exc:
            self._session.rollback()
            raise EudrDdsCandidatePersistenceError(
                "No fue posible guardar el candidato DDS."
            ) from exc

    @staticmethod
    def _requirement(
        key: str,
        label: str,
        satisfied: bool,
        source: str,
        detail: str | None = None,
    ) -> EudrRequirementView:
        return EudrRequirementView(
            key=key,
            label=label,
            satisfied=bool(satisfied),
            source=source,
            detail=detail,
        )

    def _lineage(self, shipment_code: str) -> dict[str, Any] | None:
        try:
            return TraceabilityLineageService(
                session=self._session,
                organization_id=self._organization_id,
            ).trace_shipment(shipment_code)
        except TraceabilityLineageError:
            return None

    @staticmethod
    def _plots(lineage: dict[str, Any] | None) -> tuple[dict[str, Any], ...]:
        if not lineage:
            return ()
        plots: list[dict[str, Any]] = []
        for source in lineage.get("source_lotes", ()):
            lote = dict(source.get("lote") or {})
            geojson, geometry_error = _plot_geojson(lote)
            plots.append(
                {
                    "parcel_identifier": lote.get("identificador"),
                    "producer_reference": lote.get("productor_id"),
                    "product": lote.get("producto_forestal"),
                    "area_ha": lote.get("hectareas"),
                    "attributed_shipment_quantity": source.get(
                        "attributed_shipment_quantity"
                    ),
                    "unit": source.get("unit"),
                    "geojson": geojson,
                    "geometry_error": geometry_error,
                }
            )
        return tuple(plots)

    @staticmethod
    def _payload(
        *,
        candidate: EudrCandidateView,
        lineage: dict[str, Any],
        plots: tuple[dict[str, Any], ...],
    ) -> dict[str, Any]:
        shipment = dict(lineage["shipment"])
        return {
            "profile": EUDR_LOCAL_PAYLOAD_PROFILE,
            "legal_effect": "NONE_LOCAL_CANDIDATE",
            "target": {
                "system": "EU_EUDR_INFORMATION_SYSTEM",
                "environment": EUDR_TARGET_ENVIRONMENT,
                "api_family": "V3",
                "spec_profile": candidate.spec_profile,
                "spec_fingerprint_sha256": candidate.spec_fingerprint_sha256,
            },
            "shipment": {
                "public_id": str(candidate.shipment_public_id),
                "shipment_code": candidate.shipment_code,
                "destination_country": shipment.get("destination_country"),
                "lineage_state": shipment.get("lineage_state"),
            },
            "activity_type": candidate.activity_type,
            "operator": {
                "name": candidate.operator_name,
                "address": candidate.operator_address,
                "country_code": candidate.operator_country_code,
                "eori": candidate.operator_eori,
            },
            "product": {
                "commodity_profile": candidate.commodity_profile,
                "hs_code": candidate.hs_code,
                "trade_name": candidate.trade_name,
                "description": candidate.product_description,
                "common_species_name": candidate.common_species_name,
                "scientific_species_name": candidate.scientific_species_name,
                "net_mass_kg": candidate.net_mass_kg,
            },
            "production": {
                "country_code": candidate.production_country_code,
                "date_from": candidate.production_date_from,
                "date_to": candidate.production_date_to,
                "plots": plots,
            },
            "previous_dds": {
                "relied_upon": candidate.relies_on_previous_dds,
                "reference": candidate.previous_dds_reference,
                "verification": candidate.previous_dds_verification,
            },
            "due_diligence": {
                "risk_conclusion": candidate.risk_conclusion,
                "risk_assessment_reference": candidate.risk_assessment_reference,
                "risk_assessed_at": candidate.risk_assessed_at,
                "automatic_compliance_claim": False,
            },
        }

    def conformance(self, shipment_code: str) -> EudrDdsConformanceView:
        shipment = self._shipment(shipment_code)
        candidate_row = self._session.scalar(
            select(EudrDdsCandidate).where(
                EudrDdsCandidate.organization_id == self._organization_id,
                EudrDdsCandidate.shipment_id == shipment.id,
            )
        )
        candidate = (
            self._view(candidate_row, shipment) if candidate_row is not None else None
        )
        lineage = self._lineage(shipment.shipment_code)
        plots = self._plots(lineage)
        requirements: list[EudrRequirementView] = []

        requirements.append(
            self._requirement(
                "DDS_CANDIDATE",
                "Candidato DDS local configurado",
                candidate is not None,
                "Litoral Trace",
            )
        )
        lineage_complete = bool(lineage and lineage.get("complete"))
        requirements.append(
            self._requirement(
                "LINEAGE_COMPLETE",
                "Genealogía completa del despacho",
                lineage_complete,
                "Cadena de custodia",
                None if lineage_complete else "El origen no puede reconstruirse sin brechas.",
            )
        )
        requirements.append(
            self._requirement(
                "SOURCE_PLOTS",
                "Al menos una parcela de producción atribuida",
                bool(plots),
                "Genealogía",
            )
        )
        invalid_plots = [
            str(plot.get("parcel_identifier") or "sin identificador")
            for plot in plots
            if plot.get("geojson") is None
        ]
        requirements.append(
            self._requirement(
                "ALL_PLOTS_GEOLOCATED",
                "Geolocalización válida de todas las parcelas de producción",
                bool(plots) and not invalid_plots,
                "Lotes / geometría",
                (
                    None
                    if not invalid_plots
                    else "Sin geometría EUDR utilizable: " + ", ".join(invalid_plots)
                ),
            )
        )

        if candidate is not None:
            destination = str(
                (lineage or {}).get("shipment", {}).get("destination_country")
                or shipment.destination_country
                or ""
            ).strip().upper()
            requirements.extend(
                [
                    self._requirement(
                        "SHIPMENT_DESTINATION",
                        "País de destino del despacho",
                        len(destination) == 2,
                        "Despacho",
                    ),
                    self._requirement(
                        "OPERATOR_NAME",
                        "Nombre del operador EUDR responsable",
                        bool(candidate.operator_name),
                        "Operador UE",
                    ),
                    self._requirement(
                        "OPERATOR_ADDRESS",
                        "Domicilio del operador EUDR",
                        bool(candidate.operator_address),
                        "Operador UE",
                    ),
                    self._requirement(
                        "OPERATOR_COUNTRY",
                        "País del operador EUDR",
                        bool(candidate.operator_country_code),
                        "Operador UE",
                    ),
                    self._requirement(
                        "OPERATOR_EORI",
                        "EORI del operador cuando la actividad es importación",
                        (
                            bool(candidate.operator_eori)
                            if candidate.activity_type == "IMPORT"
                            else True
                        ),
                        "Operador UE / aduana",
                    ),
                    self._requirement(
                        "HS_CODE",
                        "Código HS/CN explícito",
                        bool(candidate.hs_code),
                        "Producto",
                    ),
                    self._requirement(
                        "TRADE_NAME",
                        "Nombre comercial del producto",
                        bool(candidate.trade_name),
                        "Producto",
                    ),
                    self._requirement(
                        "PRODUCT_DESCRIPTION",
                        "Descripción suficiente del producto",
                        bool(candidate.product_description),
                        "Producto",
                    ),
                    self._requirement(
                        "NET_MASS_KG",
                        "Masa neta en kg",
                        bool(candidate.net_mass_kg),
                        "Cantidad",
                    ),
                    self._requirement(
                        "PRODUCTION_COUNTRY",
                        "País de producción",
                        bool(candidate.production_country_code),
                        "Origen",
                    ),
                    self._requirement(
                        "PRODUCTION_DATES",
                        "Fecha o rango de producción",
                        bool(
                            candidate.production_date_from
                            and candidate.production_date_to
                        ),
                        "Origen",
                    ),
                    self._requirement(
                        "WOOD_COMMON_SPECIES",
                        "Nombre común de la especie de madera",
                        (
                            bool(candidate.common_species_name)
                            if candidate.commodity_profile == "WOOD"
                            else True
                        ),
                        "Producto",
                    ),
                    self._requirement(
                        "WOOD_SCIENTIFIC_SPECIES",
                        "Nombre científico completo de la especie de madera",
                        (
                            bool(candidate.scientific_species_name)
                            if candidate.commodity_profile == "WOOD"
                            else True
                        ),
                        "Producto",
                    ),
                    self._requirement(
                        "PREVIOUS_DDS_REFERENCE",
                        "Referencia DDS previa cuando se declara dependencia",
                        (
                            bool(candidate.previous_dds_reference)
                            if candidate.relies_on_previous_dds
                            else True
                        ),
                        "DDS previa",
                    ),
                    self._requirement(
                        "RISK_CONCLUSION",
                        "Conclusión de riesgo nulo o despreciable",
                        candidate.risk_conclusion == "NO_OR_NEGLIGIBLE_RISK",
                        "Debida diligencia",
                        (
                            "Una conclusión UNASSESSED o NON_NEGLIGIBLE_RISK bloquea el candidato."
                            if candidate.risk_conclusion != "NO_OR_NEGLIGIBLE_RISK"
                            else None
                        ),
                    ),
                    self._requirement(
                        "RISK_ASSESSMENT_REFERENCE",
                        "Referencia auditable de evaluación de riesgo",
                        bool(candidate.risk_assessment_reference),
                        "Debida diligencia",
                    ),
                    self._requirement(
                        "RISK_ASSESSED_AT",
                        "Fecha de cierre de evaluación de riesgo",
                        candidate.risk_assessed_at is not None,
                        "Debida diligencia",
                    ),
                    self._requirement(
                        "SPEC_PROFILE_CURRENT",
                        "Perfil local alineado a la especificación API V3 revisada",
                        (
                            candidate.spec_profile == EUDR_SPEC_PROFILE
                            and candidate.spec_fingerprint_sha256
                            == EUDR_SPEC_FINGERPRINT_SHA256
                        ),
                        "Conformance",
                    ),
                ]
            )

        missing = tuple(
            requirement.key for requirement in requirements if not requirement.satisfied
        )
        ready = candidate is not None and not missing
        payload: dict[str, Any] | None = None
        payload_sha256: str | None = None
        if candidate is not None and lineage is not None:
            payload = self._payload(
                candidate=candidate,
                lineage=lineage,
                plots=plots,
            )
            payload = _canonicalize(payload)
            payload_sha256 = canonical_payload_sha256(payload)

        return EudrDdsConformanceView(
            shipment_public_id=shipment.public_id,
            shipment_code=shipment.shipment_code,
            state=(
                "CONFORMANCE_READY"
                if ready
                else ("DRAFT" if candidate is None else "BLOCKED")
            ),
            ready=ready,
            requirements=tuple(requirements),
            missing=missing,
            candidate=candidate,
            plots=plots,
            lineage_complete=lineage_complete,
            payload=payload,
            payload_sha256=payload_sha256,
        )
