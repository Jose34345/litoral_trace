"""Deterministic DDS V3 ACCEPTANCE wire-body contract.

The mapping follows the current Operator API V3 contract reviewed for P1-D2.
WS-Security is intentionally kept in the transport module because nonce and
Timestamp are per-send values and therefore must not affect idempotency.
"""
from __future__ import annotations

import base64
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any
import xml.etree.ElementTree as ET


DDS_V3_NAMESPACE = "http://ec.europa.eu/tracesnt/certificate/eudr/due-diligence-statement/v3"
EUDR_COMMON_V3_NAMESPACE = "http://ec.europa.eu/tracesnt/certificate/eudr/common/v3"
SOAP_ACTION_SUBMIT_DDS = f"{DDS_V3_NAMESPACE}/submitDds"
WIRE_CONTRACT_PROFILE = "EUDR_OPERATOR_API_V3_2026-05-29_DDS_SUBMIT"

_WIRE_DESCRIPTOR = {
    "documentation_version": "1.0",
    "api_specification": "V3",
    "release_date": "2026-05-29",
    "service": "EUDRDueDiligenceStatementServiceV3",
    "operation": "SubmitDdsRequest",
    "dds_namespace": DDS_V3_NAMESPACE,
    "common_namespace": EUDR_COMMON_V3_NAMESPACE,
    "soap_action": SOAP_ACTION_SUBMIT_DDS,
    "operator_roles_supported_by_litoral_trace": ["OPERATOR"],
    "legacy_associated_statements_mapping": "FAIL_CLOSED",
}
WIRE_CONTRACT_SHA256 = hashlib.sha256(
    json.dumps(_WIRE_DESCRIPTOR, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()

_HS_V3_RE = re.compile(r"^[0-9]{2,6}$")
_COUNTRY_RE = re.compile(r"^[A-Z]{2}$")

ET.register_namespace("dds", DDS_V3_NAMESPACE)
ET.register_namespace("eudrCommon", EUDR_COMMON_V3_NAMESPACE)


class EudrAcceptanceContractError(ValueError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail)


@dataclass(frozen=True)
class EudrV3PreparedBody:
    xml: bytes
    sha256: str
    byte_length: int
    operator_role: str
    country_of_activity: str
    border_cross_country: str | None
    internal_reference_number: str
    wire_contract_profile: str = WIRE_CONTRACT_PROFILE
    wire_contract_sha256: str = WIRE_CONTRACT_SHA256


def _tag(namespace: str, local: str) -> str:
    return f"{{{namespace}}}{local}"


def _text(parent: ET.Element, namespace: str, name: str, value: Any) -> ET.Element:
    element = ET.SubElement(parent, _tag(namespace, name))
    element.text = str(value)
    return element


def _country(value: Any, *, field: str, required: bool = True) -> str | None:
    normalized = str(value or "").strip().upper()
    if not normalized:
        if required:
            raise EudrAcceptanceContractError(
                f"{field.upper()}_REQUIRED",
                f"{field} es obligatorio para preparar el request V3.",
            )
        return None
    if not _COUNTRY_RE.fullmatch(normalized):
        raise EudrAcceptanceContractError(
            f"INVALID_{field.upper()}",
            f"{field} debe ser un código ISO alpha-2.",
        )
    return normalized


def _geojson_base64(plot: dict[str, Any], production_country: str) -> str:
    geometry = plot.get("geojson")
    if not isinstance(geometry, dict) or not geometry.get("type"):
        raise EudrAcceptanceContractError(
            "PLOT_GEOJSON_REQUIRED",
            "Todas las parcelas deben tener GeoJSON válido antes de preparar ACCEPTANCE.",
        )

    properties: dict[str, Any] = {"ProducerCountry": production_country}
    parcel = str(plot.get("parcel_identifier") or "").strip()
    if parcel:
        properties["ProductionPlace"] = parcel
    if geometry.get("type") == "Point":
        area = plot.get("area_ha")
        if area is not None:
            properties["Area"] = area

    feature_collection = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": properties,
                "geometry": geometry,
            }
        ],
    }
    encoded = json.dumps(
        feature_collection,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return base64.b64encode(encoded).decode("ascii")


def build_submit_dds_body(
    candidate_payload: dict[str, Any],
    *,
    operator_role: str,
    country_of_activity: str,
    border_cross_country: str | None = None,
    internal_reference_number: str | None = None,
    geo_location_confidential: bool = False,
) -> EudrV3PreparedBody:
    """Map one conformance-ready local candidate to deterministic DDS V3 XML."""

    target = dict(candidate_payload.get("target") or {})
    if target.get("environment") != "ACCEPTANCE" or target.get("api_family") != "V3":
        raise EudrAcceptanceContractError(
            "LOCAL_CANDIDATE_TARGET_MISMATCH",
            "El candidato local no está dirigido a EUDR API V3 ACCEPTANCE.",
        )

    normalized_role = str(operator_role or "").strip().upper()
    if normalized_role != "OPERATOR":
        raise EudrAcceptanceContractError(
            "OPERATOR_ROLE_NOT_SUPPORTED",
            "P1-D2 sólo habilita OPERATOR; REPRESENTATIVE_OPERATOR requiere mapeo de operador representado.",
        )

    activity = str(candidate_payload.get("activity_type") or "").strip().upper()
    if activity not in {"DOMESTIC", "IMPORT", "EXPORT"}:
        raise EudrAcceptanceContractError(
            "INVALID_ACTIVITY_TYPE",
            "API V3 sólo admite DOMESTIC, IMPORT o EXPORT.",
        )

    activity_country = _country(country_of_activity, field="country_of_activity")
    border_country = _country(
        border_cross_country,
        field="border_cross_country",
        required=activity in {"IMPORT", "EXPORT"},
    )

    previous_dds = dict(candidate_payload.get("previous_dds") or {})
    if previous_dds.get("relied_upon"):
        raise EudrAcceptanceContractError(
            "PREVIOUS_DDS_V3_MAPPING_REQUIRES_REVIEW",
            "No se traduce una DDS previa a groupedDeclarations: V3 cambió esa semántica y P1-D2 falla cerrado.",
        )

    shipment = dict(candidate_payload.get("shipment") or {})
    internal_ref = str(
        internal_reference_number or shipment.get("shipment_code") or ""
    ).strip()
    if not internal_ref:
        raise EudrAcceptanceContractError(
            "INTERNAL_REFERENCE_REQUIRED",
            "Se requiere una referencia interna para la DDS ACCEPTANCE.",
        )
    if len(internal_ref) > 120:
        raise EudrAcceptanceContractError(
            "INTERNAL_REFERENCE_TOO_LONG",
            "La referencia interna no puede superar 120 caracteres.",
        )

    product = dict(candidate_payload.get("product") or {})
    hs_heading = re.sub(r"[\s.-]", "", str(product.get("hs_code") or ""))
    if not _HS_V3_RE.fullmatch(hs_heading):
        raise EudrAcceptanceContractError(
            "HS_HEADING_NOT_V3_COMPATIBLE",
            "El hsHeading V3 debe contener entre 2 y 6 dígitos; no se trunca automáticamente.",
        )
    description = str(product.get("description") or "").strip()
    if not description:
        raise EudrAcceptanceContractError(
            "PRODUCT_DESCRIPTION_REQUIRED",
            "descriptionOfGoods es obligatorio para el request V3.",
        )
    net_weight = str(product.get("net_mass_kg") or "").strip()
    if not net_weight:
        raise EudrAcceptanceContractError(
            "NET_WEIGHT_REQUIRED",
            "netWeight es obligatorio para el request V3.",
        )

    production = dict(candidate_payload.get("production") or {})
    production_country = _country(
        production.get("country_code"),
        field="production_country",
    )
    plots = list(production.get("plots") or [])
    if not plots:
        raise EudrAcceptanceContractError(
            "PRODUCERS_REQUIRED",
            "El request V3 requiere al menos una parcela/productor geolocalizado.",
        )

    root = ET.Element(_tag(DDS_V3_NAMESPACE, "SubmitDdsRequest"))
    _text(root, DDS_V3_NAMESPACE, "operatorRole", normalized_role)
    statement = ET.SubElement(root, _tag(DDS_V3_NAMESPACE, "statement"))
    _text(statement, DDS_V3_NAMESPACE, "internalReferenceNumber", internal_ref)
    _text(statement, DDS_V3_NAMESPACE, "activityType", activity)
    _text(statement, DDS_V3_NAMESPACE, "countryOfActivity", activity_country)
    if border_country:
        _text(statement, DDS_V3_NAMESPACE, "borderCrossCountry", border_country)

    commodity = ET.SubElement(statement, _tag(DDS_V3_NAMESPACE, "commodities"))
    descriptors = ET.SubElement(commodity, _tag(DDS_V3_NAMESPACE, "descriptors"))
    _text(descriptors, EUDR_COMMON_V3_NAMESPACE, "descriptionOfGoods", description)
    measure = ET.SubElement(descriptors, _tag(EUDR_COMMON_V3_NAMESPACE, "goodsMeasure"))
    _text(measure, EUDR_COMMON_V3_NAMESPACE, "netWeight", net_weight)
    _text(commodity, DDS_V3_NAMESPACE, "hsHeading", hs_heading)

    if product.get("commodity_profile") == "WOOD":
        scientific = str(product.get("scientific_species_name") or "").strip()
        common = str(product.get("common_species_name") or "").strip()
        if not scientific or not common:
            raise EudrAcceptanceContractError(
                "WOOD_SPECIES_REQUIRED",
                "La madera requiere nombre científico y común en speciesInfo.",
            )
        species = ET.SubElement(commodity, _tag(DDS_V3_NAMESPACE, "speciesInfo"))
        _text(species, DDS_V3_NAMESPACE, "scientificName", scientific)
        _text(species, DDS_V3_NAMESPACE, "commonName", common)

    for position, plot in enumerate(plots, start=1):
        producer = ET.SubElement(commodity, _tag(DDS_V3_NAMESPACE, "producers"))
        _text(producer, DDS_V3_NAMESPACE, "position", position)
        _text(producer, DDS_V3_NAMESPACE, "country", production_country)
        _text(
            producer,
            DDS_V3_NAMESPACE,
            "geometryGeojson",
            _geojson_base64(dict(plot), production_country),
        )

    _text(
        statement,
        DDS_V3_NAMESPACE,
        "geoLocationConfidential",
        "true" if geo_location_confidential else "false",
    )

    xml = ET.tostring(root, encoding="utf-8", xml_declaration=False, short_empty_elements=True)
    return EudrV3PreparedBody(
        xml=xml,
        sha256=hashlib.sha256(xml).hexdigest(),
        byte_length=len(xml),
        operator_role=normalized_role,
        country_of_activity=activity_country or "",
        border_cross_country=border_country,
        internal_reference_number=internal_ref,
    )
