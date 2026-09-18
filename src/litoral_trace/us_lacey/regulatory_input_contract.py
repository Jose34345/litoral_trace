"""Auditable, fail-closed input contract for U.S. Lacey regulatory rules.

This module classifies whether exact rule inputs are supported, review-only, missing,
or semantically unsafe. It never converts Product Intelligence observations into
regulatory facts when the source semantics do not prove the required quantity.
"""
from __future__ import annotations

from enum import StrEnum
import re
from typing import Any, Iterable, Mapping


INPUT_CONTRACT_SCHEMA_VERSION = "regulatory-input-contract-v1"
_HTS10 = re.compile(r"^\d{10}$")


class InputStatus(StrEnum):
    SUPPORTED = "SUPPORTED"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    MISSING = "MISSING"
    UNSAFE_SEMANTICS = "UNSAFE_SEMANTICS"


def _evidence(field: object) -> dict[str, Any]:
    return {
        "source_assurance_document_id": getattr(field, "source_assurance_document_id", None),
        "source_page": getattr(field, "source_page", None),
        "source_locator": getattr(field, "source_locator", None),
    }


def _missing(reason: str) -> dict[str, Any]:
    return {
        "status": InputStatus.MISSING.value,
        "value": None,
        "reason": reason,
    }


def _hts_input(
    *,
    line_reference: str | None,
    operation_fields: tuple[object, ...],
) -> dict[str, Any]:
    if not line_reference:
        return _missing("SHIPMENT_LINE_NOT_LINKED")

    matches = tuple(
        field
        for field in operation_fields
        if str(getattr(field, "merchandise_line_reference", "") or "").strip() == line_reference
        and str(getattr(field, "field_name", "") or "") == "hts_code"
        and str(getattr(field, "field_scope", "") or "").upper() == "PLANT_LINE"
    )
    if len(matches) != 1:
        return _missing(
            "HTS10_NOT_AVAILABLE"
            if not matches
            else "HTS10_LINE_ASSOCIATION_AMBIGUOUS"
        )

    field = matches[0]
    value = str(
        getattr(field, "human_value", None)
        or getattr(field, "normalized_value", None)
        or getattr(field, "original_value", None)
        or ""
    ).strip()
    if (
        str(getattr(field, "validation_status", "") or "").upper() != "VALID"
        or not _HTS10.fullmatch(value)
    ):
        return _missing("VALID_HTS10_NOT_AVAILABLE")

    reviewed = (
        getattr(field, "reviewed_at", None) is not None
        or bool(str(getattr(field, "human_value", None) or "").strip())
    )
    return {
        "status": (
            InputStatus.SUPPORTED.value
            if reviewed
            else InputStatus.REVIEW_REQUIRED.value
        ),
        "value": value,
        "reason": (
            "REVIEWED_VALID_HTS10"
            if reviewed
            else "VALID_HTS10_AWAITS_HUMAN_CONFIRMATION"
        ),
        "evidence": _evidence(field),
    }


def _observed_bom_component_mass(product: Mapping[str, Any]) -> dict[str, Any] | None:
    observations: list[dict[str, Any]] = []
    for component in product.get("components", ()):
        if not isinstance(component, Mapping):
            continue
        material = component.get("material")
        if not isinstance(material, Mapping):
            continue
        mass = material.get("mass")
        if not isinstance(mass, Mapping):
            continue
        kilograms = mass.get("kilograms")
        if kilograms is None:
            continue
        observations.append(
            {
                "kilograms": str(kilograms),
                "component_key": component.get("component_key"),
                "source": dict(material.get("source") or component.get("source") or {}),
            }
        )
    if not observations:
        return None
    if len(observations) == 1:
        return observations[0]
    return {"observations": observations}


def build_regulatory_input_contract(
    *,
    product_intelligence_payload: Mapping[str, Any],
    operation_fields: Iterable[object],
) -> dict[str, Any]:
    """Build one rule-input inventory per Product Intelligence bridge subject."""
    fields = tuple(operation_fields)
    bridge = product_intelligence_payload.get("shipment_product_bridge")
    links = bridge.get("links", ()) if isinstance(bridge, Mapping) else ()

    subjects: list[dict[str, Any]] = []
    counts = {status.value: 0 for status in InputStatus}

    for raw_link in links:
        if not isinstance(raw_link, Mapping):
            continue
        product = raw_link.get("product")
        if not isinstance(product, Mapping):
            continue
        sku = str(product.get("sku") or "").strip()
        if not sku:
            continue

        link_status = str(raw_link.get("status") or "UNLINKED_REVIEW")
        line_reference_raw = raw_link.get("shipment_line_reference")
        line_reference = (
            str(line_reference_raw).strip()
            if line_reference_raw is not None and str(line_reference_raw).strip()
            else None
        )
        hts = _hts_input(
            line_reference=line_reference if link_status == "LINKED" else None,
            operation_fields=fields,
        )

        observed_mass = _observed_bom_component_mass(product)
        if observed_mass is None:
            plant_mass = _missing("PLANT_MASS_PER_UNIT_NOT_PROVIDED")
        else:
            observed_value = observed_mass.get("kilograms")
            plant_mass = {
                "status": InputStatus.UNSAFE_SEMANTICS.value,
                "value": None,
                "observed_value": observed_value,
                "observed": observed_mass,
                "reason": "BOM_COMPONENT_WEIGHT_DOES_NOT_PROVE_PLANT_MASS_PER_UNIT",
            }

        inputs = {
            "hts10": hts,
            "plant_mass_per_unit_kg": plant_mass,
            "total_unit_mass_kg": _missing("TOTAL_UNIT_MASS_NOT_PROVIDED"),
            "entry_same_hts_plant_mass_kg": _missing(
                "ENTRY_SAME_HTS_PLANT_MASS_NOT_PROVIDED"
            ),
            "protected_status": _missing("PROTECTED_PLANT_STATUS_NOT_ASSESSED"),
        }
        for item in inputs.values():
            status = str(item.get("status") or "")
            if status in counts:
                counts[status] += 1

        subjects.append(
            {
                "subject_ref": sku,
                "line_item_key": raw_link.get("line_item_key"),
                "shipment_line_reference": line_reference,
                "link_status": link_status,
                "inputs": inputs,
            }
        )

    return {
        "schema_version": INPUT_CONTRACT_SCHEMA_VERSION,
        "summary": {
            "subject_count": len(subjects),
            "supported_count": counts[InputStatus.SUPPORTED.value],
            "review_required_count": counts[InputStatus.REVIEW_REQUIRED.value],
            "missing_count": counts[InputStatus.MISSING.value],
            "unsafe_semantics_count": counts[InputStatus.UNSAFE_SEMANTICS.value],
        },
        "subjects": subjects,
    }
