"""PPQ Form 505 (August 2025) preparation data contract.

This module describes a preparation work product only.  It does not determine
whether a declaration is legally required and it never represents an ACE or
LAWGS submission.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
import re
from typing import Callable


class PpqScope(StrEnum):
    SHIPMENT = "SHIPMENT"
    PLANT_LINE = "PLANT_LINE"


class PpqRequirement(StrEnum):
    REQUIRED = "REQUIRED"
    OPTIONAL = "OPTIONAL"
    CONDITIONAL = "CONDITIONAL"


class PpqValidationStatus(StrEnum):
    VALID = "VALID"
    INVALID = "INVALID"
    MISSING = "MISSING"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"


@dataclass(frozen=True, slots=True)
class PpqValidation:
    status: PpqValidationStatus
    normalized_value: str | None
    error: str | None = None


Validator = Callable[[object], PpqValidation]


@dataclass(frozen=True, slots=True)
class PpqField:
    number: int
    key: str
    label: str
    scope: PpqScope
    requirement: PpqRequirement = PpqRequirement.REQUIRED


# APHIS PPQ Form 505 uses one printed number for the two scientific-name parts.
# Keeping genus and species as separate internal keys prevents lossy parsing.
PPQ505_FIELDS: tuple[PpqField, ...] = (
    PpqField(1, "estimated_arrival_date", "Estimated Date of Arrival", PpqScope.SHIPMENT),
    PpqField(2, "filing_entry_reference", "Entry Number", PpqScope.SHIPMENT),
    PpqField(3, "container_number", "Container Number(s)", PpqScope.SHIPMENT, PpqRequirement.OPTIONAL),
    PpqField(4, "bill_of_lading", "Bill of Lading", PpqScope.SHIPMENT, PpqRequirement.OPTIONAL),
    PpqField(5, "manufacturer_id", "Manufacturer Identification Code (MID)", PpqScope.SHIPMENT),
    PpqField(6, "importer_name", "Importer's Name", PpqScope.SHIPMENT),
    PpqField(7, "consignee_name", "Consignee's Name", PpqScope.SHIPMENT),
    PpqField(8, "importer_address", "Importer's Address", PpqScope.SHIPMENT),
    PpqField(9, "consignee_address", "Consignee's Address", PpqScope.SHIPMENT),
    PpqField(10, "merchandise_description", "Description of Merchandise", PpqScope.SHIPMENT),
    PpqField(11, "hts_code", "HTS Number", PpqScope.PLANT_LINE),
    PpqField(12, "entered_value", "Entered Value", PpqScope.PLANT_LINE),
    PpqField(13, "article_component", "Article / Component", PpqScope.PLANT_LINE),
    PpqField(14, "genus", "Plant Scientific Name — Genus", PpqScope.PLANT_LINE),
    PpqField(14, "species", "Plant Scientific Name — Species", PpqScope.PLANT_LINE),
    PpqField(15, "country_of_harvest", "Country of Harvest", PpqScope.PLANT_LINE),
    PpqField(16, "plant_quantity", "Quantity of Plant Material", PpqScope.PLANT_LINE),
    PpqField(17, "metric_unit", "Unit", PpqScope.PLANT_LINE),
    PpqField(18, "percent_recycled", "Percent Recycled", PpqScope.PLANT_LINE, PpqRequirement.CONDITIONAL),
)

PPQ505_FIELDS_BY_KEY = {field.key: field for field in PPQ505_FIELDS}
PPQ505_SHIPMENT_FIELDS = tuple(field for field in PPQ505_FIELDS if field.scope is PpqScope.SHIPMENT)
PPQ505_PLANT_FIELDS = tuple(field for field in PPQ505_FIELDS if field.scope is PpqScope.PLANT_LINE)
PPQ505_SHIPMENT_REFERENCE = "__shipment__"

# Units accepted by the preparation contract. Values are normalized to the
# codes used in the workbook, without guessing from an unknown unit.
PPQ505_ALLOWED_UNITS = frozenset({
    "kg", "g", "cg", "mg", "kl", "l", "ml", "mm", "mm2", "mm3",
    "cm", "cm2", "cm3", "m", "m2", "m3", "km",
})
_UNIT_ALIASES = {
    "kilogram": "kg", "kilograms": "kg", "kgs": "kg",
    "gram": "g", "grams": "g", "centigram": "cg", "centigrams": "cg",
    "milligram": "mg", "milligrams": "mg", "kiloliter": "kl", "kiloliters": "kl",
    "liter": "l", "liters": "l", "litre": "l", "litres": "l",
    "milliliter": "ml", "milliliters": "ml", "millilitre": "ml", "millilitres": "ml",
    "millimeter": "mm", "millimeters": "mm", "centimeter": "cm", "centimeters": "cm",
    "meter": "m", "meters": "m", "metre": "m", "metres": "m", "kilometer": "km", "kilometers": "km",
    "square millimeter": "mm2", "square millimeters": "mm2", "mm^2": "mm2", "mm²": "mm2",
    "square centimeter": "cm2", "square centimeters": "cm2", "cm^2": "cm2", "cm²": "cm2",
    "square meter": "m2", "square meters": "m2", "m^2": "m2", "m²": "m2",
    "cubic millimeter": "mm3", "cubic millimeters": "mm3", "mm^3": "mm3", "mm³": "mm3",
    "cubic centimeter": "cm3", "cubic centimeters": "cm3", "cm^3": "cm3", "cm³": "cm3",
    "cubic meter": "m3", "cubic meters": "m3", "m^3": "m3", "m³": "m3",
}

NOT_REQUIRED_REASON_CODES = frozenset({"NOT_PAPER_OR_PAPERBOARD"})


def _missing(value: object) -> bool:
    return value is None or not str(value).strip()


def _result(normalized: str | None, error: str | None = None, *, review: bool = False) -> PpqValidation:
    if error:
        return PpqValidation(PpqValidationStatus.INVALID, normalized, error)
    if review:
        return PpqValidation(PpqValidationStatus.REVIEW_REQUIRED, normalized, "Human review is required.")
    return PpqValidation(PpqValidationStatus.VALID, normalized)


def _required(value: object) -> PpqValidation | None:
    if _missing(value):
        return PpqValidation(PpqValidationStatus.MISSING, None, "A value is required.")
    return None


def normalize_arrival_date(value: object) -> PpqValidation:
    if missing := _required(value):
        return missing
    if isinstance(value, datetime):
        parsed = value.date()
    elif isinstance(value, date):
        parsed = value
    else:
        text = str(value).strip()
        parsed = None
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
            try:
                parsed = datetime.strptime(text, fmt).date()
                break
            except ValueError:
                pass
        if parsed is None:
            return _result(None, "Estimated arrival date must be YYYY-MM-DD or MM/DD/YYYY.")
    return _result(parsed.isoformat())


def normalize_entry_number(value: object) -> PpqValidation:
    if missing := _required(value):
        return missing
    raw = str(value).strip().upper()
    compact = re.sub(r"[-\s]+", "", raw)
    if not re.fullmatch(r"[A-Z0-9]{11}", compact):
        return _result(compact or None, "Entry number must use the xxx-xxxxxxx-x structure.")
    return _result(f"{compact[:3]}-{compact[3:10]}-{compact[10]}")


def normalize_hts(value: object) -> PpqValidation:
    if missing := _required(value):
        return missing
    text = str(value).strip()
    normalized = re.sub(r"[.\s-]+", "", text)
    if not normalized.isdigit() or not 6 <= len(normalized) <= 10:
        return _result(normalized or None, "HTS number must contain 6–10 digits; symbols are not allowed in output.")
    return _result(normalized)


def _decimal(value: object, *, label: str, positive: bool, maximum: Decimal | None = None) -> PpqValidation:
    if missing := _required(value):
        return missing
    try:
        number = Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return _result(None, f"{label} must be numeric.")
    if not number.is_finite() or (number <= 0 if positive else number < 0):
        comparator = "greater than zero" if positive else "zero or greater"
        return _result(format(number, "f"), f"{label} must be {comparator}.")
    if maximum is not None and number > maximum:
        return _result(format(number, "f"), f"{label} must not exceed {maximum}.")
    normalized = format(number.normalize(), "f")
    return _result("0" if normalized == "-0" else normalized)


def normalize_entered_value(value: object) -> PpqValidation:
    return _decimal(value, label="Entered value", positive=False)


def normalize_quantity(value: object) -> PpqValidation:
    return _decimal(value, label="Plant quantity", positive=True)


def normalize_percent_recycled(value: object) -> PpqValidation:
    return _decimal(value, label="Percent recycled", positive=False, maximum=Decimal("100"))


def normalize_unit(value: object) -> PpqValidation:
    if missing := _required(value):
        return missing
    raw = re.sub(r"\s+", " ", str(value).strip().lower())
    normalized = _UNIT_ALIASES.get(raw, raw)
    if normalized not in PPQ505_ALLOWED_UNITS:
        return _result(normalized, "Unit must be one of the PPQ 505 metric units.")
    return _result(normalized)


def normalize_country_of_harvest(value: object) -> PpqValidation:
    if missing := _required(value):
        return missing
    normalized = re.sub(r"\s+", " ", str(value).strip())
    if not re.fullmatch(r"[A-Za-z][A-Za-z .'-]{1,98}", normalized):
        return _result(normalized, "Country of harvest must be a country name or code supported by source evidence.")
    return _result(normalized, review=len(normalized) == 2)


def _taxon(value: object, label: str) -> PpqValidation:
    if missing := _required(value):
        return missing
    normalized = re.sub(r"\s+", " ", str(value).strip())
    if not re.fullmatch(r"[A-Za-z][A-Za-z .×x-]{0,99}", normalized):
        return _result(normalized, f"{label} contains unsupported characters.")
    return _result(normalized)


def normalize_genus(value: object) -> PpqValidation:
    return _taxon(value, "Genus")


def normalize_species(value: object) -> PpqValidation:
    return _taxon(value, "Species")


def normalize_mid(value: object) -> PpqValidation:
    if missing := _required(value):
        return missing
    normalized = re.sub(r"[\s-]+", "", str(value).upper())
    if not re.fullmatch(r"[A-Z0-9]{5,20}", normalized):
        return _result(normalized or None, "MID must contain 5–20 letters or digits.")
    return _result(normalized)


_VALIDATORS: dict[str, Validator] = {
    "estimated_arrival_date": normalize_arrival_date,
    "filing_entry_reference": normalize_entry_number,
    "manufacturer_id": normalize_mid,
    "hts_code": normalize_hts,
    "entered_value": normalize_entered_value,
    "plant_quantity": normalize_quantity,
    "metric_unit": normalize_unit,
    "percent_recycled": normalize_percent_recycled,
    "country_of_harvest": normalize_country_of_harvest,
    "genus": normalize_genus,
    "species": normalize_species,
}


def validate_ppq_value(field_key: str, value: object) -> PpqValidation:
    """Validate without losing the caller-owned raw/source value."""
    field = PPQ505_FIELDS_BY_KEY.get(field_key)
    if field is None:
        return PpqValidation(PpqValidationStatus.INVALID, None, "Unknown PPQ 505 field.")
    validator = _VALIDATORS.get(field_key)
    if validator:
        return validator(value)
    if _missing(value):
        if field.requirement is PpqRequirement.OPTIONAL:
            return PpqValidation(PpqValidationStatus.VALID, None)
        return _required(value)  # type: ignore[return-value]
    normalized = re.sub(r"\s+", " ", str(value).strip())
    if len(normalized) > 4000:
        return _result(None, f"{field.label} is too long.")
    return _result(normalized)


def is_paper_or_paperboard(article_or_product: object) -> bool:
    return bool(re.search(r"\b(paper|paperboard)\b", str(article_or_product or ""), re.I))


def not_required_allowed(
    field_key: str, reason_code: str | None, *, article_or_product: object = None
) -> bool:
    return (
        field_key == "percent_recycled"
        and str(reason_code or "").strip().upper() in NOT_REQUIRED_REASON_CODES
        and not is_paper_or_paperboard(article_or_product)
    )
