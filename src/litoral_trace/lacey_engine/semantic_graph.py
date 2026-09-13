"""Semantic evidence primitives for the U.S. Lacey document engine.

This module is deliberately deterministic.  It gives extracted facts an identity
(scope/entity/row) before any model is allowed to reason about them.  The objective
is to distinguish corroboration, parallel plant components and stale/historical
noise from a true contradiction.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from enum import Enum
import re
import unicodedata

from .domain import AdmittedCandidate, LayoutStructureType


class EvidenceRelation(str, Enum):
    CORROBORATION = "CORROBORATION"
    PARALLEL_ENTITY = "PARALLEL_ENTITY"
    TRUE_CONFLICT = "TRUE_CONFLICT"
    OUT_OF_SCOPE = "OUT_OF_SCOPE"
    AMBIGUOUS = "AMBIGUOUS"


_OUT_OF_SCOPE = re.compile(
    r"\b(?:prior shipment|previous shipment|last year(?:'s)? shipment|old shipment|"
    r"historical(?:ly)?|archived|archive(?:d)?|reference only|do not use|not current|"
    r"former shipment|earlier shipment)\b",
    re.IGNORECASE,
)
_EMAIL = re.compile(r"\b[^\s@]+@[^\s@]+\.[^\s@]+\b", re.IGNORECASE)
_PHONE = re.compile(r"(?:\+?\d[\d() .-]{6,}\d)")
_STREET_START = re.compile(
    r"\s+(?P<address>\d{1,6}\s+[A-Za-z0-9.' -]+\b(?:street|st|road|rd|avenue|ave|way|drive|dr|"
    r"boulevard|blvd|lane|ln|highway|hwy|parkway|pkwy|place|pl)\b.*)$",
    re.IGNORECASE,
)
_COMPANY_SUFFIX = re.compile(
    r"\b(?:LLC|L\.L\.C\.|INC\.?|INCORPORATED|LTD\.?|LIMITED|CORP\.?|CORPORATION|CO\.?|COMPANY)\b",
    re.IGNORECASE,
)
_STRUCTURAL_MID_VALUES = frozenset({"CODE", "ID", "NUMBER", "NO", "IDENTIFICATION", "MANUFACTURER", "MID"})


def fold_text(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or "")).casefold()
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text).split())


def is_out_of_scope_context(text: object) -> bool:
    """Exclude facts only when source prose explicitly marks another/prior shipment."""
    return bool(_OUT_OF_SCOPE.search(str(text or "")))


def party_core(value: object) -> str:
    raw = " ".join(str(value or "").split()).strip()
    if not raw:
        return ""
    raw = _EMAIL.sub(" ", raw)
    raw = _PHONE.sub(" ", raw)
    street = _STREET_START.search(raw)
    if street:
        raw = raw[: street.start()]
    comma = raw.find(",")
    if comma > 0 and re.search(r"\b(?:[A-Z]{2}\s+\d{5}|USA|UNITED STATES|VIETNAM|THAILAND|MALAYSIA)\b", raw[comma:], re.I):
        raw = raw[:comma]
    return " ".join(raw.replace("|", " ").split()).strip(" ,-;")


def party_address(value: object) -> str | None:
    raw = " ".join(str(value or "").split()).strip()
    match = _STREET_START.search(raw)
    if not match:
        return None
    address = _EMAIL.sub(" ", match.group("address"))
    address = _PHONE.sub(" ", address)
    return " ".join(address.replace("|", " ").split()).strip(" ,-;") or None


def _mass_kg(raw: str) -> str | None:
    match = re.fullmatch(
        r"\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*(g|kg|lb|metric tons?|tonnes?)\s*",
        raw,
        re.I,
    )
    if not match:
        return None
    try:
        amount = Decimal(match.group(1).replace(",", ""))
    except InvalidOperation:
        return None
    unit = match.group(2).casefold()
    factor = {
        "g": Decimal("0.001"), "kg": Decimal("1"), "lb": Decimal("0.45359237"),
        "metric ton": Decimal("1000"), "metric tons": Decimal("1000"),
        "tonne": Decimal("1000"), "tonnes": Decimal("1000"),
    }[unit]
    return f"{(amount * factor).normalize()} kg"


def semantic_normalize(field_key: str, value: object) -> str:
    raw = " ".join(str(value or "").split()).strip()
    if not raw:
        return ""
    key = str(field_key or "").strip().casefold()
    if key in {"importer_name", "consignee_name", "shipper_name", "supplier_name", "manufacturer_name", "notify_party_name", "filer_name"}:
        raw = _COMPANY_SUFFIX.sub("", party_core(raw))
        return fold_text(raw)
    if key == "hts_code":
        return re.sub(r"\D", "", raw)
    if key in {"container_number", "bill_of_lading", "manufacturer_id", "filing_entry_reference"}:
        return re.sub(r"[^A-Z0-9]", "", raw.upper())
    if key in {"genus", "species", "country_of_harvest", "metric_unit"}:
        return fold_text(raw)
    if key == "plant_quantity":
        mass = _mass_kg(raw)
        if mass:
            return mass
        number = re.search(r"-?[0-9][0-9,]*(?:\.[0-9]+)?", raw)
        return number.group(0).replace(",", "") if number else fold_text(raw)
    if key == "entered_value":
        match = re.fullmatch(r"\s*(?:(USD|EUR|CAD|GBP|AUD|JPY)\s*)?\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*", raw, re.I)
        if match:
            currency = (match.group(1) or ("USD" if "$" in raw else "")).upper()
            amount = Decimal(match.group(2).replace(",", "")).normalize()
            return f"{currency} {amount}".strip()
        return fold_text(raw)
    if key == "percent_recycled":
        number = re.search(r"-?[0-9][0-9,]*(?:\.[0-9]+)?", raw)
        return number.group(0).replace(",", "") if number else fold_text(raw)
    return fold_text(raw)


def semantic_equal(field_key: str, left: object, right: object) -> bool:
    a = semantic_normalize(field_key, left)
    b = semantic_normalize(field_key, right)
    return bool(a and b and a == b)


def valid_mid_value(value: object) -> bool:
    normalized = str(value or "").strip().upper()
    return bool(normalized) and normalized not in _STRUCTURAL_MID_VALUES and len(normalized) >= 5


def association_key(candidate: AdmittedCandidate, scope: str, document_id: str) -> str | None:
    block = candidate.raw.source_block
    if scope not in {"MERCHANDISE_LINE", "PLANT_COMPONENT"}:
        return None
    if block.table_id and block.row_index is not None and block.structure_type in {LayoutStructureType.LINE_ITEM_TABLE, LayoutStructureType.MATRIX_TABLE}:
        return f"{document_id}:{block.table_id}:row:{block.row_index}"
    label = str(candidate.raw.label or "")
    match = re.search(r"(?:component|line)\s*(?:#|number)?\s*([a-z0-9-]+)", label, re.I)
    return match.group(1).casefold() if match else None


def evidence_relation(
    field_key: str,
    left_value: object,
    right_value: object,
    *,
    left_entity: str | None = None,
    right_entity: str | None = None,
    left_context: object = "",
    right_context: object = "",
) -> EvidenceRelation:
    if is_out_of_scope_context(left_context) or is_out_of_scope_context(right_context):
        return EvidenceRelation.OUT_OF_SCOPE
    if left_entity and right_entity and left_entity != right_entity:
        return EvidenceRelation.PARALLEL_ENTITY
    if semantic_equal(field_key, left_value, right_value):
        return EvidenceRelation.CORROBORATION
    if left_entity and right_entity and left_entity == right_entity:
        return EvidenceRelation.TRUE_CONFLICT
    return EvidenceRelation.AMBIGUOUS
