from __future__ import annotations
import re
from .domain import RawCandidate
from .garbage_patterns import is_label_garbage
from .trade_validation import (
    is_valid_entity_name,
    normalize_invoice_total,
    normalize_iso6346_container,
    normalize_mid,
    normalize_seal_number,
)
_MERCHANDISE_DESCRIPTION_LABEL = re.compile(
    r"(?:merchandise description|commodity description|cargo description(?:\s+\d+)?|description of goods|goods description)",
    re.I,
)


def admit(raw: RawCandidate) -> bool:
    value = raw.normalized_value.strip()
    if raw.evidence_class.value == "INFERRED" or not value or is_label_garbage(value):
        return False
    if raw.field_key == "container_number":
        return (
            raw.label is not None
            and "container" in raw.label.casefold()
            and normalize_iso6346_container(value) is not None
        )
    if raw.field_key == "seal_number":
        return (
            raw.label is not None
            and "seal" in raw.label.casefold()
            and normalize_seal_number(value) is not None
        )
    if raw.field_key == "invoice_total":
        return (
            raw.label is not None
            and "total" in raw.label.casefold()
            and normalize_invoice_total(value) is not None
        )
    if raw.field_key == "manufacturer_id":
        return normalize_mid(
            value,
            source_text=raw.source_block.text,
            label=raw.label or "",
        ) is not None
    if raw.field_key in {"supplier_name", "importer_name"}:
        return is_valid_entity_name(value)
    if raw.field_key == "bill_of_lading":
        # A B/L heading or report column is not a B/L identifier. Keep this
        # label-bound and require an identifier-like value, rather than allowing
        # nearby table headings such as "Voyage" into the candidate pool.
        return (
            raw.label is not None
            and bool(re.fullmatch(r"(?:master\s+(?:bill of lading|bol|b/l)(?:\s*#)?|house\s+(?:bill of lading|bol|b/l)(?:\s*#)?|bill of lading|b/l\s*no\.?|bol)", raw.label, re.I))
            and bool(re.fullmatch(r"[A-Z0-9][A-Z0-9-]{5,34}", value, re.I))
            and any(character.isdigit() for character in value)
        )
    if raw.field_key == "consignee_name":
        return (
            raw.label is not None
            and bool(re.fullmatch(r"consignee(?: name)?", raw.label, re.I))
            and not any(x in value.casefold() for x in ("address line", "city", "state province", "zip code", "country code"))
            and any(character.isalpha() for character in value)
        )
    if raw.field_key == "description":
        return raw.label is not None and bool(_MERCHANDISE_DESCRIPTION_LABEL.fullmatch(raw.label.strip()))
    if raw.field_key == "country_of_harvest":
        return raw.label is not None and bool(re.search(r"(?:country of harvest|harvest country|harvested in)", raw.label, re.I))
    if raw.field_key == "plant_quantity":
        return raw.label is not None and not bool(re.search(r"(?:gross|net|shipment|manifest) weight", raw.label, re.I))
    return True
