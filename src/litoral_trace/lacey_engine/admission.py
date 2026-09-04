from __future__ import annotations
import re
from .domain import RawCandidate
from .garbage_patterns import is_label_garbage

_CONTAINER = re.compile(r"^[A-Z]{4}\d{7}$")


def admit(raw: RawCandidate) -> bool:
    value = raw.normalized_value.strip()
    if raw.evidence_class.value == "INFERRED" or not value or is_label_garbage(value):
        return False
    if raw.field_key == "container_number":
        return raw.label is not None and "container" in raw.label.casefold() and bool(_CONTAINER.fullmatch(value))
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
    if raw.field_key == "country_of_harvest":
        return raw.label is not None and bool(re.search(r"(?:country of harvest|harvest country|harvested in)", raw.label, re.I))
    if raw.field_key == "plant_quantity":
        return raw.label is not None and not bool(re.search(r"(?:gross|net|shipment|manifest) weight", raw.label, re.I))
    return True
