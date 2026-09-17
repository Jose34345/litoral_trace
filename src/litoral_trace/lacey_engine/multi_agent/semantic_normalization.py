"""Comparison-only semantic normalization for Lacey multi-agent evidence.

The functions in this module never rewrite source evidence. They produce deterministic
keys used only to decide whether two already-admitted candidates mean the same thing.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re
import unicodedata

from ..ai_shadow import comparison_key


_NUMERIC_FIELDS = frozenset({"entered_value", "plant_quantity"})
_CURRENCY_PREFIX = re.compile(
    r"^(?:USD|EUR|CAD|GBP|AUD|JPY|CNY|BRL|MXN)\s*",
    re.IGNORECASE,
)
_CURRENCY_SYMBOLS = frozenset({"$", "€", "£", "¥"})

_COUNTRY_ALIASES = {
    "br": "BR",
    "brasil": "BR",
    "brazil": "BR",
}

_UNIT_ALIASES = {
    "cubic meter": "m3",
    "cubic meters": "m3",
    "cubic metre": "m3",
    "cubic metres": "m3",
    "m3": "m3",
}


def _fold(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", text.casefold()).strip()


def _decimal_key(value: object) -> str | None:
    text = unicodedata.normalize("NFKC", str(value or "")).strip()
    text = _CURRENCY_PREFIX.sub("", text, count=1)
    text = "".join(ch for ch in text if ch not in _CURRENCY_SYMBOLS)
    text = re.sub(r"[\s\u00a0\u202f]+", "", text).replace(",", "")
    if not re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)", text):
        return None
    try:
        number = Decimal(text)
    except InvalidOperation:
        return None
    if not number.is_finite():
        return None
    normalized = format(number.normalize(), "f")
    return "0" if normalized == "-0" else normalized


def _unit_key(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    text = re.sub(r"\s+", " ", text).strip()
    return _UNIT_ALIASES.get(text, text)


def _taxon_key(value: object) -> str:
    return _fold(value)


def semantic_value_key(
    field_key: str,
    value: object,
    *,
    genus_context: frozenset[str] = frozenset(),
) -> str:
    """Return a deterministic semantic identity without mutating source evidence."""
    field = str(field_key or "").strip()
    raw = str(value or "").strip()
    if not raw:
        return ""

    if field == "hts_code":
        compact = re.sub(r"[.\s-]+", "", unicodedata.normalize("NFKC", raw))
        if compact.isdigit() and 6 <= len(compact) <= 10:
            return compact

    if field in _NUMERIC_FIELDS:
        numeric = _decimal_key(raw)
        if numeric is not None:
            return numeric

    if field == "metric_unit":
        return _unit_key(raw)

    if field == "country_of_harvest":
        folded = _fold(raw)
        return _COUNTRY_ALIASES.get(folded, folded)

    if field == "genus":
        return _taxon_key(raw)

    if field == "species":
        taxon = _taxon_key(raw)
        tokens = taxon.split()
        if (
            len(tokens) >= 2
            and len(genus_context) == 1
            and tokens[0] in genus_context
        ):
            return " ".join(tokens[1:])
        return taxon

    existing = comparison_key(field, raw)
    return existing or _fold(raw)
