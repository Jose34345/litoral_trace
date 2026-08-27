"""Deterministic normalization helpers for Assurance document ingestion.

These functions intentionally avoid generative inference. They convert common
Argentine business-document representations into comparable canonical values
used by reconciliation and preflight rules.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import re
import unicodedata


_NUMBER_ALLOWED_RE = re.compile(r"[^0-9,\.\-+]")
_QUANTITY_RE = re.compile(
    r"^\s*([+\-]?[0-9][0-9.,\s]*)\s*([A-Za-z³3_./ -]*)\s*$"
)
_NON_DIGIT_RE = re.compile(r"\D+")

_UNIT_ALIASES = {
    "t": "t",
    "tn": "t",
    "ton": "t",
    "tons": "t",
    "tonelada": "t",
    "toneladas": "t",
    "kg": "kg",
    "kgs": "kg",
    "kilogramo": "kg",
    "kilogramos": "kg",
    "g": "g",
    "gr": "g",
    "gramo": "g",
    "gramos": "g",
    "m3": "m3",
    "m³": "m3",
    "metro cubico": "m3",
    "metros cubicos": "m3",
    "unidad": "unit",
    "unidades": "unit",
    "un": "unit",
    "u": "unit",
}

_DATE_PATTERNS = (
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%d.%m.%Y",
    "%Y/%m/%d",
    "%d/%m/%y",
    "%d-%m-%y",
)


class NormalizationError(ValueError):
    """Raised when a value cannot be normalized without guessing."""


@dataclass(frozen=True, slots=True)
class NormalizedQuantity:
    amount: Decimal
    unit: str | None


def _strip_accents(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def normalize_argentine_number(value: object) -> Decimal:
    """Normalize Argentine/ISO decimal representations without float loss.

    Examples: ``1.234,56`` -> ``1234.56`` and ``1,234.56`` -> ``1234.56``.
    Ambiguous single separators are interpreted as decimal unless exactly
    three trailing digits strongly indicate a thousands group.
    """
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool) or value is None:
        raise NormalizationError("Valor numerico invalido.")
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))

    text = str(value).strip().replace("\u00a0", " ")
    if not text:
        raise NormalizationError("Valor numerico vacio.")

    negative_parentheses = text.startswith("(") and text.endswith(")")
    if negative_parentheses:
        text = text[1:-1]

    text = _NUMBER_ALLOWED_RE.sub("", text.replace(" ", ""))
    if not text or text in {"+", "-"}:
        raise NormalizationError("Valor numerico invalido.")

    comma_pos = text.rfind(",")
    dot_pos = text.rfind(".")

    if comma_pos >= 0 and dot_pos >= 0:
        decimal_sep = "," if comma_pos > dot_pos else "."
        thousands_sep = "." if decimal_sep == "," else ","
        text = text.replace(thousands_sep, "")
        text = text.replace(decimal_sep, ".")
    elif comma_pos >= 0:
        integer, fractional = text.rsplit(",", 1)
        if len(fractional) == 3 and integer.replace("+", "").replace("-", "").isdigit():
            text = integer + fractional
        else:
            text = integer + "." + fractional
    elif dot_pos >= 0:
        integer, fractional = text.rsplit(".", 1)
        if text.count(".") > 1:
            groups = text.split(".")
            if all(len(group) == 3 for group in groups[1:]):
                text = "".join(groups)
        elif len(fractional) == 3 and integer.replace("+", "").replace("-", "").isdigit():
            text = integer + fractional

    try:
        result = Decimal(text)
    except InvalidOperation as exc:
        raise NormalizationError("Valor numerico invalido.") from exc

    return -result if negative_parentheses and result > 0 else result


def normalize_date(value: object) -> date:
    """Normalize known date representations; never infer month/day order."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        raise NormalizationError("Fecha invalida.")

    text = str(value).strip()
    if not text:
        raise NormalizationError("Fecha vacia.")

    iso_candidate = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(iso_candidate).date()
    except ValueError:
        pass

    for pattern in _DATE_PATTERNS:
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    raise NormalizationError("Formato de fecha no reconocido.")


def normalize_cuit(value: object, *, validate_checksum: bool = True) -> str:
    """Return an 11-digit CUIT, optionally verifying the AFIP checksum."""
    digits = _NON_DIGIT_RE.sub("", str(value or ""))
    if len(digits) != 11:
        raise NormalizationError("CUIT debe contener 11 digitos.")

    if validate_checksum:
        weights = (5, 4, 3, 2, 7, 6, 5, 4, 3, 2)
        total = sum(int(digit) * weight for digit, weight in zip(digits[:10], weights))
        verifier = 11 - (total % 11)
        verifier = 0 if verifier == 11 else 9 if verifier == 10 else verifier
        if verifier != int(digits[-1]):
            raise NormalizationError("CUIT con digito verificador invalido.")
    return digits


def normalize_identifier(value: object) -> str:
    """Normalize identifiers while preserving meaningful alphanumerics."""
    text = unicodedata.normalize("NFKC", str(value or "")).strip().upper()
    text = re.sub(r"\s+", " ", text)
    if not text:
        raise NormalizationError("Identificador vacio.")
    return text


def normalize_quantity(value: object, unit: object | None = None) -> NormalizedQuantity:
    """Normalize an amount plus an optional unit token."""
    if unit is not None:
        amount = normalize_argentine_number(value)
        raw_unit = str(unit).strip()
    else:
        match = _QUANTITY_RE.match(str(value or ""))
        if match is None:
            raise NormalizationError("Cantidad invalida.")
        amount = normalize_argentine_number(match.group(1))
        raw_unit = match.group(2).strip()

    normalized_unit: str | None = None
    if raw_unit:
        token = _strip_accents(raw_unit.lower()).replace("³", "3")
        token = re.sub(r"\s+", " ", token).strip(" .")
        normalized_unit = _UNIT_ALIASES.get(token)
        if normalized_unit is None:
            normalized_unit = token.replace(" ", "_")[:32]

    return NormalizedQuantity(amount=amount, unit=normalized_unit)
