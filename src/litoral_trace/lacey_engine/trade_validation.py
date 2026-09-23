"""Trade-domain validation for shipment identity candidates.

These helpers are pure and provider-neutral. They validate documentary values before
candidate admission; they do not decide regulatory applicability or mutate persistence.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re


_CONTAINER_COMPACT = re.compile(r"^[A-Z]{4}[0-9]{7}$")
_SEAL_COMPACT = re.compile(r"^[A-Z0-9][A-Z0-9-]{2,23}$")
_FINANCIAL_VALUE = re.compile(
    r"^\s*(?:(?P<currency>USD|EUR|CAD|GBP|AUD|JPY|CNY|BRL|MXN)\s*)?"
    r"(?P<symbol>\$)?\s*(?P<amount>[0-9][0-9,]*(?:\.[0-9]{1,4})?)\s*"
    r"(?P<suffix>USD|EUR|CAD|GBP|AUD|JPY|CNY|BRL|MXN)?\s*$",
    re.I,
)
_PHONE_ONLY = re.compile(r"^\s*\+?[0-9][0-9() .-]{5,}[0-9](?:\s*(?:x|ext\.?)\s*[0-9]+)?\s*$", re.I)
_MID_COMPACT = re.compile(r"^[A-Z]{2}[A-Z0-9]{3,13}$")
_SCAC_MARKER = re.compile(r"\bSCAC\b", re.I)

# ISO 6346 owner-code letter values skip 11 and its multiples.
_ISO6346_LETTER_VALUE: dict[str, int] = {}
_value = 10
for _letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
    while _value % 11 == 0:
        _value += 1
    _ISO6346_LETTER_VALUE[_letter] = _value
    _value += 1


def normalize_iso6346_container(value: object) -> str | None:
    """Return canonical ISO 6346 container ID only when its check digit is valid."""
    compact = re.sub(r"[\s-]+", "", str(value or "").upper())
    if not _CONTAINER_COMPACT.fullmatch(compact):
        return None

    # The fourth character is the equipment category identifier. U identifies
    # freight containers; J/Z are detachable equipment/chassis, not container IDs.
    if compact[3] != "U":
        return None

    total = 0
    for index, character in enumerate(compact[:10]):
        numeric = (
            int(character)
            if character.isdigit()
            else _ISO6346_LETTER_VALUE[character]
        )
        total += numeric * (2**index)

    remainder = total % 11
    expected = 0 if remainder == 10 else remainder
    if expected != int(compact[-1]):
        return None
    return compact


def normalize_seal_number(value: object) -> str | None:
    """Normalize a seal only after the caller has established explicit seal context."""
    compact = re.sub(r"\s+", "", str(value or "").upper()).strip()
    if not _SEAL_COMPACT.fullmatch(compact):
        return None
    if normalize_iso6346_container(compact) is not None:
        return None
    return compact


def normalize_invoice_total(value: object) -> str | None:
    """Return a decimal invoice total; reject OCR/text garbage and ambiguous formats."""
    text = " ".join(str(value or "").split()).strip()
    match = _FINANCIAL_VALUE.fullmatch(text)
    if match is None:
        return None

    prefix = (match.group("currency") or "").upper()
    suffix = (match.group("suffix") or "").upper()
    if prefix and suffix and prefix != suffix:
        return None

    try:
        amount = Decimal(match.group("amount").replace(",", ""))
    except InvalidOperation:
        return None
    if not amount.is_finite() or amount < 0:
        return None

    normalized = format(amount, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def is_valid_entity_name(value: object) -> bool:
    """Reject OCR fragments and phone-only strings without overfitting company names."""
    text = " ".join(str(value or "").split()).strip(" ,;|")
    if not text or _PHONE_ONLY.fullmatch(text):
        return False

    if text.isalpha() and len(text) <= 2:
        return False

    letters = sum(character.isalpha() for character in text)
    if letters < 2:
        return False

    # Preserve legitimate compact brands such as 3M, but reject strings that are
    # overwhelmingly numeric punctuation with a stray OCR letter.
    significant = sum(character.isalnum() for character in text)
    return significant > 0 and letters / significant >= 0.20


def normalize_mid(
    value: object,
    *,
    source_text: object = "",
    label: object = "",
) -> str | None:
    """Validate a CBP-style MID candidate and reject carrier/SCAC context.

    CBP MIDs are constructed from country, manufacturer name, address and city,
    contain no inserted spaces, and are at most 15 characters. This function
    validates the observable structural contract without fabricating missing pieces.
    """
    raw = str(value or "").upper().strip()
    compact = re.sub(r"[^A-Z0-9]", "", raw)
    source = str(source_text or "")
    label_text = str(label or "")

    if not _MID_COMPACT.fullmatch(compact):
        return None
    if len(compact) > 15:
        return None
    if "SCAC" in compact:
        return None

    explicit_mid_label = bool(
        re.search(
            r"\b(?:MID|MANUFACTURER\s+(?:ID|IDENTIFICATION(?:\s+CODE)?))\b",
            label_text,
            re.I,
        )
        or re.search(
            r"\b(?:MID|MANUFACTURER\s+(?:ID|IDENTIFICATION(?:\s+CODE)?))\b\s*[:#-]",
            source,
            re.I,
        )
    )
    if _SCAC_MARKER.search(source) and not explicit_mid_label:
        return None
    return compact


def is_scac_context(value: object, *, source_text: object = "") -> bool:
    compact = re.sub(r"[^A-Z]", "", str(value or "").upper())
    return (
        bool(re.fullmatch(r"[A-Z]{2,4}", compact))
        or "SCAC" in compact
        or bool(_SCAC_MARKER.search(str(source_text or "")))
    )
