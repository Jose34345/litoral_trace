"""Deterministic normalization helpers for heterogeneous spreadsheet headers."""

from __future__ import annotations

import re
import unicodedata


SMART_HEADER_TEXT_MAX_CHARS = 256

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_WHITESPACE_RE = re.compile(r"\s+")

_ABBREVIATION_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    (r"\bnro\b", "numero"),
    (r"\bnº\b", "numero"),
    (r"\bnum\b", "numero"),
    (r"\bcod\b", "codigo"),
    (r"\bsup\b", "superficie"),
    (r"\bhs\b", "hectareas"),
    (r"\bhas\b", "hectareas"),
    (r"\bton\b", "ton"),
    (r"\btoneladas?\b", "ton"),
    (r"\btons\b", "ton"),
)


def normalize_header(value: object) -> str:
    """Return a stable accent/case/punctuation-insensitive header string.

    Header normalization is on an untrusted-upload path and feeds fuzzy matching.
    Bound the raw text before Unicode expansion/regex work so a pathological cell
    cannot turn discovery into an unbounded CPU or memory operation.
    """

    if value is None:
        return ""

    raw = str(value)[:SMART_HEADER_TEXT_MAX_CHARS]
    text = unicodedata.normalize("NFKD", raw)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.lower().strip()
    text = _NON_ALNUM_RE.sub(" ", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()

    for pattern, replacement in _ABBREVIATION_REPLACEMENTS:
        text = re.sub(pattern, replacement, text)

    return _WHITESPACE_RE.sub(" ", text).strip()


def normalize_aliases(values: frozenset[str]) -> frozenset[str]:
    """Normalize one alias set at import time without mutating its source."""

    return frozenset(
        normalized
        for value in values
        if (normalized := normalize_header(value))
    )
