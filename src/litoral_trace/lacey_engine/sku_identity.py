"""Conservative complete-token source identity checks.

Only ASCII case and surrounding whitespace normalize. ``- _ . / +`` remain
significant, including trailing separators. Whitespace, colon, comma, semicolon
and brackets delimit source tokens; other punctuation is never discarded to
manufacture a match. Unsupported SKU spellings abstain instead of truncating.
"""
from __future__ import annotations

import re

SKU_TOKEN_PATTERN = r'[A-Z0-9][A-Z0-9._/+\-]*'
_SOURCE_DELIMITER = r'[\s:;,()\[\]{}]'
_SKU_LABEL_PREFIX = (
    rf'\bSKU(?=$|[\s:#])\s*(?:(?:NO\.?|NUMBER|ID)(?=$|[\s:#])\s*)?(?:[:#]\s*)*'
    rf'(?!(?:NO\.?|NUMBER|ID)(?=$|{_SOURCE_DELIMITER}))'
)
_LABELLED_SKU = re.compile(
    rf'{_SKU_LABEL_PREFIX}({SKU_TOKEN_PATTERN})(?=$|{_SOURCE_DELIMITER})',
    re.IGNORECASE | re.ASCII,
)
# Read the entire labelled token only to veto unsupported spellings. It never
# supplies a positive product identity or expands the accepted SKU grammar.
_RAW_LABELLED_SKU = re.compile(
    rf'{_SKU_LABEL_PREFIX}([^\s:;,()\[\]{{}}]+)(?=$|{_SOURCE_DELIMITER})',
    re.IGNORECASE | re.ASCII,
)

_LEADING_SOURCE_SKU = re.compile(
    rf"^\s*(?:(?:LINE\s*)?\d{{1,6}}[.)]?\s+)?({SKU_TOKEN_PATTERN})(?=$|{_SOURCE_DELIMITER})",
    re.IGNORECASE | re.ASCII,
)


def normalize_sku(value: str) -> str:
    """Comparison-only normalization; never rewrite a source value or sidecar."""
    return value.strip().translate(str.maketrans("abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"))


def is_sku_token(value: str) -> bool:
    return re.fullmatch(SKU_TOKEN_PATTERN, value.strip(), re.IGNORECASE | re.ASCII) is not None


def has_unsupported_source_sku(text: str) -> bool:
    """An explicit but unsupported SKU forbids falling back to a literal alias."""
    comparison_text = " ".join(text.split())
    return any(not is_sku_token(match.group(1)) for match in _RAW_LABELLED_SKU.finditer(comparison_text))


def labelled_source_skus(text: str) -> frozenset[str]:
    comparison_text = " ".join(text.split())
    return frozenset(normalize_sku(match.group(1)) for match in _LABELLED_SKU.finditer(comparison_text))


def observed_source_skus(text: str) -> frozenset[str]:
    """Return explicit labelled and leading row SKUs, without inferred aliases."""
    text = " ".join(text.split())
    seen = set(labelled_source_skus(text))
    leading = _LEADING_SOURCE_SKU.search(text)
    if leading:
        token = normalize_sku(leading.group(1))
        if (
            len(token) >= 4
            and re.search(r"[A-Z]", token)
            and re.search(r"[0-9]", token)
            and any(separator in token for separator in "-_/+")
        ):
            seen.add(token)
    return frozenset(seen)



def has_complete_source_sku_token(identity: str, text: str) -> bool:
    """Check literal token boundaries without asserting global SKU identity.

    A row may contain multiple contradictory source SKUs. In that case the
    quoted source token can still be real evidence while the line must abstain
    from packet-global binding. This predicate never authorizes a SKU key.
    """
    token = identity.strip()
    if not token or not is_sku_token(token):
        return False
    comparison_text = " ".join(text.split())
    return re.search(
        rf'(?<![^\\s:;,()\\[\\]{{}}]){re.escape(token)}(?=$|{_SOURCE_DELIMITER})',
        comparison_text,
        re.IGNORECASE | re.ASCII,
    ) is not None



def has_exact_source_identity(identity: str, text: str) -> bool:
    """Require a complete token, and reject contradictory explicit source SKUs.

    A numeric source-local line key can also be checked here. Its comparison must
    not authorize LINE:1 from LINE:11. A row's verified locator remains usable
    even when an invalid or ambiguous SKU sidecar is rejected.
    """
    # Whitespace normalization is comparison-only. ASCII regex flags still
    # constrain identifier letters/digits; no source text or SKU is rewritten.
    text = " ".join(text.split())
    token = identity.strip()
    if has_unsupported_source_sku(text) or not token or not (re.fullmatch(r"[0-9]+", token) or is_sku_token(token)):
        return False
    labelled = observed_source_skus(text)
    if not token.isdecimal() and labelled and labelled != {normalize_sku(token)}:
        return False
    return re.search(
        rf'(?<![^\s:;,()\[\]{{}}]){re.escape(token)}(?=$|{_SOURCE_DELIMITER})',
        text, re.IGNORECASE | re.ASCII,
    ) is not None
