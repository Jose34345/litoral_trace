"""Deterministic commercial/botanical line identity binding.

Binding never asks an LLM to invent identity.  It derives the strongest available key
from exact evidence in the required priority order: SKU, line number, Engine 2 row
locator, then a deterministic evidence fingerprint.  Non-line fields remain unbound.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import re
from typing import Mapping, Sequence
from uuid import UUID

from .contracts import CandidateEnvelope


LINE_SCOPED_FIELDS = frozenset(
    {
        "description",
        "article_component",
        "hts_code",
        "entered_value",
        "genus",
        "species",
        "country_of_harvest",
        "plant_quantity",
        "metric_unit",
    }
)


@dataclass(frozen=True, slots=True)
class RowLocator:
    table_id: str
    page: int
    row_index: int


SourceLocatorKey = tuple[UUID, int, str]

_SKU_LABEL = re.compile(r"\bSKU\s*(?:NO\.?|NUMBER|#|:)?\s*([A-Z0-9][A-Z0-9._/-]{2,})\b", re.I)
_LEADING_LINE_AND_SKU = re.compile(
    r"^\s*(?:LINE\s*)?(\d{1,6})[.)]?\s+([A-Z0-9][A-Z0-9._/-]{2,})\b",
    re.I,
)
_LEADING_SKU = re.compile(r"^\s*([A-Z0-9]*[A-Z][A-Z0-9]*[-_/][A-Z0-9._/-]*\d[A-Z0-9._/-]*)\b", re.I)
_LINE_LABEL = re.compile(
    r"\b(?:LINE|ITEM)\s*(?:NO\.?|NUMBER|#)?\s*[:#]?\s*(\d{1,6})\b",
    re.I,
)


def source_locator_key(envelope: CandidateEnvelope) -> SourceLocatorKey:
    return (
        envelope.document_id,
        envelope.candidate.page,
        " ".join(envelope.candidate.source_text.split()),
    )


def derive_line_item_key(
    envelope: CandidateEnvelope,
    *,
    row_locator: RowLocator | None = None,
) -> str | None:
    """Return deterministic line identity for one line-scoped candidate."""
    if envelope.candidate.field_key not in LINE_SCOPED_FIELDS:
        return None

    source = " ".join(envelope.candidate.source_text.split()).strip()
    if not source:
        return None

    labelled_sku = _SKU_LABEL.search(source)
    if labelled_sku:
        return f"SKU:{_normalize_token(labelled_sku.group(1))}"

    leading = _LEADING_LINE_AND_SKU.search(source)
    if leading and _looks_like_sku(leading.group(2)):
        return f"SKU:{_normalize_token(leading.group(2))}"

    leading_sku = _LEADING_SKU.search(source)
    if leading_sku and _looks_like_sku(leading_sku.group(1)):
        return f"SKU:{_normalize_token(leading_sku.group(1))}"

    labelled_line = _LINE_LABEL.search(source)
    if labelled_line:
        return f"LINE:{int(labelled_line.group(1))}"

    if leading:
        return f"LINE:{int(leading.group(1))}"

    if row_locator is not None:
        table = _normalize_token(row_locator.table_id)
        if table and row_locator.page >= 1 and row_locator.row_index >= 0:
            return (
                f"ROW:{envelope.document_id}:P{row_locator.page}:"
                f"T{table}:R{row_locator.row_index}"
            )

    fingerprint_text = _fingerprintable_text(source)
    if fingerprint_text is None:
        return None
    digest = hashlib.sha256(fingerprint_text.encode("utf-8")).hexdigest()[:24].upper()
    return f"FP:{digest}"


def bind_line_item(
    envelope: CandidateEnvelope,
    *,
    row_locator: RowLocator | None = None,
) -> CandidateEnvelope:
    """Return a new frozen envelope with a derived key, preserving all other data."""
    return replace(
        envelope,
        line_item_key=derive_line_item_key(envelope, row_locator=row_locator),
    )


def bind_line_items(
    candidates: Sequence[CandidateEnvelope],
    *,
    row_locators: Mapping[SourceLocatorKey, RowLocator] | None = None,
) -> tuple[CandidateEnvelope, ...]:
    row_locators = row_locators or {}
    return tuple(
        bind_line_item(candidate, row_locator=row_locators.get(source_locator_key(candidate)))
        for candidate in candidates
    )


def line_item_binding_accuracy(
    expected_keys: Sequence[str | None],
    candidates: Sequence[CandidateEnvelope],
) -> float:
    """Compute exact key accuracy for a labeled validation set."""
    if len(expected_keys) != len(candidates):
        raise ValueError("expected_keys and candidates must have the same length")
    if not expected_keys:
        return 1.0
    correct = sum(
        1 for expected, candidate in zip(expected_keys, candidates) if candidate.line_item_key == expected
    )
    return correct / len(expected_keys)


def _normalize_token(value: str) -> str:
    return re.sub(r"[^A-Z0-9._/-]+", "", value.upper()).strip("-_/.")


def _looks_like_sku(value: str) -> bool:
    token = _normalize_token(value)
    return (
        len(token) >= 4
        and bool(re.search(r"[A-Z]", token))
        and bool(re.search(r"\d", token))
        and any(separator in token for separator in ("-", "_", "/"))
    )


def _fingerprintable_text(source: str) -> str | None:
    normalized = re.sub(r"[^A-Z0-9]+", " ", source.upper()).strip()
    tokens = normalized.split()
    # A one- or two-token snippet such as "Acacia" or "18,900" is evidence for a
    # value, not reliable evidence for a row identity.  Leave it explicitly unbound.
    if len(tokens) < 4 or len(normalized) < 16:
        return None
    return normalized
