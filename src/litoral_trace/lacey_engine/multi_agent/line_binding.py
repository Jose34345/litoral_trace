"""Deterministic commercial/botanical line identity binding.

Binding never asks an LLM to invent identity. It derives the strongest available key
from exact source-row identity or exact evidence in this order: explicit SKU/line key,
source-text SKU/line, Engine 2 row locator, specialized table/row sidecar, then a
deterministic evidence fingerprint. Non-line fields remain unbound.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import re
from typing import Mapping, Sequence
from uuid import UUID

from .contracts import CandidateEnvelope


LINE_BINDING_VERSION = "lacey_document_local_line_binding_v1"


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
_EXPLICIT_LINE_KEY = re.compile(
    r"^(?:(?:LINE|ITEM)\s*(?:NO\.?|NUMBER|#)?\s*[:#]?\s*)?(\d{1,6})$",
    re.I,
)


def source_locator_key(envelope: CandidateEnvelope) -> SourceLocatorKey:
    return (
        envelope.document_id,
        envelope.candidate.page,
        " ".join(envelope.candidate.source_text.split()),
    )


def line_identity_scope(envelope: CandidateEnvelope) -> tuple[str | None, str | None]:
    """Return the comparison scope for one line identity.

    Explicit SKUs are packet-global. Every other derived line identity remains local
    to its source document until deterministic reconciliation rewrites it to a SKU.
    """
    key = envelope.line_item_key
    if key is None:
        return None, str(envelope.document_id)
    if key.startswith("SKU:"):
        return key, None
    return key, str(envelope.document_id)


def derive_line_item_key(
    envelope: CandidateEnvelope,
    *,
    row_locator: RowLocator | None = None,
) -> str | None:
    """Return deterministic line identity for one line-scoped candidate."""
    if envelope.candidate.field_key not in LINE_SCOPED_FIELDS:
        return None

    explicit_key = " ".join((envelope.source_line_key or "").split()).strip()
    if explicit_key:
        if _looks_like_sku(explicit_key):
            return f"SKU:{_normalize_token(explicit_key)}"
        explicit_line = _EXPLICIT_LINE_KEY.fullmatch(explicit_key)
        if explicit_line:
            return f"LINE:{int(explicit_line.group(1))}"

    source = " ".join(envelope.candidate.source_text.split()).strip()
    if source:
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

    table = _normalize_token(envelope.source_table_id or "")
    row_index = envelope.source_row_index
    if table and row_index is not None and row_index >= 0 and envelope.candidate.page >= 1:
        return (
            f"ROW:{envelope.document_id}:P{envelope.candidate.page}:"
            f"T{table}:R{row_index}"
        )

    if not source:
        return None
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


def _semantic_text(value: object) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", str(value or "").upper()).strip()


def _candidate_value(envelope: CandidateEnvelope) -> str:
    return str(
        envelope.candidate.normalized_value
        or envelope.candidate.value
        or ""
    ).strip()


def _taxonomy_phrases(group: Sequence[CandidateEnvelope]) -> frozenset[str]:
    """Return explicit two-token-or-better botanical identities for one SKU group."""
    genera = {
        _semantic_text(_candidate_value(item))
        for item in group
        if item.candidate.field_key == "genus" and _candidate_value(item)
    }
    species = {
        _semantic_text(_candidate_value(item))
        for item in group
        if item.candidate.field_key == "species" and _candidate_value(item)
    }
    phrases: set[str] = set()
    for value in species:
        if len(value.split()) >= 2:
            phrases.add(value)
            continue
        for genus in genera:
            combined = f"{genus} {value}".strip()
            if len(combined.split()) >= 2:
                phrases.add(combined)
    return frozenset(phrases)


def _description_texts(group: Sequence[CandidateEnvelope]) -> frozenset[str]:
    return frozenset(
        _semantic_text(_candidate_value(item))
        for item in group
        if item.candidate.field_key in {"description", "article_component"}
        and _candidate_value(item)
    )


def _group_matches_sku(
    source_group: Sequence[CandidateEnvelope],
    sku_group: Sequence[CandidateEnvelope],
) -> bool:
    """Require explicit semantic agreement; never join lines by ordinal alone."""
    source_descriptions = _description_texts(source_group)
    sku_descriptions = _description_texts(sku_group)
    if source_descriptions and sku_descriptions and source_descriptions & sku_descriptions:
        return True

    taxonomy = _taxonomy_phrases(sku_group)
    return any(
        phrase and phrase in description
        for description in source_descriptions
        for phrase in taxonomy
    )


def _reconcile_to_unique_sku(
    bound: Sequence[CandidateEnvelope],
) -> tuple[CandidateEnvelope, ...]:
    """Canonicalize non-SKU line keys only when one unique SKU is semantically proven.

    A line number is local to a document and is therefore never enough to link two
    documents. Reconciliation requires exact description equality or an explicit
    genus+species phrase contained in the commercial description. Ambiguous matches
    remain separate for human review.
    """
    groups: dict[tuple[str | None, str | None], list[CandidateEnvelope]] = {}
    for envelope in bound:
        if envelope.line_item_key:
            groups.setdefault(line_identity_scope(envelope), []).append(envelope)

    sku_groups = {
        line_key: tuple(group)
        for (line_key, document_scope), group in groups.items()
        if line_key is not None
        and line_key.startswith("SKU:")
        and document_scope is None
    }
    if not sku_groups:
        return tuple(bound)

    proposals: dict[tuple[str | None, str | None], str] = {}
    for source_scope, source_group in groups.items():
        source_key, document_scope = source_scope
        if source_key is None or source_key.startswith("SKU:"):
            continue
        matches = [
            sku_key
            for sku_key, sku_group in sku_groups.items()
            if _group_matches_sku(source_group, sku_group)
        ]
        if len(matches) == 1:
            proposals[source_scope] = matches[0]

    # Multiple documents may independently corroborate the same SKU. What is unsafe
    # is two different local rows inside one document claiming the same SKU.
    reverse_counts: dict[tuple[str | None, str], int] = {}
    for source_scope, sku_key in proposals.items():
        _source_key, document_scope = source_scope
        counter_key = (document_scope, sku_key)
        reverse_counts[counter_key] = reverse_counts.get(counter_key, 0) + 1
    accepted = {
        source_scope: sku_key
        for source_scope, sku_key in proposals.items()
        if reverse_counts[(source_scope[1], sku_key)] == 1
    }
    if not accepted:
        return tuple(bound)

    return tuple(
        replace(
            envelope,
            line_item_key=accepted.get(
                line_identity_scope(envelope),
                envelope.line_item_key,
            ),
        )
        for envelope in bound
    )


def bind_line_items(
    candidates: Sequence[CandidateEnvelope],
    *,
    row_locators: Mapping[SourceLocatorKey, RowLocator] | None = None,
) -> tuple[CandidateEnvelope, ...]:
    row_locators = row_locators or {}
    bound = tuple(
        bind_line_item(candidate, row_locator=row_locators.get(source_locator_key(candidate)))
        for candidate in candidates
    )
    return _reconcile_to_unique_sku(bound)


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
    # value, not reliable evidence for a row identity. Leave it explicitly unbound.
    if len(tokens) < 4 or len(normalized) < 16:
        return None
    return normalized
