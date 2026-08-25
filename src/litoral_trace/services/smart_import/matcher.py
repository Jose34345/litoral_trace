"""Deterministic column matching and confidence scoring for Smart Import."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from difflib import SequenceMatcher
import math
import re
from typing import Any

from .aliases import LOTES_CANONICAL_FIELDS
from .contracts import (
    CanonicalFieldSpec,
    ColumnMapping,
    MappingDecision,
    MappingStatus,
)
from .normalize import normalize_aliases, normalize_header


_CUIT_RE = re.compile(r"^\d{2}-?\d{8}-?\d$")


def _safe_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        text = value.strip().replace(",", ".")
        if not text:
            return None
        try:
            number = float(text)
        except ValueError:
            return None
    else:
        return None
    if not math.isfinite(number):
        return None
    return number


def _fuzzy_score(source: str, aliases: frozenset[str]) -> float:
    if not source:
        return 0.0
    if source in aliases:
        return 1.0
    return max(
        (SequenceMatcher(None, source, alias).ratio() for alias in aliases),
        default=0.0,
    )


def _semantic_score(
    semantic_type: str,
    sample_values: Sequence[Any],
) -> tuple[float, str | None]:
    values = [value for value in sample_values if value not in (None, "")]
    if not values:
        return 0.0, None

    numeric_values = [
        number
        for value in values
        if (number := _safe_float(value)) is not None
    ]
    numeric_ratio = len(numeric_values) / len(values)

    if semantic_type == "latitude" and numeric_values:
        in_range = sum(-90 <= value <= 90 for value in numeric_values)
        ratio = in_range / len(numeric_values)
        return numeric_ratio * ratio, "valores compatibles con latitud"

    if semantic_type == "longitude" and numeric_values:
        in_range = sum(-180 <= value <= 180 for value in numeric_values)
        ratio = in_range / len(numeric_values)
        return numeric_ratio * ratio, "valores compatibles con longitud"

    if semantic_type in {"area_ha", "volume_ton_in", "volume_ton_out"}:
        if not numeric_values:
            return 0.0, None
        non_negative = sum(value >= 0 for value in numeric_values)
        ratio = non_negative / len(numeric_values)
        return numeric_ratio * ratio, "valores numéricos no negativos"

    if semantic_type == "supplier":
        string_values = [str(value).strip() for value in values]
        cuit_ratio = sum(
            bool(_CUIT_RE.fullmatch(value.replace(" ", "")))
            for value in string_values
        ) / len(string_values)
        if cuit_ratio:
            return min(1.0, 0.55 + 0.45 * cuit_ratio), "patrón CUIT detectado"
        return 0.35, "valores textuales compatibles con proveedor"

    if semantic_type in {"identifier", "product"}:
        text_ratio = sum(bool(str(value).strip()) for value in values) / len(values)
        return 0.45 * text_ratio, "valores textuales presentes"

    return 0.0, None


def _decision_thresholds(field: CanonicalFieldSpec) -> tuple[float, float]:
    # Critical traceability fields require stronger confidence before auto-map.
    if field.high_risk:
        return 0.96, 0.78
    return 0.93, 0.75


def score_column_for_field(
    source_header: object,
    sample_values: Sequence[Any],
    field: CanonicalFieldSpec,
) -> tuple[float, tuple[str, ...]]:
    """Score one source column against one canonical field, from 0.0 to 1.0."""

    source = normalize_header(source_header)
    aliases = normalize_aliases(field.aliases | frozenset({field.name}))
    name_score = _fuzzy_score(source, aliases)
    semantic_score, semantic_reason = _semantic_score(
        field.semantic_type,
        sample_values,
    )

    # Header semantics dominate. Content only corroborates and must never make a
    # semantically unrelated generic numeric column auto-map on its own.
    confidence = min(1.0, (0.82 * name_score) + (0.18 * semantic_score))
    reasons: list[str] = []

    if source in aliases:
        reasons.append("alias exacto normalizado")
    elif name_score >= 0.75:
        reasons.append(f"similitud de encabezado {name_score:.0%}")

    if semantic_reason and semantic_score >= 0.5:
        reasons.append(semantic_reason)

    return confidence, tuple(reasons)


def map_source_column(
    source_header: object,
    sample_values: Sequence[Any],
    *,
    source_index: int,
    fields: Iterable[CanonicalFieldSpec] = LOTES_CANONICAL_FIELDS,
) -> ColumnMapping:
    """Return the safest deterministic mapping proposal for one source column."""

    normalized = normalize_header(source_header)
    scored = [
        (
            field,
            *score_column_for_field(source_header, sample_values, field),
        )
        for field in fields
    ]
    scored.sort(key=lambda item: item[1], reverse=True)

    if not scored:
        decision = MappingDecision(
            canonical_field=None,
            confidence=0.0,
            status=MappingStatus.IGNORED,
            reasons=("sin campos canónicos configurados",),
        )
    else:
        best_field, best_score, reasons = scored[0]
        second_score = scored[1][1] if len(scored) > 1 else 0.0
        margin = best_score - second_score
        auto_threshold, confirm_threshold = _decision_thresholds(best_field)

        # A narrow margin is ambiguous even when the top score is high.
        if best_score >= auto_threshold and margin >= 0.12:
            status = MappingStatus.AUTO
        elif best_score >= confirm_threshold and margin >= 0.07:
            status = MappingStatus.CONFIRM
        elif best_score >= 0.58:
            status = MappingStatus.MANUAL
        else:
            decision = MappingDecision(
                canonical_field=None,
                confidence=best_score,
                status=MappingStatus.IGNORED,
                reasons=reasons or ("sin coincidencia suficientemente segura",),
            )
            return ColumnMapping(
                source_column=str(source_header or ""),
                source_index=source_index,
                normalized_source=normalized,
                decision=decision,
                sample_values=tuple(sample_values[:8]),
            )

        decision = MappingDecision(
            canonical_field=best_field.name,
            confidence=best_score,
            status=status,
            reasons=reasons,
        )

    return ColumnMapping(
        source_column=str(source_header or ""),
        source_index=source_index,
        normalized_source=normalized,
        decision=decision,
        sample_values=tuple(sample_values[:8]),
    )


def resolve_duplicate_targets(
    mappings: Sequence[ColumnMapping],
) -> tuple[ColumnMapping, ...]:
    """Prevent two source columns from being auto-mapped to one canonical field."""

    winners: dict[str, ColumnMapping] = {}
    result = list(mappings)

    for mapping in mappings:
        target = mapping.decision.canonical_field
        if target is None:
            continue
        current = winners.get(target)
        if current is None or mapping.decision.confidence > current.decision.confidence:
            winners[target] = mapping

    for index, mapping in enumerate(result):
        target = mapping.decision.canonical_field
        if target is None or winners.get(target) is mapping:
            continue
        result[index] = ColumnMapping(
            source_column=mapping.source_column,
            source_index=mapping.source_index,
            normalized_source=mapping.normalized_source,
            decision=MappingDecision(
                canonical_field=target,
                confidence=mapping.decision.confidence,
                status=MappingStatus.MANUAL,
                reasons=mapping.decision.reasons + ("destino canónico duplicado",),
            ),
            sample_values=mapping.sample_values,
        )

    return tuple(result)
