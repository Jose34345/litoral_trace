"""Fail-safe planning for optional specialized U.S. Lacey projection.

The first boundary is deliberately pure: specialized line identities may plan missing
PPQ plant-line slots, but this module does not mutate operational data. Only stable
line identities derived before fusion are auto-materializable in V1.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
from typing import Iterable

from litoral_trace.lacey_engine.multi_agent.contracts import CandidateEnvelope
from litoral_trace.lacey_engine.multi_agent.line_binding import LINE_SCOPED_FIELDS


_ROW_KEY = re.compile(
    r"^ROW:(?P<document>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}):P(?P<page>[1-9][0-9]*):"
    r"T(?P<table>[A-Z0-9._/-]+):R(?P<row>[0-9]+)$"
)
_LINE_KEY = re.compile(r"^LINE:[1-9][0-9]*$")
_SKU_KEY = re.compile(r"^SKU:[A-Z0-9][A-Z0-9._/-]*$", re.IGNORECASE)
_FINGERPRINT_KEY = re.compile(r"^FP:[A-F0-9]{24,64}$", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class PlannedPlantLine:
    line_item_key: str
    line_reference: str


@dataclass(frozen=True, slots=True)
class LineMaterializationPlan:
    line_references: tuple[str, ...]
    generated_lines: tuple[PlannedPlantLine, ...]
    review_only_line_keys: tuple[str, ...]


def _stable_line_key(value: str) -> bool:
    return bool(
        _SKU_KEY.fullmatch(value)
        or _LINE_KEY.fullmatch(value)
        or _ROW_KEY.fullmatch(value)
    )


def _derived_line_reference(line_item_key: str) -> str:
    digest = hashlib.sha256(line_item_key.encode("utf-8")).hexdigest()[:20].upper()
    return f"LT-{digest}"


def plan_line_materialization(
    candidates: Iterable[CandidateEnvelope],
    *,
    existing_line_references: tuple[str, ...],
) -> LineMaterializationPlan:
    """Plan deterministic missing plant lines without changing existing human order.

    Only line-scoped candidates participate. ``SKU``, positive ``LINE`` and complete
    ``ROW`` identities are stable enough for deterministic materialization. ``FP`` and
    any unknown/malformed non-empty identity remain review-only in V1.
    """
    materializable: set[str] = set()
    review_only: set[str] = set()

    for envelope in candidates:
        if envelope.candidate.field_key not in LINE_SCOPED_FIELDS:
            continue
        raw_key = str(envelope.line_item_key or "").strip()
        if not raw_key:
            continue
        if _stable_line_key(raw_key):
            materializable.add(raw_key)
        elif _FINGERPRINT_KEY.fullmatch(raw_key) or raw_key:
            review_only.add(raw_key)

    existing = tuple(existing_line_references)
    existing_set = set(existing)
    generated: list[PlannedPlantLine] = []
    for line_item_key in sorted(materializable):
        line_reference = _derived_line_reference(line_item_key)
        if line_reference in existing_set:
            continue
        generated.append(
            PlannedPlantLine(
                line_item_key=line_item_key,
                line_reference=line_reference,
            )
        )
        existing_set.add(line_reference)

    return LineMaterializationPlan(
        line_references=existing + tuple(item.line_reference for item in generated),
        generated_lines=tuple(generated),
        review_only_line_keys=tuple(sorted(review_only)),
    )
