"""Offline, deterministic Shadow-vs-Canonical benchmark for U.S. Lacey.

This module is evaluation infrastructure only. Production extraction, reconciliation,
publication, routing and HTTP code must never import it.

The comparator deliberately performs no fuzzy or semantic repair. It measures the
normalized values already emitted by Engine 2 Shadow and Canonical ShipmentTruth so
binding/authority losses remain visible instead of being normalized away by the
benchmark itself.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from enum import Enum
import json
from pathlib import Path
import sys
from typing import TextIO

from pydantic import BaseModel, Field


_SHIPMENT_ENTITY = "__shipment__"
_SHADOW_SUPPORTED_STATES = frozenset({"SUPPORTED", "SUPPORTED_MULTIPLE"})
_CANONICAL_SUPPORTED_STATES = frozenset({"SUPPORTED", "SUPPORTED_MULTIPLE"})
_PUBLISHED_SUPPORTED_STATES = frozenset({"FOUND", "MATCHED"})
_CANONICAL_CONFLICT_STATES = frozenset({"CONFLICT", "REVIEW_REQUIRED"})
_CANONICAL_CONFLICT_ISSUES = frozenset({"INCONSISTENT_SET"})

_FIELD_LABELS = {
    "plant_quantity": "Quantity",
    "species": "Species",
    "genus": "Genus",
    "country_of_harvest": "Country of Harvest",
    "metric_unit": "Metric Unit",
    "entered_value": "Entered Value",
    "hts_code": "HTS Code",
    "importer_address": "Importer Address",
    "manufacturer_id": "Manufacturer ID",
}


class _StrictModel(BaseModel):
    class Config:
        extra = "forbid"


class Engine2EvidenceSnapshot(_StrictModel):
    scope: str
    line_key: str | None = None
    component_key: str | None = None
    normalized_value: str
    source_filename: str
    page: int = Field(ge=1)
    evidence_verified: bool = True
    language: str | None = None


class Engine2FieldSnapshot(_StrictModel):
    field_key: str
    state: str
    values: tuple[str, ...] = ()
    evidence: tuple[Engine2EvidenceSnapshot, ...] = ()


class Engine2DossierSnapshot(_StrictModel):
    availability: str
    fields: tuple[Engine2FieldSnapshot, ...] = ()


class CanonicalFieldSnapshot(_StrictModel):
    state: str
    values: tuple[str, ...] = ()
    publication_status: str | None = None
    issue_types: tuple[str, ...] = ()


class CanonicalPlantLineSnapshot(_StrictModel):
    entity_key: str
    fields: dict[str, CanonicalFieldSnapshot] = Field(default_factory=dict)


class ShipmentTruthSnapshot(_StrictModel):
    shipment_fields: dict[str, CanonicalFieldSnapshot] = Field(default_factory=dict)
    plant_lines: tuple[CanonicalPlantLineSnapshot, ...] = ()


class ExpectedDiff(_StrictModel):
    comparable_slots: int = Field(ge=0)
    agreement_count: int = Field(ge=0)
    shadow_supported_but_canonical_missing: int = Field(ge=0)
    canonical_supported_but_shadow_missing: int = Field(ge=0)
    false_conflict_count: int = Field(ge=0)


class BenchmarkFixture(_StrictModel):
    version: str
    pack_id: str
    description: str
    shadow: Engine2DossierSnapshot
    canonical: ShipmentTruthSnapshot
    expected_diff: ExpectedDiff


class ComparisonOutcome(str, Enum):
    AGREEMENT = "agreement"
    SHADOW_SUPPORTED_BUT_CANONICAL_MISSING = (
        "shadow_supported_but_canonical_missing"
    )
    CANONICAL_SUPPORTED_BUT_SHADOW_MISSING = (
        "canonical_supported_but_shadow_missing"
    )
    FALSE_CONFLICT = "false_conflict"


class _LayerSlot(_StrictModel):
    scope: str
    entity_key: str
    field_key: str
    state: str
    values: tuple[str, ...]
    publication_status: str | None = None
    issue_types: tuple[str, ...] = ()


class DiffEntry(_StrictModel):
    scope: str
    entity_key: str
    field_key: str
    shadow_state: str | None
    canonical_state: str | None
    shadow_values: tuple[str, ...] = ()
    canonical_values: tuple[str, ...] = ()
    canonical_issue_types: tuple[str, ...] = ()
    outcome: ComparisonOutcome


class FieldDiffSummary(_StrictModel):
    field_key: str
    comparable_slots: int
    agreement_count: int
    shadow_supported_but_canonical_missing: int
    canonical_supported_but_shadow_missing: int
    false_conflict_count: int
    agreement_rate: float
    false_conflict_rate: float


class ShadowCanonicalScorecard(_StrictModel):
    comparable_slots: int
    ignored_slots: int
    agreement_count: int
    shadow_supported_but_canonical_missing: int
    canonical_supported_but_shadow_missing: int
    false_conflict_count: int
    agreement_rate: float
    false_conflict_rate: float
    shadow_supported_but_canonical_missing_rate: float
    canonical_supported_but_shadow_missing_rate: float
    by_field: dict[str, FieldDiffSummary]
    entries: tuple[DiffEntry, ...]

    def matches_expected(self, expected: ExpectedDiff) -> bool:
        return (
            self.comparable_slots == expected.comparable_slots
            and self.agreement_count == expected.agreement_count
            and self.shadow_supported_but_canonical_missing
            == expected.shadow_supported_but_canonical_missing
            and self.canonical_supported_but_shadow_missing
            == expected.canonical_supported_but_shadow_missing
            and self.false_conflict_count == expected.false_conflict_count
        )


def _normalized_values(values: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    """Compare already-normalized layer values without adding new semantics."""
    return tuple(
        sorted(
            {
                str(value).strip()
                for value in values
                if str(value).strip()
            }
        )
    )


def _slot_key(slot: _LayerSlot) -> tuple[str, str, str]:
    return slot.scope, slot.entity_key, slot.field_key


def _shadow_entity(
    evidence: Engine2EvidenceSnapshot,
    *,
    ordinal: int,
) -> tuple[str, str]:
    scope = str(evidence.scope or "").strip().upper()
    if scope == "SHIPMENT":
        return "SHIPMENT", _SHIPMENT_ENTITY

    line_key = str(evidence.line_key or "").strip()
    component_key = str(evidence.component_key or "").strip()
    if line_key:
        return "PLANT_LINE", line_key
    if component_key:
        return "PLANT_LINE", component_key

    # Unbound evidence must never be silently compared as shipment-wide truth.
    return (
        "PLANT_LINE",
        f"__unbound__:{evidence.source_filename}:p{evidence.page}:{ordinal}",
    )


def _flatten_shadow(
    dossier: Engine2DossierSnapshot,
) -> dict[tuple[str, str, str], _LayerSlot]:
    grouped: dict[
        tuple[str, str, str],
        dict[str, object],
    ] = {}

    for field in dossier.fields:
        for ordinal, evidence in enumerate(field.evidence, start=1):
            if evidence.evidence_verified is not True:
                continue
            value = str(evidence.normalized_value or "").strip()
            if not value:
                continue

            scope, entity_key = _shadow_entity(evidence, ordinal=ordinal)
            key = (scope, entity_key, field.field_key)
            bucket = grouped.setdefault(
                key,
                {
                    "scope": scope,
                    "entity_key": entity_key,
                    "field_key": field.field_key,
                    "state": field.state,
                    "values": [],
                },
            )
            bucket["values"].append(value)  # type: ignore[union-attr]

    return {
        key: _LayerSlot(
            scope=str(payload["scope"]),
            entity_key=str(payload["entity_key"]),
            field_key=str(payload["field_key"]),
            state=str(payload["state"]),
            values=_normalized_values(payload["values"]),  # type: ignore[arg-type]
        )
        for key, payload in grouped.items()
    }


def _flatten_canonical(
    truth: ShipmentTruthSnapshot,
) -> dict[tuple[str, str, str], _LayerSlot]:
    slots: dict[tuple[str, str, str], _LayerSlot] = {}

    for field_key, field in truth.shipment_fields.items():
        slot = _LayerSlot(
            scope="SHIPMENT",
            entity_key=_SHIPMENT_ENTITY,
            field_key=field_key,
            state=field.state,
            values=_normalized_values(field.values),
            publication_status=field.publication_status,
            issue_types=tuple(sorted(set(field.issue_types))),
        )
        slots[_slot_key(slot)] = slot

    for line in truth.plant_lines:
        entity_key = str(line.entity_key).strip()
        for field_key, field in line.fields.items():
            slot = _LayerSlot(
                scope="PLANT_LINE",
                entity_key=entity_key,
                field_key=field_key,
                state=field.state,
                values=_normalized_values(field.values),
                publication_status=field.publication_status,
                issue_types=tuple(sorted(set(field.issue_types))),
            )
            slots[_slot_key(slot)] = slot

    return slots


def _shadow_supported(slot: _LayerSlot | None) -> bool:
    return bool(
        slot is not None
        and slot.state in _SHADOW_SUPPORTED_STATES
        and slot.values
    )


def _canonical_supported(slot: _LayerSlot | None) -> bool:
    if slot is None or slot.state not in _CANONICAL_SUPPORTED_STATES or not slot.values:
        return False
    if slot.publication_status is None:
        return True
    return slot.publication_status in _PUBLISHED_SUPPORTED_STATES


def _canonical_conflicted(slot: _LayerSlot | None) -> bool:
    return bool(
        slot is not None
        and (
            slot.state in _CANONICAL_CONFLICT_STATES
            or bool(_CANONICAL_CONFLICT_ISSUES.intersection(slot.issue_types))
        )
    )


def _classify(
    shadow: _LayerSlot | None,
    canonical: _LayerSlot | None,
) -> ComparisonOutcome | None:
    shadow_supported = _shadow_supported(shadow)
    canonical_supported = _canonical_supported(canonical)

    if shadow_supported and _canonical_conflicted(canonical):
        return ComparisonOutcome.FALSE_CONFLICT

    if shadow_supported and canonical_supported:
        if shadow is not None and canonical is not None and shadow.values == canonical.values:
            return ComparisonOutcome.AGREEMENT
        return ComparisonOutcome.FALSE_CONFLICT

    if shadow_supported and not canonical_supported:
        return ComparisonOutcome.SHADOW_SUPPORTED_BUT_CANONICAL_MISSING

    if canonical_supported and not shadow_supported:
        return ComparisonOutcome.CANONICAL_SUPPORTED_BUT_SHADOW_MISSING

    return None


def _rate(numerator: int, denominator: int) -> float:
    return 0.0 if denominator == 0 else numerator / denominator


def _field_summary(
    field_key: str,
    entries: tuple[DiffEntry, ...],
) -> FieldDiffSummary:
    comparable = len(entries)
    agreement = sum(item.outcome is ComparisonOutcome.AGREEMENT for item in entries)
    shadow_only = sum(
        item.outcome
        is ComparisonOutcome.SHADOW_SUPPORTED_BUT_CANONICAL_MISSING
        for item in entries
    )
    canonical_only = sum(
        item.outcome
        is ComparisonOutcome.CANONICAL_SUPPORTED_BUT_SHADOW_MISSING
        for item in entries
    )
    conflicts = sum(
        item.outcome is ComparisonOutcome.FALSE_CONFLICT
        for item in entries
    )
    return FieldDiffSummary(
        field_key=field_key,
        comparable_slots=comparable,
        agreement_count=agreement,
        shadow_supported_but_canonical_missing=shadow_only,
        canonical_supported_but_shadow_missing=canonical_only,
        false_conflict_count=conflicts,
        agreement_rate=_rate(agreement, comparable),
        false_conflict_rate=_rate(conflicts, comparable),
    )


def evaluate_shadow_canonical_diff(
    shadow: Engine2DossierSnapshot,
    canonical: ShipmentTruthSnapshot,
) -> ShadowCanonicalScorecard:
    """Compare Shadow and Canonical by exact field/entity slot.

    A slot is comparable only when at least one layer has a supported value or
    Canonical explicitly conflicts with a supported Shadow value. Slots where
    neither layer has publishable/supportable data are tracked as ignored so
    missing-on-both cannot inflate agreement.
    """
    shadow_slots = _flatten_shadow(shadow)
    canonical_slots = _flatten_canonical(canonical)
    keys = sorted(set(shadow_slots) | set(canonical_slots))

    entries: list[DiffEntry] = []
    ignored = 0
    for key in keys:
        shadow_slot = shadow_slots.get(key)
        canonical_slot = canonical_slots.get(key)
        outcome = _classify(shadow_slot, canonical_slot)
        if outcome is None:
            ignored += 1
            continue

        scope, entity_key, field_key = key
        entries.append(
            DiffEntry(
                scope=scope,
                entity_key=entity_key,
                field_key=field_key,
                shadow_state=None if shadow_slot is None else shadow_slot.state,
                canonical_state=(
                    None if canonical_slot is None else canonical_slot.state
                ),
                shadow_values=(
                    () if shadow_slot is None else shadow_slot.values
                ),
                canonical_values=(
                    () if canonical_slot is None else canonical_slot.values
                ),
                canonical_issue_types=(
                    ()
                    if canonical_slot is None
                    else canonical_slot.issue_types
                ),
                outcome=outcome,
            )
        )

    ordered_entries = tuple(entries)
    comparable = len(ordered_entries)
    agreement = sum(
        item.outcome is ComparisonOutcome.AGREEMENT
        for item in ordered_entries
    )
    shadow_only = sum(
        item.outcome
        is ComparisonOutcome.SHADOW_SUPPORTED_BUT_CANONICAL_MISSING
        for item in ordered_entries
    )
    canonical_only = sum(
        item.outcome
        is ComparisonOutcome.CANONICAL_SUPPORTED_BUT_SHADOW_MISSING
        for item in ordered_entries
    )
    conflicts = sum(
        item.outcome is ComparisonOutcome.FALSE_CONFLICT
        for item in ordered_entries
    )

    by_field_entries: dict[str, list[DiffEntry]] = defaultdict(list)
    for item in ordered_entries:
        by_field_entries[item.field_key].append(item)
    by_field = {
        field_key: _field_summary(field_key, tuple(items))
        for field_key, items in sorted(by_field_entries.items())
    }

    return ShadowCanonicalScorecard(
        comparable_slots=comparable,
        ignored_slots=ignored,
        agreement_count=agreement,
        shadow_supported_but_canonical_missing=shadow_only,
        canonical_supported_but_shadow_missing=canonical_only,
        false_conflict_count=conflicts,
        agreement_rate=_rate(agreement, comparable),
        false_conflict_rate=_rate(conflicts, comparable),
        shadow_supported_but_canonical_missing_rate=_rate(
            shadow_only,
            comparable,
        ),
        canonical_supported_but_shadow_missing_rate=_rate(
            canonical_only,
            comparable,
        ),
        by_field=by_field,
        entries=ordered_entries,
    )


def load_benchmark_fixture(path: Path) -> BenchmarkFixture:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return BenchmarkFixture(**payload)


def _percent(value: float) -> str:
    return f"{100.0 * value:.2f}%"


def _field_label(field_key: str) -> str:
    return _FIELD_LABELS.get(
        field_key,
        field_key.replace("_", " ").title(),
    )


def print_report(
    report: ShadowCanonicalScorecard,
    *,
    pack_id: str,
    stream: TextIO = sys.stdout,
) -> None:
    """Print a deterministic console scorecard with global and per-field delta."""
    print(f"Shadow vs Canonical Diff — {pack_id}", file=stream)
    print("=" * 88, file=stream)
    print(
        f"{'Metric':48} {'Count':>12} {'Rate':>12}",
        file=stream,
    )
    print("-" * 88, file=stream)

    metrics = (
        (
            "agreement_rate",
            report.agreement_count,
            report.agreement_rate,
        ),
        (
            "shadow_supported_but_canonical_missing",
            report.shadow_supported_but_canonical_missing,
            report.shadow_supported_but_canonical_missing_rate,
        ),
        (
            "canonical_supported_but_shadow_missing",
            report.canonical_supported_but_shadow_missing,
            report.canonical_supported_but_shadow_missing_rate,
        ),
        (
            "false_conflict_rate",
            report.false_conflict_count,
            report.false_conflict_rate,
        ),
    )
    for label, count, rate in metrics:
        print(
            f"{label:48} {f'{count}/{report.comparable_slots}':>12} {_percent(rate):>12}",
            file=stream,
        )

    print(
        f"{'ignored_both_unsupported':48} {report.ignored_slots:>12} {'-':>12}",
        file=stream,
    )
    print("", file=stream)
    print("By field", file=stream)
    print("-" * 108, file=stream)
    print(
        f"{'Field':24} {'Slots':>7} {'Agree':>7} {'Shadow>Canon':>14} "
        f"{'Canon>Shadow':>14} {'FalseConflict':>15} {'Agreement':>12}",
        file=stream,
    )
    print("-" * 108, file=stream)
    for field_key, summary in report.by_field.items():
        print(
            f"{_field_label(field_key):24} "
            f"{summary.comparable_slots:>7} "
            f"{summary.agreement_count:>7} "
            f"{summary.shadow_supported_but_canonical_missing:>14} "
            f"{summary.canonical_supported_but_shadow_missing:>14} "
            f"{summary.false_conflict_count:>15} "
            f"{_percent(summary.agreement_rate):>12}",
            file=stream,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Offline Shadow-vs-Canonical U.S. Lacey benchmark."
    )
    parser.add_argument(
        "fixture",
        type=Path,
        help="Path to a P2 shadow/canonical benchmark fixture JSON.",
    )
    args = parser.parse_args(argv)

    fixture = load_benchmark_fixture(args.fixture)
    report = evaluate_shadow_canonical_diff(
        fixture.shadow,
        fixture.canonical,
    )
    print_report(report, pack_id=fixture.pack_id)

    if not report.matches_expected(fixture.expected_diff):
        print(
            "ERROR: current diff does not match fixture expected_diff baseline.",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
