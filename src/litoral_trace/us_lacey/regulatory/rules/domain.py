"""Immutable contracts for deterministic, non-canonical U.S. Lacey rules."""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Mapping


RULESET_VERSION = "us-lacey-regulatory-rules-v3"


class RuleStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    INDETERMINATE = "INDETERMINATE"


class ProtectedPlantStatus(StrEnum):
    CLEAR = "CLEAR"
    PRESENT = "PRESENT"
    UNKNOWN = "UNKNOWN"


class TriState(StrEnum):
    YES = "YES"
    NO = "NO"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class EvidenceRef:
    """Small pointer to an already-existing evidence/source identity."""

    source_type: str
    source_id: str | None = None
    locator: str | None = None


@dataclass(frozen=True, slots=True)
class RuleAssessment:
    """One rule-scoped decision; never an aggregate shipment compliance verdict."""

    rule_id: str
    ruleset_version: str
    status: RuleStatus
    reason_codes: tuple[str, ...]
    explanation: str
    calculation_trace: Mapping[str, str | None] = field(default_factory=dict)
    evidence_refs: tuple[EvidenceRef, ...] = ()
    review_required: bool = False


@dataclass(frozen=True, slots=True)
class DeMinimisInput:
    subject_ref: str
    hts10: str | None
    plant_mass_per_unit_kg: Decimal | None
    total_unit_mass_kg: Decimal | None
    entry_same_hts_plant_mass_kg: Decimal | None
    protected_status: ProtectedPlantStatus
    evidence_refs: tuple[EvidenceRef, ...] = ()


@dataclass(frozen=True, slots=True)
class SpecialCompositeInput:
    subject_ref: str
    small_fibers_more_than_one_plant_kind: TriState
    mechanically_processed_mixed_chemically_bonded: TriState
    thin_solid_plies_or_layers: TriState
    species_determinable_after_due_care: TriState
    evidence_refs: tuple[EvidenceRef, ...] = ()


@dataclass(frozen=True, slots=True)
class SpecialRecycledInput:
    subject_ref: str
    highly_processed_recycled_material: TriState
    species_determinable_after_due_care: TriState
    evidence_refs: tuple[EvidenceRef, ...] = ()


@dataclass(frozen=True, slots=True)
class CompositeMaterialFacts:
    material_normalized: str
    small_fibers_more_than_one_plant_kind: TriState
    mechanically_processed_mixed_chemically_bonded: TriState
    thin_solid_plies_or_layers: TriState
