from __future__ import annotations

from datetime import date
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from typing import Mapping, Tuple

from pydantic import BaseModel, ConfigDict, Field


class RegulatorySource(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_ref: str = Field(min_length=1)
    url: str = Field(min_length=1)
    last_modified: date


class UnitRule(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    dimension: str = Field(min_length=1)
    to_base: Decimal = Field(gt=0)


class DeMinimisRule(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    max_unit_percent: Decimal = Field(gt=0)
    max_entry_kg_same_hts: Decimal = Field(gt=0)
    source_ref: str = Field(min_length=1)


class CompositeRule(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    known_composite: Tuple[str, ...]
    known_non_composite: Tuple[str, ...]
    source_ref: str = Field(min_length=1)


class CompletenessRule(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_ref: str = Field(min_length=1)


class RegulatoryRuleSet(BaseModel):
    """Versioned offline regulatory facts used by the deterministic evaluator."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    ruleset_id: str = Field(min_length=1)
    version: str = Field(min_length=1)
    snapshot_date: date
    schema_version: int = Field(ge=1)
    sources: Tuple[RegulatorySource, ...]
    valid_units: dict[str, UnitRule]
    de_minimis: DeMinimisRule
    composite: CompositeRule
    completeness: CompletenessRule

    @classmethod
    def load(cls, path: Path) -> "RegulatoryRuleSet":
        return cls.model_validate_json(path.read_text(encoding="utf-8"))

    @property
    def fingerprint(self) -> str:
        canonical = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    @property
    def source_refs(self) -> frozenset[str]:
        return frozenset(source.source_ref for source in self.sources)
