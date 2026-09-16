from __future__ import annotations

from datetime import date
import hashlib
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Tuple

from pydantic import BaseModel, ConfigDict, Field


class SpecialUseDesignation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    genus: str = Field(min_length=1)
    species: str = Field(min_length=1)
    category: str = Field(min_length=1)
    members: Tuple[str, ...] = ()
    requires_due_care: bool = False
    current: bool = True
    notes: str | None = None
    source_ref: str = Field(min_length=1)


class SpecialUseSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source: str = Field(min_length=1)
    source_url: str = Field(min_length=1)
    last_modified: date
    snapshot_date: date
    schema_version: int = Field(ge=1)
    designations: Tuple[SpecialUseDesignation, ...]


class SpecialUseRegistry:
    def __init__(self, snapshot: SpecialUseSnapshot, fingerprint: str) -> None:
        self.snapshot = snapshot
        self.fingerprint = fingerprint
        index: dict[tuple[str, str], SpecialUseDesignation] = {}
        for designation in snapshot.designations:
            key = (designation.genus.strip().upper(), designation.species.strip().upper())
            if key in index:
                raise ValueError(f"duplicate APHIS SUD pair: {key}")
            if designation.current:
                index[key] = designation
        self._index: Mapping[tuple[str, str], SpecialUseDesignation] = MappingProxyType(index)

    @classmethod
    def load(cls, path: Path) -> "SpecialUseRegistry":
        raw = path.read_bytes()
        snapshot = SpecialUseSnapshot.model_validate_json(raw)
        fingerprint = hashlib.sha256(raw).hexdigest()
        return cls(snapshot, fingerprint)

    def resolve(self, genus: str, species: str) -> SpecialUseDesignation | None:
        return self._index.get((genus.strip().upper(), species.strip().upper()))
