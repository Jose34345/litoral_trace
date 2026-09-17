from __future__ import annotations

from datetime import date
from enum import Enum
from typing import Tuple

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TaxonResolutionStatus(str, Enum):
    EXACT_ACCEPTED = "EXACT_ACCEPTED"
    EXACT_SYNONYM = "EXACT_SYNONYM"
    GENUS_SPECIES = "GENUS_SPECIES"
    EXACT_ALIAS = "EXACT_ALIAS"
    CANDIDATE = "CANDIDATE"
    AMBIGUOUS = "AMBIGUOUS"
    NOT_FOUND = "NOT_FOUND"
    SPECIAL_USE = "SPECIAL_USE"


class TaxonRecord(BaseModel):
    """One immutable record in a normalized GRIN-style taxonomy snapshot."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str = Field(min_length=1)
    scientific_name: str = Field(min_length=1)
    genus: str = Field(min_length=1)
    species: str = Field(min_length=1)
    is_accepted: bool
    accepted_source_id: str | None = None
    common_names: Tuple[str, ...] = ()
    commercial_aliases: Tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_acceptance_target(self) -> "TaxonRecord":
        if self.is_accepted and self.accepted_source_id is not None:
            raise ValueError("accepted taxa cannot point to accepted_source_id")
        if not self.is_accepted and not self.accepted_source_id:
            raise ValueError("synonym records require accepted_source_id")
        return self


class TaxonomySnapshotManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source: str = Field(min_length=1)
    snapshot_version: str = Field(min_length=1)
    snapshot_date: date
    source_url: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    schema_version: int = Field(ge=1)


class TaxonCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    canonical_source_id: str
    scientific_name: str
    score: float = Field(ge=0.0, le=1.0)
    matched_on: str


class TaxonResolution(BaseModel):
    """Resolution result. Only deterministic exact states may carry authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: TaxonResolutionStatus
    canonical_source_id: str | None = None
    canonical_scientific_name: str | None = None
    genus: str | None = None
    species: str | None = None
    review_required: bool = True
    matched_value: str | None = None
    candidates: Tuple[TaxonCandidate, ...] = ()
