"""Conservative, versioned APHIS Lacey HTS implementation-schedule seed.

This is intentionally a small deterministic seed for P1-05, not a complete
authoritative reproduction of APHIS' implementation schedule. Entries included
here are limited to prefixes/codes backed by current APHIS guidance/examples.

A future milestone may replace the seed with a fully curated, versioned schedule
snapshot without changing the catalog API.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re


APHIS_HTS_SCHEDULE_SOURCE = (
    "https://www.aphis.usda.gov/plant-imports/lacey-act/implementation-schedule"
)
APHIS_HTS_SCHEDULE_VERSION = "aphis-lacey-hts-2026-01-13"
_PREFIX = re.compile(r"^\d{4,10}$")


@dataclass(frozen=True, slots=True)
class HtsScheduleEntry:
    """One effective HTS prefix/code in the APHIS Lacey implementation schedule."""

    hts_prefix: str
    effective_from: date
    effective_to: date | None = None

    def __post_init__(self) -> None:
        if not _PREFIX.fullmatch(self.hts_prefix):
            raise ValueError("HTS schedule prefixes must contain 4 to 10 digits.")
        if self.effective_to is not None and self.effective_to < self.effective_from:
            raise ValueError("HTS schedule effective_to cannot precede effective_from.")

    def is_effective(self, on_date: date) -> bool:
        if on_date < self.effective_from:
            return False
        return self.effective_to is None or on_date <= self.effective_to

    def matches(self, hts10: str, *, on_date: date) -> bool:
        return self.is_effective(on_date) and hts10.startswith(self.hts_prefix)


@dataclass(frozen=True, slots=True)
class HtsScheduleCatalog:
    """Immutable prefix catalog with deterministic longest-prefix matching."""

    version: str
    as_of: date
    entries: tuple[HtsScheduleEntry, ...]
    source_url: str = APHIS_HTS_SCHEDULE_SOURCE
    is_complete: bool = False

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("HTS schedule catalog version must be non-empty.")
        identities = tuple(
            (entry.hts_prefix, entry.effective_from, entry.effective_to)
            for entry in self.entries
        )
        if len(identities) != len(set(identities)):
            raise ValueError("Duplicate HTS schedule entries are not allowed.")

    def match(
        self,
        hts10: str,
        *,
        effective_date: date | None = None,
    ) -> HtsScheduleEntry | None:
        """Return the most-specific active entry for a 10-digit HTS code."""

        on_date = effective_date or self.as_of
        candidates = tuple(
            entry
            for entry in self.entries
            if entry.matches(hts10, on_date=on_date)
        )
        if not candidates:
            return None
        return max(
            candidates,
            key=lambda entry: (len(entry.hts_prefix), entry.effective_from),
        )

    def contains(
        self,
        hts10: str,
        *,
        effective_date: date | None = None,
    ) -> bool:
        return self.match(hts10, effective_date=effective_date) is not None


APHIS_HTS_SCHEDULE = HtsScheduleCatalog(
    version=APHIS_HTS_SCHEDULE_VERSION,
    as_of=date(2026, 1, 13),
    entries=(
        # APHIS implementation schedule: 4407, wood sawn/chipped lengthwise,
        # effective April 1, 2009.
        HtsScheduleEntry(
            hts_prefix="4407",
            effective_from=date(2009, 4, 1),
        ),
        # APHIS implementation schedule: 4415, wooden cases/boxes/crates,
        # containers and pallets, effective October 1, 2021.
        HtsScheduleEntry(
            hts_prefix="4415",
            effective_from=date(2021, 10, 1),
        ),
        # APHIS Special Use Designation guidance uses 9401692010 for wooden
        # seats/components. Exact-code seeding avoids treating all 9401 goods
        # as plant products.
        HtsScheduleEntry(
            hts_prefix="9401692010",
            effective_from=date(2024, 12, 1),
        ),
    ),
)


__all__ = [
    "APHIS_HTS_SCHEDULE",
    "APHIS_HTS_SCHEDULE_SOURCE",
    "APHIS_HTS_SCHEDULE_VERSION",
    "HtsScheduleCatalog",
    "HtsScheduleEntry",
]
