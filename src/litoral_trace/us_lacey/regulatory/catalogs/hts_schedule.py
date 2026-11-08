"""Versioned cumulative APHIS Lacey HTS implementation schedule through Phase VII.

The catalog models the published APHIS schedule at the heading/subheading level.
A prefix denotes every descendant HTS10 code unless APHIS published a narrower
code. Phase VII became effective December 1, 2024. Vetiver essential oil
3301295142 is intentionally excluded from Phase VII per APHIS' final clarification.

Sources:
- 74 FR 45415-45418 (Phases II-IV)
- 80 FR 6681-6683 (Phase V)
- 86 FR 35259-35261 (Phase VI)
- 89 FR / FR Doc. 2024-11901 and APHIS Phase VII clarification
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import re
from types import MappingProxyType
from typing import Mapping


APHIS_HTS_SCHEDULE_SOURCE = (
    "https://www.aphis.usda.gov/plant-imports/lacey-act/implementation-schedule"
)
APHIS_HTS_SCHEDULE_VERSION = "aphis-phase-vii-2024"
_CODE = re.compile(r"^\d{2,10}$")


@dataclass(frozen=True, slots=True)
class HtsScheduleEntry:
    """One effective HTS prefix/code in the APHIS implementation schedule."""

    hts_prefix: str
    effective_from: date
    phase: str = "UNSPECIFIED"
    effective_to: date | None = None

    def __post_init__(self) -> None:
        if not _CODE.fullmatch(self.hts_prefix):
            raise ValueError("HTS schedule prefixes must contain 2 to 10 digits.")
        if self.effective_to is not None and self.effective_to < self.effective_from:
            raise ValueError("HTS schedule effective_to cannot precede effective_from.")
        if not str(self.phase or "").strip():
            raise ValueError("HTS schedule phase must be non-empty.")

    def is_effective(self, on_date: date) -> bool:
        if on_date < self.effective_from:
            return False
        return self.effective_to is None or on_date <= self.effective_to

    def matches(self, hts_code: str, *, on_date: date) -> bool:
        return self.is_effective(on_date) and hts_code.startswith(self.hts_prefix)


@dataclass(frozen=True, slots=True)
class HtsScheduleCatalog:
    """Immutable APHIS catalog with O(length-prefixes) hierarchical lookup."""

    version: str
    as_of: date
    entries: tuple[HtsScheduleEntry, ...]
    source_url: str = APHIS_HTS_SCHEDULE_SOURCE
    is_complete: bool = True
    _active_by_prefix: Mapping[str, tuple[HtsScheduleEntry, ...]] = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("HTS schedule catalog version must be non-empty.")
        identities = tuple(
            (entry.hts_prefix, entry.effective_from, entry.effective_to)
            for entry in self.entries
        )
        if len(identities) != len(set(identities)):
            raise ValueError("Duplicate HTS schedule entries are not allowed.")
        by_prefix: dict[str, list[HtsScheduleEntry]] = {}
        for entry in self.entries:
            by_prefix.setdefault(entry.hts_prefix, []).append(entry)
        object.__setattr__(
            self,
            "_active_by_prefix",
            MappingProxyType(
                {
                    prefix: tuple(sorted(values, key=lambda row: row.effective_from))
                    for prefix, values in by_prefix.items()
                }
            ),
        )

    @property
    def catalog_version(self) -> str:
        return self.version

    def match(
        self,
        hts_code: str,
        *,
        effective_date: date | None = None,
    ) -> HtsScheduleEntry | None:
        """Return the most-specific active ancestor for an HTS code/prefix.

        Queries may contain 2 through 10 digits. Ten-digit customs codes are the
        normal applicability input; shorter values are supported for deterministic
        chapter/heading navigation and catalog diagnostics.
        """
        raw = str(hts_code or "").strip()
        if not _CODE.fullmatch(raw):
            return None
        on_date = effective_date or self.as_of

        # Exact/ancestor lookup is bounded by ten probes and avoids scanning the
        # full cumulative schedule for every merchandise line.
        for size in range(len(raw), 1, -1):
            for entry in reversed(self._active_by_prefix.get(raw[:size], ())):
                if entry.is_effective(on_date):
                    return entry
        return None

    def descendants(
        self,
        hts_prefix: str,
        *,
        effective_date: date | None = None,
    ) -> tuple[HtsScheduleEntry, ...]:
        """Return active schedule entries at or below a 2-10 digit hierarchy node."""
        raw = str(hts_prefix or "").strip()
        if not _CODE.fullmatch(raw):
            return ()
        on_date = effective_date or self.as_of
        return tuple(
            entry
            for entry in self.entries
            if entry.hts_prefix.startswith(raw) and entry.is_effective(on_date)
        )

    def contains(
        self,
        hts_code: str,
        *,
        effective_date: date | None = None,
    ) -> bool:
        return self.match(hts_code, effective_date=effective_date) is not None


def _phase_entries(
    phase: str,
    effective_from: date,
    prefixes: tuple[str, ...],
) -> tuple[HtsScheduleEntry, ...]:
    return tuple(
        HtsScheduleEntry(
            hts_prefix=prefix,
            effective_from=effective_from,
            phase=phase,
        )
        for prefix in prefixes
    )


# Cumulative enforcement schedule before Phase VII.
_PHASE_II = (
    "4401", "4403", "4404", "4406", "4407", "4408", "4409", "4417", "4418",
)
_PHASE_III = (
    "4402", "4412", "4414", "4419", "4420", "9201", "9202",
)
_PHASE_IV = (
    "4421", "6602", "8201", "9302", "93051020", "940169", "950420", "9703",
)
_PHASE_V = (
    "4416003010", "4416003020", "4416003030", "4416006010", "4416006020",
    "4416006030", "4416006040", "4416006050", "4416009020", "4416009040",
    "8211926000", "8215992400", "9401612010", "9401612030", "9401901500",
    "9403304000", "9403404000", "9403504000", "9403604000", "9614002100",
)
_PHASE_VI = (
    "3301295109", "3301295121", "3301295139",
    "4202292000", "4202992000", "4202993000",
    "441012", "4415",
    "9205902000", "9205904020", "9205904060", "9205904080",
    "9206002000", "9207900040", "920992", "9209992000", "9209994040",
    "9209998000", "9620005500",
)

# Phase VII additions as published for December 1, 2024. Mixed 4/6/8/10-digit
# identifiers are deliberate: APHIS publishes both whole headings and narrower
# tariff lines. 3301295142 is omitted by final APHIS clarification.
_PHASE_VII = (
    "1211201090", "1211500000", "1211600000", "1302140100",
    "1401", "1404903000", "1404904000", "1404909020", "1404909040",
    "3301295141", "3301295150", "3303001000", "3307410000", "3307490000",
    "3605000030", "3805901000",
    "4202392000", "4202925000", "4202929315",
    "4413",
    "4501", "4503",
    "460121", "460122", "460129", "460192", "460193", "460194",
    "460211", "460212", "4602190500", "4602191200", "4602193500",
    "4602194500", "4602196000", "4602198000",
    "5608901000",
    "64029923", "64029925", "64035111", "64035910", "64039111",
    "64039910", "64039920",
    "6601", "660320", "66039041",
    "820210", "820310", "82032020", "8203206030", "820520", "820530",
    "820540", "82055115", "8205513030", "8206", "821110", "8211918030",
    "82119290", "82119300", "821410", "821420", "82149030", "82159940",
    "841891", "84189940", "8436800070", "8436990030", "843840", "844520",
    "8446100010", "8447119090", "8447129090", "844811", "844833",
    "8448420000", "84509040", "8451210090", "8451906000", "84529010",
    "8481805060", "84819050",
    "851821", "851822", "8538100000", "8543709810", "8543709820",
    "87089450", "871610", "871620", "8716390010", "8716390020",
    "8716400000", "87168010", "8716901090",
    "8802200115",
    "890110", "890190", "8902", "890321", "890322", "890323", "890331",
    "890332", "890333", "89039305", "89039315", "89039390", "89039906",
    "89039916", "8903999100", "8904", "8905901000",
    "900319", "900390", "900410", "9005100040", "9005100080", "90058040",
    "9005904000", "900691", "90138020", "90141060", "90171080", "9017204000",
    "910212", "91022902", "9105195000", "9105295000", "911180", "91119070",
    "91122080", "911290",
    "92059012", "92059014", "92059015", "92059018", "92059019",
    "92099180", "92099905", "92099910", "92099961",
    "9301903020", "93019060", "930310", "930320", "9303304020",
    "9303304030", "9303308010", "9303308012", "9303308017", "9303308025",
    "9303308030", "93039040", "9303908000", "93052005",
    "940131", "940152", "940153", "940159", "94016140", "94016160",
    "9401790011", "9401790015", "9401790025", "9401790035", "9401806025",
    "9401806028", "94019115", "94019190", "94019925", "94033080",
    "94034060", "94035060", "94035090", "94036080", "94038200", "94038300",
    "940389", "94039100", "94039920", "940410", "94051180", "9405428410",
    "9406100000",
    "9504300020", "9504300040", "9504300060", "9504904000", "95049060",
    "9504909060", "95051015", "95051030", "95061120", "95061140",
    "95061160", "9506198040", "9506290020", "9506290030", "950640",
    "950651", "9506594080", "9506990510", "9506990520", "95069915",
    "95069920", "95069925", "95069945", "95071000", "95079080", "950810",
    "950822", "950829", "950830", "950840",
    "960310", "960330", "96034020", "9603404040", "9603404060", "96039040",
    "96039080", "9605", "96062960", "960810", "960830", "96084040",
    "960910", "9610", "9611", "961320", "96140025", "96151940",
    "96151960", "96159030", "96170010", "96170030", "96170040", "9618",
)


APHIS_HTS_SCHEDULE = HtsScheduleCatalog(
    version=APHIS_HTS_SCHEDULE_VERSION,
    as_of=date(2024, 12, 1),
    entries=(
        *_phase_entries("II", date(2009, 4, 1), _PHASE_II),
        *_phase_entries("III", date(2009, 10, 1), _PHASE_III),
        *_phase_entries("IV", date(2010, 4, 1), _PHASE_IV),
        *_phase_entries("V", date(2015, 8, 6), _PHASE_V),
        *_phase_entries("VI", date(2021, 10, 1), _PHASE_VI),
        *_phase_entries("VII", date(2024, 12, 1), _PHASE_VII),
    ),
    is_complete=True,
)


__all__ = [
    "APHIS_HTS_SCHEDULE",
    "APHIS_HTS_SCHEDULE_SOURCE",
    "APHIS_HTS_SCHEDULE_VERSION",
    "HtsScheduleCatalog",
    "HtsScheduleEntry",
]
