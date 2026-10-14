"""Presentation-only labels for the existing CBP entry-type scenario values.

The applicability engine remains the sole authority for the codes and their
outcomes.  This module deliberately maps each existing supported code to plain
language so a user can navigate the advisory scenario without introducing a
new regulatory classification layer.
"""
from __future__ import annotations

from dataclasses import dataclass

from litoral_trace.us_lacey.regulatory.applicability.triage import (
    NOT_REQUIRED_TYPES,
    REQUIRED_TYPES,
    SPECIAL_TYPES,
)


@dataclass(frozen=True, slots=True)
class EntryTypeOption:
    code: str
    group: str
    label: str
    official_title: str
    description: str
    caution: str = ""


_OPTIONS = (
    EntryTypeOption("01", "Standard commercial import", "Standard commercial import", "Formal consumption", "Goods entering the U.S. for regular commercial use."),
    EntryTypeOption("02", "Special customs procedures", "Quota or visa entry", "Quota / visa", "Entry subject to a quota, visa, or another specific requirement.", "Confirm the applicable quota or visa treatment with your broker."),
    EntryTypeOption("03", "Special customs procedures", "AD/CVD entry", "AD/CVD", "Entry subject to antidumping or countervailing duty treatment.", "Confirm current AD/CVD treatment with your broker."),
    EntryTypeOption("06", "Foreign-trade zone", "Foreign-trade zone", "Foreign-trade zone", "Goods entering through an FTZ-related procedure.", "FTZ entries have specific PGA information timing requirements. Confirm the required presentation time with your broker."),
    EntryTypeOption("07", "Special customs procedures", "AD/CVD and quota entry", "AD/CVD + quota", "Entry subject to both AD/CVD and quota treatment.", "Confirm the applicable AD/CVD and quota treatment with your broker."),
    EntryTypeOption("11", "Informal entry", "Informal entry", "Informal", "Entry processed under an eligible informal procedure."),
    EntryTypeOption("12", "Informal entry", "Informal quota entry", "Informal quota", "Informal procedure with quota treatment.", "Confirm that the shipment remains eligible for this procedure."),
    EntryTypeOption("13", "Informal entry", "International mail entry", "International mail", "Entry processed for an eligible international mail shipment."),
    EntryTypeOption("21", "Customs warehouse", "Customs warehouse", "Warehouse", "Goods entering a bonded customs warehouse."),
    EntryTypeOption("22", "Customs warehouse", "Re-warehouse entry", "Re-warehouse", "Goods moved under a bonded warehouse procedure."),
    EntryTypeOption("23", "Temporary import", "Temporary import", "Temporary import", "Goods entering temporarily under an eligible procedure."),
    EntryTypeOption("31", "Withdrawal from warehouse", "Warehouse withdrawal", "Warehouse withdrawal", "Goods released from a customs warehouse."),
    EntryTypeOption("32", "Withdrawal from warehouse", "Quota withdrawal", "Withdrawal quota", "Warehouse withdrawal with quota treatment.", "Confirm the applicable quota treatment with your broker."),
    EntryTypeOption("34", "Withdrawal from warehouse", "AD/CVD withdrawal", "Withdrawal AD/CVD", "Warehouse withdrawal subject to AD/CVD treatment.", "Confirm current AD/CVD treatment with your broker."),
    EntryTypeOption("38", "Withdrawal from warehouse", "Combined warehouse withdrawal", "Withdrawal combined", "Warehouse withdrawal with combined special treatment.", "Confirm the applicable special treatment with your broker."),
    EntryTypeOption("52", "Special customs procedures", "Government entry", "Government", "Entry for an eligible government procedure."),
    EntryTypeOption("86", "Special customs procedures", "Informal de minimis entry", "Informal de minimis", "Official CBP entry type for an eligible informal de minimis procedure.", "Verify current eligibility with your customs broker. Do not assume a low-value shipment qualifies."),
)


def entry_type_options() -> tuple[EntryTypeOption, ...]:
    """Return all and only the codes already supported by the triage engine."""
    supported_codes = REQUIRED_TYPES | NOT_REQUIRED_TYPES | SPECIAL_TYPES
    option_codes = frozenset(option.code for option in _OPTIONS)
    if option_codes != supported_codes:  # defensive guard against silent UX drift
        raise RuntimeError("Entry type option labels do not match the applicability engine")
    return _OPTIONS
