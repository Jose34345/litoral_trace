"""Read-only APHIS applicability *triage*, never a filing verdict.

Inputs selected in the UI are what-if assumptions, not verified or persisted
source facts. The actual filing gate and declaration scope are unchanged.
Rule basis: APHIS requirements and entry-type schedule, checked 2026-10-13.
"""
from __future__ import annotations

from dataclasses import dataclass
import re

from litoral_trace.us_lacey.regulatory.catalogs.hts_schedule import APHIS_HTS_SCHEDULE

APHIS_REQUIREMENTS = "https://www.aphis.usda.gov/plant-imports/file-lacey-act-declaration/requirements"
APHIS_ENTRY_CODES = "https://www.aphis.usda.gov/plant-imports/file-lacey-act-declaration/lacey-act-requirements-entry-type-code"
APHIS_HTS_SOURCE = "https://www.aphis.usda.gov/plant-imports/lacey-act/implementation-schedule"
HTS10 = re.compile(r"^\d{10}$")
REQUIRED_TYPES = frozenset({"01", "02", "03", "07", "31", "32", "34", "38", "52"})
NOT_REQUIRED_TYPES = frozenset({"11", "12", "13", "21", "22", "23", "86"})
# Entry type 06 depends on PGA filing timing and FTZ particulars: never auto-clear.
SPECIAL_TYPES = frozenset({"06"})


@dataclass(frozen=True)
class ApplicabilityPreview:
    status: str
    title: str
    explanation: str
    hts10: str
    entry_type: str
    bamboo_origin: str
    is_bamboo: bool
    hts_status: str
    catalog_version: str
    catalog_source: str = APHIS_HTS_SOURCE


def _field_value(detail, field_name: str) -> str:
    rows = [
        row for row in getattr(detail, "fields", ())
        if getattr(row, "field_name", "") == field_name
    ]
    if not rows:
        return ""
    # No guessing between distinct merchandise lines: a multi-line operation
    # must use its separate line-level regulatory analysis.
    distinct = {str(row.effective_value or row.proposed_value or "").strip() for row in rows}
    return next(iter(distinct)) if len(distinct) == 1 else ""


def preview_applicability(detail, *, entry_type: str = "", bamboo_origin: str = "") -> ApplicabilityPreview:
    """Conservative contextual evaluation. This result never waives a gate."""
    entry_type = str(entry_type or "").strip()
    if entry_type not in REQUIRED_TYPES | NOT_REQUIRED_TYPES | SPECIAL_TYPES:
        entry_type = ""
    bamboo_origin = str(bamboo_origin or "").strip().upper()
    if bamboo_origin not in {"CULTIVATED", "WILD", "UNKNOWN"}:
        bamboo_origin = ""
    descriptions = " ".join(
        str(getattr(field, "effective_value", None) or getattr(field, "proposed_value", None) or "")
        for field in getattr(detail, "fields", ())
        if getattr(field, "field_name", "") in {"article_component", "merchandise_description"}
    ).lower()
    is_bamboo = "bamboo" in descriptions
    hts = re.sub(r"[.\s-]", "", _field_value(detail, "hts_code"))
    version = APHIS_HTS_SCHEDULE.version
    matched = HTS10.fullmatch(hts) and APHIS_HTS_SCHEDULE.match(hts)
    hts_status = ("MISSING_OR_INVALID" if not HTS10.fullmatch(hts) else
                  "LISTED" if matched else "NOT_LISTED")
    if not HTS10.fullmatch(hts):
        status, title, explanation = (
            "NEEDS_INFORMATION", "HTS classification must be confirmed",
            "Verify the 10-digit HTS on the entry line with the importer or broker; "
            "the APHIS schedule cannot be evaluated without it.",
        )
    elif not matched:
        status, title, explanation = (
            "PRELIMINARY_EXCEPTION", "HTS appears outside the current APHIS schedule",
            "Verify the classified HTS and effective date before treating the line "
            "as not requiring a declaration.",
        )
    elif not entry_type:
        status, title, explanation = (
            "NEEDS_INFORMATION", "Confirm the CBP entry type",
            "This HTS appears on the APHIS schedule. Whether a declaration is required "
            "also depends on the actual CBP entry type and any applicable exceptions.",
        )
    elif entry_type in SPECIAL_TYPES:
        status, title, explanation = (
            "NEEDS_INFORMATION", "Entry type 06 needs FTZ-specific review",
            "FTZ PGA timing differs for Cargo Release, regular type 06 and weekly "
            "estimates. Confirm the precise entry workflow with the customs broker.",
        )
    elif entry_type in NOT_REQUIRED_TYPES:
        status, title, explanation = (
            "PRELIMINARY_EXCEPTION", "The selected entry type is listed as not requiring Lacey",
            "Verify the CBP entry type against the actual filing. This scenario "
            "is not a declaration waiver or a compliance determination.",
        )
    elif is_bamboo and not bamboo_origin:
        status, title, explanation = (
            "NEEDS_INFORMATION", "Confirm whether the bamboo was cultivated",
            "APHIS distinguishes commercially cultivated bamboo from wild or "
            "unknown-origin bamboo. Request supplier-origin evidence.",
        )
    elif is_bamboo and bamboo_origin == "CULTIVATED":
        status, title, explanation = (
            "PRELIMINARY_EXCEPTION", "Cultivated-bamboo exception may apply",
            "APHIS says cultivated commercial bamboo does not require a Lacey "
            "declaration. The supplier's cultivation claim must be verified; "
            "protected-plant rules and legal sourcing still apply.",
        )
    else:
        status, title, explanation = (
            "POTENTIALLY_REQUIRED", "Declaration appears applicable to this scenario",
            "A scheduled HTS and a declaration-requiring entry type were selected. "
            "Confirm material, exceptions and source-backed facts before filing.",
        )
    return ApplicabilityPreview(
        status=status, title=title, explanation=explanation,
        hts10=hts, entry_type=entry_type, bamboo_origin=bamboo_origin,
        is_bamboo=is_bamboo, hts_status=hts_status, catalog_version=version,
    )
