"""Minimal regulatory output adapter contract. No simulated UFLPA/EUDR engine."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence

from litoral_trace.us_lacey.ppq505 import (
    PPQ505_SHIPMENT_FIELDS, PPQ505_PLANT_FIELDS, PPQ505_SHIPMENT_REFERENCE,
    PpqField, PpqRequirement,
)


@dataclass(frozen=True)
class RegulationBlock:
    code: str
    subject: str


class RegulatoryOutputAdapter(Protocol):
    regulation_id: str
    def required_fields(self, line_references: Sequence[str]) -> tuple[tuple[str, PpqField], ...]: ...
    def assess(self, rules: Sequence[Mapping[str, Any]], ruleset_version: str) -> tuple[RegulationBlock, ...]: ...


class LaceyRegulatoryOutputAdapter:
    regulation_id = "US_LACEY_ACT"
    output_types = ("PPQ505_PREPARATION", "LAWGS_MERCHANDISE_XML", "LACEY_EXCEL")

    def required_fields(self, line_references: Sequence[str]) -> tuple[tuple[str, PpqField], ...]:
        shipment = ((PPQ505_SHIPMENT_REFERENCE, item) for item in PPQ505_SHIPMENT_FIELDS
                    if item.requirement == PpqRequirement.REQUIRED)
        lines = ((line, item) for line in line_references for item in PPQ505_PLANT_FIELDS
                 if item.requirement == PpqRequirement.REQUIRED)
        return tuple(shipment) + tuple(lines)

    def assess(self, rules: Sequence[Mapping[str, Any]], ruleset_version: str) -> tuple[RegulationBlock, ...]:
        blocks: list[RegulationBlock] = []
        if not rules or not ruleset_version:
            return (RegulationBlock("RULES_NOT_EVALUATED", self.regulation_id),)
        for rule in rules:
            identifier = str(rule.get("id") or "")
            if not identifier or str(rule.get("version") or "") != ruleset_version or (
                not rule.get("inputs_fingerprint")
            ):
                blocks.append(RegulationBlock("UNVERSIONED_RULE", identifier or "unknown"))
            # A rule PASS is only a rule-level outcome; never a shipment verdict.
            if bool(rule.get("blocking")) and str(rule.get("status") or "").upper() not in {"PASS", "NOT_APPLICABLE"}:
                blocks.append(RegulationBlock("BLOCKING_REGULATORY_RULE", identifier or "unknown"))
        return tuple(blocks)


LACEY_ADAPTER = LaceyRegulatoryOutputAdapter()

def rules_from_current_assessment(
    view: Any,
    *, current_source_set_fingerprint: str,
    current_ruleset_version: str,
) -> tuple[dict[str, Any], ...]:
    """Adapt existing read-only CURRENT RegulatoryAssessmentView into V2.

    The native payload uses rule_id/ruleset_version and review_required; this
    adapter never evaluates missing facts and never upgrades a rule result.
    The authority loader must call the fenced current-only DB read function.
    """
    if view is None or str(getattr(view, "status", "")) != "CURRENT":
        return ()
    if str(getattr(view, "source_set_fingerprint", "")) != current_source_set_fingerprint:
        return ()
    if str(getattr(view, "ruleset_version", "")) != current_ruleset_version:
        return ()
    fingerprint = str(getattr(view, "input_fingerprint", "") or "")
    if not fingerprint:
        return ()
    raw = getattr(view, "payload", {}) or {}
    if not isinstance(raw, Mapping):
        return ()
    result: list[dict[str, Any]] = []
    for assessment in raw.get("assessments", ()):
        if not isinstance(assessment, Mapping):
            return ()
        if not assessment.get("rule_id") or not assessment.get("status"):
            return ()
        result.append({
            "id": str(assessment["rule_id"]),
            "version": current_ruleset_version,
            "inputs_fingerprint": fingerprint,
            "status": str(assessment["status"]),
            "blocking": bool(assessment.get("is_blocking", assessment.get("review_required", False))),
            "subject_ref": str(assessment.get("subject_ref") or ""),
            "evidence_refs": list(assessment.get("evidence_refs") or []),
            "reason_codes": list(assessment.get("reason_codes") or []),
        })
    return tuple(result)
