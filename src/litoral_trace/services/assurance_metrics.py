"""Metrics used to prove whether Assurance v1 reduces operational friction.

These definitions are deliberately product-facing. They measure elimination of
manual re-entry, review speed, detected discrepancies and preflight outcomes.
Persistence/telemetry adapters can consume the snapshot later without changing
the metric contract.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum


class AssuranceMetric(StrEnum):
    PROCESSING_SECONDS = "processing_seconds"
    FIELDS_DETECTED = "fields_detected"
    FIELDS_AUTO_ACCEPTED = "fields_auto_accepted"
    FIELDS_MANUALLY_REVIEWED = "fields_manually_reviewed"
    FIELDS_MANUALLY_CHANGED = "fields_manually_changed"
    AUTOMATIC_DATA_PERCENTAGE = "automatic_data_percentage"
    RECONCILIATION_ISSUES = "reconciliation_issues"
    BLOCKING_ISSUES = "blocking_issues"
    PREFLIGHT_READY = "preflight_ready"
    PREFLIGHT_CONDITIONAL = "preflight_conditional"
    PREFLIGHT_BLOCKED = "preflight_blocked"


@dataclass(slots=True)
class PilotMetricsSnapshot:
    processing_seconds: float = 0.0
    fields_detected: int = 0
    fields_auto_accepted: int = 0
    fields_manually_reviewed: int = 0
    fields_manually_changed: int = 0
    reconciliation_issues: int = 0
    blocking_issues: int = 0
    preflight_ready: int = 0
    preflight_conditional: int = 0
    preflight_blocked: int = 0

    @property
    def automatic_data_percentage(self) -> float:
        """Share of detected fields that did not require human re-entry/review."""
        if self.fields_detected <= 0:
            return 0.0
        value = (self.fields_auto_accepted / self.fields_detected) * 100.0
        return round(min(max(value, 0.0), 100.0), 2)

    def as_metrics(self) -> dict[str, float | int]:
        payload: dict[str, float | int] = asdict(self)
        payload[AssuranceMetric.AUTOMATIC_DATA_PERCENTAGE.value] = (
            self.automatic_data_percentage
        )
        return payload

    def meets_zero_friction_target(self, target_percentage: float = 70.0) -> bool:
        return self.automatic_data_percentage >= target_percentage
