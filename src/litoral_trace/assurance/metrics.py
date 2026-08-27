"""Product metrics for the Assurance v1 pilot.

The sprint is successful only if it removes manual work and finds operational
problems earlier. These metrics are therefore kept independent from UI counters.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass


ZERO_FRICTION_TARGET_PERCENTAGE = 70.0
REVIEW_TIME_REDUCTION_TARGET_PERCENTAGE = 50.0


@dataclass(slots=True)
class AssurancePilotMetrics:
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
        if self.fields_detected <= 0:
            return 0.0
        value = self.fields_auto_accepted * 100.0 / self.fields_detected
        return round(min(100.0, max(0.0, value)), 2)

    @property
    def human_touch_percentage(self) -> float:
        if self.fields_detected <= 0:
            return 0.0
        touched = min(self.fields_detected, max(0, self.fields_manually_reviewed))
        return round(touched * 100.0 / self.fields_detected, 2)

    def meets_zero_friction_target(
        self,
        *,
        target_percentage: float = ZERO_FRICTION_TARGET_PERCENTAGE,
    ) -> bool:
        return self.automatic_data_percentage >= target_percentage

    def as_dict(self) -> dict[str, int | float]:
        payload: dict[str, int | float] = asdict(self)
        payload["automatic_data_percentage"] = self.automatic_data_percentage
        payload["human_touch_percentage"] = self.human_touch_percentage
        return payload


def review_time_reduction_percentage(
    *,
    baseline_seconds: float,
    assurance_seconds: float,
) -> float:
    """Return measured reduction against the client's real baseline."""
    if baseline_seconds <= 0:
        return 0.0
    reduction = (baseline_seconds - max(0.0, assurance_seconds)) * 100.0 / baseline_seconds
    return round(min(100.0, reduction), 2)
