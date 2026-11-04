"""Cost-aware model routing policy for the Lacey intelligent workflow.

The routing layer is intentionally pure. It decides whether additional model work is
worth paying for; it never changes declaration values. Deterministic validators and
human review remain the authority boundary.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import os

from .ai_shadow import ReconciliationStatus


class AITask(StrEnum):
    NONE = "NONE"
    EXTRACT = "EXTRACT"
    RECONCILE = "RECONCILE"
    ADJUDICATE = "ADJUDICATE"


@dataclass(frozen=True, slots=True)
class AITierConfig:
    extraction_model: str = "gpt-5.6-luna"
    reconciliation_model: str = "gpt-5.6-terra"
    adjudication_model: str = "gpt-5.6-sol"

    @classmethod
    def from_env(cls) -> "AITierConfig":
        provider = os.getenv("US_LACEY_AI_PROVIDER", "openai").strip().lower()
        if provider == "gemini":
            # Stable Gemini defaults: Flash-Lite for high-volume extraction and the
            # current GA 3.8 Flash model for bounded reconciliation/adjudication.
            # The review adapter uses lower thinking for reconciliation and higher
            # thinking for true blocking conflicts while keeping the same stable model.
            default_extract = "gemini-3.5-flash-lite"
            default_reconcile = "gemini-3.8-flash"
            default_adjudicate = "gemini-3.8-flash"
        else:
            default_extract = "gpt-5.6-luna"
            default_reconcile = "gpt-5.6-terra"
            default_adjudicate = "gpt-5.6-sol"
        return cls(
            extraction_model=os.getenv("US_LACEY_AI_EXTRACT_MODEL", default_extract).strip() or default_extract,
            reconciliation_model=os.getenv("US_LACEY_AI_RECONCILE_MODEL", default_reconcile).strip() or default_reconcile,
            adjudication_model=os.getenv("US_LACEY_AI_ADJUDICATE_MODEL", default_adjudicate).strip() or default_adjudicate,
        )

    def model_for(self, task: AITask) -> str | None:
        if task is AITask.EXTRACT:
            return self.extraction_model
        if task is AITask.RECONCILE:
            return self.reconciliation_model
        if task is AITask.ADJUDICATE:
            return self.adjudication_model
        return None


def next_ai_task(status: ReconciliationStatus) -> AITask:
    """Route only unresolved evidence states to more expensive models.

    AGREEMENT already has two independent extraction paths pointing to the same value,
    so paying for another model adds little. BOTH_MISSING and AI_REJECTED are data
    availability/evidence problems: a stronger model must not invent the missing fact.
    Single-engine values get a semantic cross-document reconciliation pass. Genuine
    contradictions and ambiguity are the only states escalated to the highest reasoning
    tier configured for the selected provider.
    """
    if status in {
        ReconciliationStatus.CONFLICT,
        ReconciliationStatus.AI_AMBIGUOUS,
        ReconciliationStatus.ENGINE2_CONFLICT,
    }:
        return AITask.ADJUDICATE
    if status in {
        ReconciliationStatus.AI_ONLY,
        ReconciliationStatus.ENGINE2_ONLY,
    }:
        return AITask.RECONCILE
    return AITask.NONE


def can_offer_bulk_confirmation(status: ReconciliationStatus) -> bool:
    """Only evidence agreement can be promoted to a one-click human suggestion."""
    return status is ReconciliationStatus.AGREEMENT
