"""Display-only grouping of already projected review fields.

Groups never authorize acceptance or combine decisions. Exact document IDs and
existing line references keep unrelated evidence apart; no inferred identities.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable

from litoral_trace.us_lacey.quick_review_eligibility import quick_accept_eligible


@dataclass(frozen=True, slots=True)
class ExceptionTask:
    key: str
    title: str
    fields: tuple[Any, ...]
    responsibility: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class ExceptionTaskGroup:
    category: str
    label: str
    tasks: tuple[ExceptionTask, ...]


_CATEGORIES = (
    ("verify", "Verify existing evidence"),
    ("documents", "Obtain missing or original documents"),
    ("complete", "Complete missing information"),
    ("filing", "Required before filing"),
)
_ENTITIES = {
    "importer_name": ("importer", "Importer details", "Importer or customs broker"),
    "importer_address": ("importer", "Importer details", "Importer or customs broker"),
    "consignee_name": ("consignee", "Consignee details", "Importer or consignee"),
    "consignee_address": ("consignee", "Consignee details", "Importer or consignee"),
    "filing_entry_reference": ("entry", "CBP Entry Number", "Importer or customs broker, after entry creation"),
    "genus": ("botanical", "Plant identity", "Supplier or manufacturer"),
    "species": ("botanical", "Plant identity", "Supplier or manufacturer"),
    "country_of_harvest": ("harvest", "Harvest declaration", "Supplier or manufacturer"),
    "plant_quantity": ("mass", "Plant material quantity and unit", "Supplier or manufacturer"),
    "metric_unit": ("mass", "Plant material quantity and unit", "Supplier or manufacturer"),
}


def exception_task_groups(fields, *, source_class: Callable, article_contexts=None) -> tuple[ExceptionTaskGroup, ...]:
    """Organize fields for presentation without mutating fields/readiness/evidence."""
    buckets = {}
    contexts = article_contexts or {}
    for field in fields:
        name = field.field_name
        candidates = tuple(getattr(field, "candidates", ()) or ())
        derived = source_class(getattr(field, "source_filename", None) or "").code == "DERIVED_PREPARATION"
        conflict = field.status == "CONFLICT"
        if name == "filing_entry_reference" and not field.proposed_value:
            category, reason = "filing", "May remain pending during document preparation; still required for filing-ready output."
        elif conflict:
            category, reason = "verify", "Conflicting information. Compare all sources; no direct confirmation is available."
        elif field.id in contexts:
            category, reason = "verify", "Compare the contextual original before an explicit, documented human decision."
        elif derived:
            category, reason = "documents", "Original document needed. A preparation worksheet cannot establish authority."
        elif len(candidates) == 1 and quick_accept_eligible(field, candidates[0]):
            category, reason = "verify", "Inspect the linked evidence before confirming each eligible value."
        elif not candidates and field.proposed_value and (getattr(field, "source_assurance_document_id", None) or getattr(field, "provenance", "") == "reused_evidence"):
            category, reason = "verify", "Inspect the linked evidence before confirming each value; the server revalidates the decision."
        elif candidates or getattr(field, "source_assurance_document_id", None):
            category, reason = "documents", "The linked information cannot be confirmed directly; verify or obtain sufficient original evidence."
        else:
            category, reason = "complete", "No sufficient linked information. Enter only a value you can verify or prepare an evidence request."
        entity, title, owner = _ENTITIES.get(name, (name, field.label, None))
        # Scope/line and source IDs are grouping boundaries, never identity inference.
        # Conflicts remain individual tasks to avoid hiding distinct reasons/sources.
        document = getattr(field, "source_assurance_document_id", None)
        key = (category, field.scope, field.line_reference, entity, document, field.id if conflict else None)
        if key not in buckets:
            line_title = title if field.scope == "SHIPMENT" else f"Plant line {field.line_reference} — {title}"
            buckets[key] = [line_title, owner, reason, []]
        buckets[key][3].append(field)
    result = []
    for category, label in _CATEGORIES:
        tasks = tuple(
            ExceptionTask(f"task-{data[3][0].id}", data[0], tuple(data[3]), data[1], data[2])
            for key, data in buckets.items() if key[0] == category
        )
        if tasks:
            result.append(ExceptionTaskGroup(category, label, tasks))
    return tuple(result)
