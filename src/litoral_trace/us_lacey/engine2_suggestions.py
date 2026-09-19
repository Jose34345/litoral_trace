"""Compatibility seam for Engine 2 analysis and canonical shipment publication.

The former module independently interpreted shipment evidence and wrote PPQ review
fields. That second writer is retired. ``supported_engine2_suggestions`` remains as a
pure analysis helper for regression/diagnostic callers, while runtime publication has
exactly one authority: CanonicalShipmentTruth.
"""
from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Mapping

from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.canonical_publication_support import (
    prepare_canonical_publication,
    publish_derived_article_components,
)
from litoral_trace.us_lacey.canonical_shipment_truth import publish_canonical_shipment_truth
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.lacey_engine_service import ENGINE2_SHADOW, engine2_mode


LOGGER = logging.getLogger(__name__)

_ENGINE2_TO_PREPARATION_FIELD = {
    "estimated_arrival_date": "estimated_arrival_date",
    "filing_entry_reference": "filing_entry_reference",
    "container_number": "container_number",
    "bill_of_lading": "bill_of_lading",
    "manufacturer_id": "manufacturer_id",
    "importer_name": "importer_name",
    "consignee_name": "consignee_name",
    "importer_address": "importer_address",
    "consignee_address": "consignee_address",
    "description": "merchandise_description",
    "hts_code": "hts_code",
    "entered_value": "entered_value",
    "article_component": "article_component",
    "genus": "genus",
    "species": "species",
    "country_of_harvest": "country_of_harvest",
    "plant_quantity": "plant_quantity",
    "metric_unit": "metric_unit",
    "percent_recycled": "percent_recycled",
}
_SUPPORTED_STATES = frozenset({"SUPPORTED", "SUPPORTED_MULTIPLE", "NEAR_MATCH"})


@dataclass(frozen=True, slots=True)
class Engine2Suggestion:
    """Pure, non-authoritative view of one supported Engine 2 evidence value."""

    field_name: str
    value: str
    operation_document_id: int
    source_text: str
    source_page: int
    evidence_class: str
    confidence: float
    engine_version: str
    association_key: str | None = None
    requires_review: bool = False


def _candidate_payload(evidence: Mapping[str, object]) -> Mapping[str, object] | None:
    candidate = evidence.get("candidate")
    return candidate if isinstance(candidate, Mapping) else None


def _association_key(evidence: Mapping[str, object]) -> str | None:
    for key in ("component_key", "line_key"):
        value = str(evidence.get(key) or "").strip()
        if value:
            return value
    return None


def _suggestions_from_field(
    field_key: str,
    payload: Mapping[str, object],
    *,
    engine_version: str,
) -> tuple[Engine2Suggestion, ...]:
    target = _ENGINE2_TO_PREPARATION_FIELD.get(field_key)
    state = str(payload.get("state") or "")
    review_only = state == "REVIEW_REQUIRED" and field_key == "country_of_harvest"
    if target is None or (state not in _SUPPORTED_STATES and not review_only):
        return ()

    evidence_rows = payload.get("supporting_evidence")
    if not isinstance(evidence_rows, list):
        return ()

    eligible: dict[
        tuple[str | None, str],
        list[tuple[float, float, int, str, str, int, str]],
    ] = {}
    for evidence in evidence_rows:
        if not isinstance(evidence, Mapping):
            continue
        candidate = _candidate_payload(evidence)
        if candidate is None:
            continue
        raw = candidate.get("raw")
        provenance = candidate.get("provenance")
        if not isinstance(raw, Mapping) or not isinstance(provenance, Mapping):
            continue
        evidence_class = str(
            raw.get("evidence_class") or provenance.get("evidence_class") or ""
        )
        if evidence_class not in {"EXPLICIT", "DERIVED"}:
            continue
        if review_only and evidence_class != "EXPLICIT":
            continue
        value = str(
            evidence.get("normalized_value") or raw.get("normalized_value") or ""
        ).strip()
        source_text = str(provenance.get("source_text") or "").strip()
        association = _association_key(evidence)
        try:
            page = int(provenance.get("page") or 0)
            operation_document_id = int(evidence.get("document_id") or 0)
            source_authority = float(evidence.get("source_authority") or 0.0)
            candidate_score = float(
                evidence.get("candidate_score") or candidate.get("score") or 0.0
            )
        except (TypeError, ValueError):
            continue
        if not value or not source_text or page < 1 or operation_document_id <= 0:
            continue
        if review_only and association is None:
            continue
        eligible.setdefault((association, value), []).append(
            (
                source_authority,
                candidate_score,
                operation_document_id,
                value,
                source_text,
                page,
                evidence_class,
            )
        )

    if not eligible:
        return ()

    # A scalar/unassociated field must have one canonical value. Multiple unrelated
    # values stay fail-closed; semantically associated line/component values remain
    # available to diagnostics as separate suggestions.
    unassociated_values = {
        value for (association, value) in eligible if association is None
    }
    if len(unassociated_values) > 1:
        eligible = {key: rows for key, rows in eligible.items() if key[0] is not None}

    suggestions: list[Engine2Suggestion] = []
    for (association, _normalized_value), rows in eligible.items():
        if not rows:
            continue
        (
            source_authority,
            candidate_score,
            document_id,
            value,
            source_text,
            page,
            evidence_class,
        ) = max(rows, key=lambda item: (item[0], item[1]))
        del source_authority, candidate_score
        confidence = 0.94 if len(rows) > 1 else 0.86
        if evidence_class == "DERIVED":
            confidence = min(confidence, 0.90)
        if review_only:
            confidence = min(confidence, 0.75)
        suggestions.append(
            Engine2Suggestion(
                target,
                value,
                document_id,
                source_text,
                page,
                evidence_class,
                confidence,
                engine_version,
                association,
                review_only,
            )
        )
    return tuple(suggestions)


def supported_engine2_suggestions(
    payload: Mapping[str, object],
) -> tuple[Engine2Suggestion, ...]:
    """Return a pure diagnostic view without writing PPQ/candidate state."""

    fields = payload.get("canonical_fields")
    if not isinstance(fields, Mapping):
        return ()
    engine_version = str(payload.get("engine_version") or "lacey-engine-2")
    suggestions: list[Engine2Suggestion] = []
    for field_key, field_payload in fields.items():
        if isinstance(field_payload, Mapping):
            suggestions.extend(
                _suggestions_from_field(
                    str(field_key), field_payload, engine_version=engine_version
                )
            )
    return tuple(suggestions)


def project_engine2_supported_suggestions(*, organization_id: int, operation_id: int) -> int:
    """Publish CanonicalShipmentTruth only when Engine 2 canonical input is active.

    ``US_LACEY_ENGINE2_MODE=SHADOW`` is the existing contract that causes the worker
    to build/persist the shipment run. With Engine 2 OFF there is no canonical input
    to publish, so this compatibility seam is deliberately a no-op. When Engine 2 is
    active, publication failures propagate and keep the owned worker job fail-closed.

    Per-document plant lines are provisional. Before the canonical writer runs, only
    surplus rows with provable deterministic-machine provenance and no human review
    may be compacted. Article / Component is then derived from that same canonical
    line truth inside this transaction, so no second independent writer is restored.
    """

    if engine2_mode() != ENGINE2_SHADOW:
        return 0

    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, organization_id)
        truth = prepare_canonical_publication(
            session,
            organization_id=organization_id,
            operation_id=operation_id,
        )
        result = publish_canonical_shipment_truth(
            session,
            organization_id=organization_id,
            operation_id=operation_id,
        )
        derived_count = publish_derived_article_components(
            session,
            organization_id=organization_id,
            operation_id=operation_id,
            truth=truth,
        )
        session.commit()
        return int(result.field_count) + int(derived_count)
    except Exception:
        session.rollback()
        LOGGER.exception(
            "U.S. Lacey canonical publication failed",
            extra={
                "event": "us_lacey_canonical_publication_failed",
                "organization_id": int(organization_id),
                "operation_id": int(operation_id),
            },
        )
        raise
    finally:
        session.close()
