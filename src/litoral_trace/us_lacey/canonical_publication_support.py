"""Support the single canonical U.S. Lacey publication boundary.

Per-document deterministic projection may temporarily materialize plant lines before
Engine 2 has reconciled the complete shipment.  Canonical publication is allowed to
compact those rows only when they are provably machine-created and untouched by a
human.  This module also derives PPQ Article / Component from canonical line-local
product wording by exact taxon subtraction; it never invents a product description.
"""
from __future__ import annotations

import re
from typing import Iterable

from sqlalchemy import select

from litoral_trace.db.models import (
    UsLaceyEngineShipmentRun,
    UsLaceyOperation,
    UsLaceyOperationDocument,
    UsLaceyOperationField,
    UsLaceyPpqPlantLine,
)
from litoral_trace.us_lacey.canonical_shipment_truth import (
    CanonicalEvidence,
    CanonicalFieldTruth,
    CanonicalShipmentTruth,
    CanonicalTruthState,
    _publish_field,
    build_canonical_shipment_truth,
)


_MACHINE_PROVISIONAL_EXTRACTORS = frozenset(
    {
        "assurance-deterministic-parser",
        "us-lacey-deterministic-projector",
    }
)
_PROVISIONAL_REFERENCE = re.compile(r"(?:\d+|auto-\d+(?:-\d+)?)\Z", re.IGNORECASE)
_TAXON_KEY = re.compile(r"taxon:([^:]+):([^:]+)\Z", re.IGNORECASE)
_EDGE_SEPARATORS = " \t\r\n-–—/:;,.|()[]{}"


def derive_article_component(description: str, taxon_key: str | None) -> str | None:
    """Return source-grounded product wording after removing one exact binomial.

    The derivation is deliberately conservative: both genus and species must occur as
    one contiguous binomial exactly once.  If removing it leaves no product wording,
    the caller must keep Article / Component missing for human review.
    """

    text = " ".join(str(description or "").split())
    match = _TAXON_KEY.fullmatch(str(taxon_key or "").strip())
    if not text or match is None:
        return None
    genus, species = match.groups()
    taxon = re.compile(
        rf"(?<![A-Za-z0-9]){re.escape(genus)}\s+{re.escape(species)}(?![A-Za-z0-9])",
        re.IGNORECASE,
    )
    occurrences = list(taxon.finditer(text))
    if len(occurrences) != 1:
        return None
    residual = taxon.sub(" ", text, count=1)
    residual = " ".join(residual.split()).strip(_EDGE_SEPARATORS)
    return residual or None


def _field_has_human_review(field) -> bool:
    return bool(
        getattr(field, "reviewed_at", None) is not None
        or getattr(field, "reviewed_by_user_id", None) is not None
        or str(getattr(field, "human_value", None) or "").strip()
    )


def is_safe_provisional_machine_line(*, line_reference: str, fields: Iterable[object]) -> bool:
    """Prove that a provisional line is disposable machine state.

    A numeric/``auto-*`` reference alone is never sufficient.  At least one field
    must carry known deterministic machine provenance, every populated/provenanced
    field must use that provenance, and no field may contain human review state.
    """

    if _PROVISIONAL_REFERENCE.fullmatch(str(line_reference or "").strip()) is None:
        return False

    saw_machine_provenance = False
    for field in fields:
        if _field_has_human_review(field):
            return False
        original = str(getattr(field, "original_value", None) or "").strip()
        normalized = str(getattr(field, "normalized_value", None) or "").strip()
        extractor = str(getattr(field, "extractor", None) or "").strip()
        source_document_id = getattr(field, "source_assurance_document_id", None)
        has_machine_state = bool(original or normalized or extractor or source_document_id)
        if not has_machine_state:
            continue
        if extractor not in _MACHINE_PROVISIONAL_EXTRACTORS or source_document_id is None:
            return False
        saw_machine_provenance = True
    return saw_machine_provenance


def latest_canonical_truth(
    session,
    *,
    organization_id: int,
    operation_id: int,
) -> CanonicalShipmentTruth:
    run = session.scalar(
        select(UsLaceyEngineShipmentRun)
        .where(
            UsLaceyEngineShipmentRun.organization_id == int(organization_id),
            UsLaceyEngineShipmentRun.operation_id == int(operation_id),
        )
        .order_by(UsLaceyEngineShipmentRun.id.desc())
    )
    if run is None:
        raise RuntimeError("CANONICAL_SHIPMENT_RUN_NOT_FOUND")
    return build_canonical_shipment_truth(run.resolution_json)


def compact_provisional_machine_lines(
    session,
    *,
    organization_id: int,
    operation_id: int,
    needed_line_count: int,
) -> int:
    """Delete only surplus lines whose machine-only origin is provable."""

    lines = list(
        session.scalars(
            select(UsLaceyPpqPlantLine)
            .where(
                UsLaceyPpqPlantLine.organization_id == int(organization_id),
                UsLaceyPpqPlantLine.operation_id == int(operation_id),
            )
            .order_by(UsLaceyPpqPlantLine.ordinal.asc(), UsLaceyPpqPlantLine.id.asc())
        ).all()
    )
    removed = 0
    for line in lines[max(0, int(needed_line_count)) :]:
        # Native canonical rows are already handled by the canonical publisher, which
        # also protects reviewed lines.  This repair is only for the older provisional
        # numeric/auto rows produced by per-document deterministic projection.
        if str(line.line_reference).startswith("CANONICAL-"):
            continue
        fields = session.scalars(
            select(UsLaceyOperationField).where(
                UsLaceyOperationField.organization_id == int(organization_id),
                UsLaceyOperationField.operation_id == int(operation_id),
                UsLaceyOperationField.plant_line_id == int(line.id),
            )
        ).all()
        if not is_safe_provisional_machine_line(
            line_reference=str(line.line_reference),
            fields=fields,
        ):
            continue
        session.delete(line)
        removed += 1
    if removed:
        session.flush()
    return removed


def prepare_canonical_publication(
    session,
    *,
    organization_id: int,
    operation_id: int,
) -> CanonicalShipmentTruth:
    """Build canonical truth and remove only proven provisional surplus rows."""

    truth = latest_canonical_truth(
        session,
        organization_id=organization_id,
        operation_id=operation_id,
    )
    compact_provisional_machine_lines(
        session,
        organization_id=organization_id,
        operation_id=operation_id,
        needed_line_count=len(truth.plant_lines),
    )
    return truth


def _derived_component_truth(
    canonical_line,
) -> CanonicalFieldTruth | None:
    if canonical_line.fields.get("article_component") is not None:
        return None
    description = canonical_line.fields.get("merchandise_description")
    if (
        description is None
        or len(description.values) != 1
        or description.state is CanonicalTruthState.CONFLICT
        or canonical_line.taxon_key is None
        or not description.evidence
    ):
        return None
    derived = derive_article_component(description.values[0], canonical_line.taxon_key)
    if derived is None:
        return None
    primary = max(
        description.evidence,
        key=lambda row: (row.source_authority, row.candidate_score, row.candidate_id),
    )
    evidence = CanonicalEvidence(
        candidate_id=f"{primary.candidate_id}:article-component",
        document_id=primary.document_id,
        field_key="article_component",
        normalized_value=derived,
        source_authority=primary.source_authority,
        candidate_score=primary.candidate_score,
        source_page=primary.source_page,
        source_text=primary.source_text,
        line_key=primary.line_key,
        component_key=canonical_line.taxon_key,
        evidence_class="DERIVED",
    )
    if description.state in {
        CanonicalTruthState.REVIEW_REQUIRED,
        CanonicalTruthState.NEAR_MATCH,
    }:
        state = CanonicalTruthState.REVIEW_REQUIRED
    elif description.state is CanonicalTruthState.SUPPORTED_MULTIPLE:
        state = CanonicalTruthState.SUPPORTED_MULTIPLE
    else:
        state = CanonicalTruthState.SUPPORTED
    return CanonicalFieldTruth(
        field_name="article_component",
        state=state,
        values=(derived,),
        evidence=(evidence,),
    )


def publish_derived_article_components(
    session,
    *,
    organization_id: int,
    operation_id: int,
    truth: CanonicalShipmentTruth,
) -> int:
    """Publish source-grounded Article / Component inside the canonical transaction."""

    org_id = int(organization_id)
    op_id = int(operation_id)
    lines = list(
        session.scalars(
            select(UsLaceyPpqPlantLine)
            .where(
                UsLaceyPpqPlantLine.organization_id == org_id,
                UsLaceyPpqPlantLine.operation_id == op_id,
            )
            .order_by(UsLaceyPpqPlantLine.ordinal.asc(), UsLaceyPpqPlantLine.id.asc())
        ).all()
    )
    if len(lines) != len(truth.plant_lines):
        raise RuntimeError("CANONICAL_LINE_COUNT_MISMATCH")

    operation_documents = session.scalars(
        select(UsLaceyOperationDocument).where(
            UsLaceyOperationDocument.organization_id == org_id,
            UsLaceyOperationDocument.operation_id == op_id,
            UsLaceyOperationDocument.is_current.is_(True),
        )
    ).all()
    assurance_by_operation_document = {
        int(row.id): int(row.assurance_document_id) for row in operation_documents
    }

    targets = session.scalars(
        select(UsLaceyOperationField).where(
            UsLaceyOperationField.organization_id == org_id,
            UsLaceyOperationField.operation_id == op_id,
            UsLaceyOperationField.field_name == "article_component",
        )
    ).all()
    by_line_id = {
        int(row.plant_line_id): row for row in targets if row.plant_line_id is not None
    }

    published_count = 0
    for line_row, canonical_line in zip(lines, truth.plant_lines):
        derived_truth = _derived_component_truth(canonical_line)
        if derived_truth is None:
            continue
        target = by_line_id.get(int(line_row.id))
        if target is None:
            raise RuntimeError("CANONICAL_PPQ_FIELD_SLOT_MISSING")
        published, _reviews, _rejected = _publish_field(
            session,
            organization_id=org_id,
            operation_id=op_id,
            target=target,
            truth=derived_truth,
            assurance_by_operation_document=assurance_by_operation_document,
            ambiguous_component_binding=False,
        )
        published_count += published

    if published_count:
        operation = session.scalar(
            select(UsLaceyOperation).where(
                UsLaceyOperation.organization_id == org_id,
                UsLaceyOperation.id == op_id,
            )
        )
        if operation is None:
            raise RuntimeError("CANONICAL_OPERATION_NOT_FOUND")
        session.flush()
        from litoral_trace.us_lacey.projection import refresh_us_lacey_operation_status

        refresh_us_lacey_operation_status(
            session,
            organization_id=org_id,
            operation=operation,
        )
    return published_count
