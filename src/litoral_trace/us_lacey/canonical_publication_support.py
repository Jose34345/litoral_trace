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



# The same canonical publication transaction may use an already byte-verified
# Assurance V2 observation to populate a REVIEW candidate. It must NEVER turn a
# candidate into a regulatory fact, reuse, MATCHED field or export authorization.
_DOCUMENTARY_REVIEW_FIELDS = frozenset({
    "entered_value", "article_component", "plant_quantity", "metric_unit",
    "merchandise_description",
})


def publish_source_linked_review_candidates(
    session, *, organization_id: int, operation_id: int,
) -> int:
    """Fill missing machine-only PPQ *review* fields from exact-PDF claims.

    Hard fences: current finalized/finishing source-set membership and hashes;
    a corroborated documentary binding plus SAME SKU on current line for plant
    fields; a literal shipment-scope claim for shipment fields; unique value per
    field; PPQ validation; no existing value or human decision. Publication uses
    the existing canonical _publish_field writer in this transaction, not a
    competing ingestion writer.
    """
    from litoral_trace.db.models import (
        AssuranceDocument, UsLaceyOperationProductLink,
        UsLaceySourceSetRevision, UsLaceySupplierProduct, VaultDocument,
    )
    from litoral_trace.us_lacey.assurance_v2_case_reader import (
        _source_claim_observations,
    )
    from litoral_trace.us_lacey.ppq505 import (
        PPQ505_SHIPMENT_REFERENCE, validate_ppq_value,
    )

    revision = session.scalar(select(UsLaceySourceSetRevision).where(
        UsLaceySourceSetRevision.organization_id == organization_id,
        UsLaceySourceSetRevision.operation_id == operation_id,
        UsLaceySourceSetRevision.is_current.is_(True),
        UsLaceySourceSetRevision.status.in_(("FINALIZING", "FINALIZED")),
    ))
    if revision is None:
        return 0
    # The current source set is immutable under the worker's operation lock.
    source_rows = session.execute(
        select(UsLaceyOperationDocument, VaultDocument)
        .join(AssuranceDocument, (
            AssuranceDocument.id == UsLaceyOperationDocument.assurance_document_id
        ) & (
            AssuranceDocument.organization_id == organization_id
        ))
        .join(VaultDocument, (
            VaultDocument.id == AssuranceDocument.vault_document_id
        ) & (
            VaultDocument.organization_id == organization_id
        ))
        .where(
            UsLaceyOperationDocument.organization_id == organization_id,
            UsLaceyOperationDocument.operation_id == operation_id,
            UsLaceyOperationDocument.is_current.is_(True),
        )
    ).all()
    if len(source_rows) != revision.document_count or not source_rows:
        return 0
    docs = [{
        "id": str(link.assurance_document_id),
        "operation_document_id": int(link.id),
        "version": str(link.version_number),
        "sha256": vault.sha256,
    } for link, vault in source_rows if vault.status == "available"]
    if len(docs) != revision.document_count:
        return 0
    try:
        claims = _source_claim_observations(
            session, organization_id=organization_id, operation_id=operation_id,
            revision=revision, documents=docs,
        )
    except ValueError:
        return 0
    if claims is None:
        return 0

    lines = session.scalars(select(UsLaceyPpqPlantLine).where(
        UsLaceyPpqPlantLine.organization_id == organization_id,
        UsLaceyPpqPlantLine.operation_id == operation_id,
    )).all()
    line_by_ref = {line.line_reference: line for line in lines}
    links = session.execute(
        select(UsLaceyOperationProductLink, UsLaceySupplierProduct)
        .join(UsLaceySupplierProduct, (
            UsLaceySupplierProduct.id == UsLaceyOperationProductLink.supplier_product_id
        ) & (
            UsLaceySupplierProduct.organization_id == organization_id
        ))
        .where(
            UsLaceyOperationProductLink.organization_id == organization_id,
            UsLaceyOperationProductLink.operation_id == operation_id,
            UsLaceyOperationProductLink.source_set_revision_id == revision.id,
        )
    ).all()
    sku_by_line: dict[str, str] = {}
    ambiguous: set[str] = set()
    for link, product in links:
        ref = str(link.line_reference)
        if ref in sku_by_line:
            ambiguous.add(ref)
        sku_by_line[ref] = str(product.sku)
    for ref in ambiguous:
        sku_by_line.pop(ref, None)

    targets = session.scalars(select(UsLaceyOperationField).where(
        UsLaceyOperationField.organization_id == organization_id,
        UsLaceyOperationField.operation_id == operation_id,
        UsLaceyOperationField.field_name.in_(_DOCUMENTARY_REVIEW_FIELDS),
    )).all()
    # Current Vault + operation-role classification is a negative authority
    # boundary: never re-source a pending original document as another original.
    # Filename detection can identify a derivative, but cannot grant approval.
    from litoral_trace.us_lacey.document_evidence_class import classify_document
    source_classes = {
        int(link.assurance_document_id): classify_document(
            vault.original_filename, link.document_role,
        ).code
        for link, vault in source_rows
    }
    source_to_assurance = {int(link.id): int(link.assurance_document_id)
                           for link, _ in source_rows}
    operation_by_assurance = {int(link.assurance_document_id): int(link.id)
                              for link, _ in source_rows}
    published = 0
    for target in targets:
        if (_field_has_human_review(target)
                or str(target.human_value or "").strip()
                or target.field_status == "NOT_REQUIRED"):
            continue
        current_value = str(target.normalized_value or target.original_value or "").strip()
        # The canonical reviewer already has a machine-only worksheet candidate.
        # A current corroborated original PDF may REPLACE ITS PRIMARY SOURCE only
        # when the quantity/unit is identical and still needs human review.
        # This is never a silent correction, bulk acceptance or filing approval.
        replacing_machine_worksheet = bool(
            current_value
            and target.field_scope == "PLANT_LINE"
            and target.field_name in {"plant_quantity", "metric_unit"}
            and target.field_status == "REVIEW"
            and target.extractor == "canonical-shipment-truth"
            and target.source_assurance_document_id is not None
            and source_classes.get(int(target.source_assurance_document_id))
                == "DERIVED_PREPARATION"
        )
        if current_value and not replacing_machine_worksheet:
            continue
        ref = str(target.merchandise_line_reference or "")
        if target.field_scope == "PLANT_LINE":
            if ref not in line_by_ref or target.plant_line_id != line_by_ref[ref].id:
                continue
            expected_sku = sku_by_line.get(ref)
            if not expected_sku:
                continue
        elif target.field_scope == "SHIPMENT" and ref == PPQ505_SHIPMENT_REFERENCE:
            expected_sku = None
        else:
            continue

        eligible = []
        for claim in claims:
            if (claim.get("field_name") != target.field_name
                    or claim.get("authority_status") != "CANDIDATE"
                    or claim.get("state") != "CANDIDATE"
                    or claim.get("support_status") != "SUPPORTED"
                    or not str(claim.get("normalized_value") or "").strip()
                    or not isinstance(claim.get("source_locator"), dict)
                    or claim["source_locator"].get("kind") != "TEXT_SPAN"):
                continue
            if expected_sku is None:
                if (claim.get("entity_type") != "SHIPMENT"
                        or claim.get("association_status") != "UNBOUND"):
                    continue
            elif not (
                claim.get("association_status") == "DOCUMENT_EXACT_SINGLE_SKU"
                and claim.get("line_reference") == ref
                and claim.get("sku") == expected_sku
            ):
                continue
            try:
                assurance_id = int(claim["assurance_document_id"])
            except (ValueError, TypeError):
                continue
            operation_document_id = operation_by_assurance.get(assurance_id)
            if operation_document_id is None:
                continue
            validation = validate_ppq_value(
                target.field_name, claim["normalized_value"],
            )
            if validation.status.value != "VALID" or not validation.normalized_value:
                continue
            eligible.append((claim, operation_document_id, validation.normalized_value))
        if not eligible or len({value for _claim, _doc, value in eligible}) != 1:
            # Genuine conflicting values or ambiguous scope MUST abstain.
            continue
        original_net_mass = [
            row for row in eligible
            if (row[0].get("source_claim") or {}).get("extractor_name")
            == "packing_net_mass_exact"
        ]
        if replacing_machine_worksheet:
            # The only overridable facts are two exact single-SKU plant facts
            # with ONE dedicated source witness from an ORIGINAL packing list.
            # No value change; merely prefer its verifiable provenance over the
            # already pending derivative worksheet.
            if (len(original_net_mass) != 1
                    or original_net_mass[0][0]["assurance_document_id"]
                    == target.source_assurance_document_id
                    or source_classes.get(
                        original_net_mass[0][0]["assurance_document_id"]
                    ) not in {"COMMERCIAL_SOURCE", "UNVERIFIED_COMMERCIAL"}):
                continue
            previous = validate_ppq_value(target.field_name, current_value)
            if (previous.status.value != "VALID"
                    or previous.normalized_value != original_net_mass[0][2]):
                continue
        chosen_value = eligible[0][2]
        evidence = tuple(CanonicalEvidence(
            candidate_id=str(claim["id"]),
            document_id=str(operation_document_id),
            field_key=target.field_name,
            normalized_value=value,
            source_authority=(
                0.95 if (claim.get("source_claim") or {}).get("extractor_name")
                == "packing_net_mass_exact" else 0.5
            ),
            candidate_score=0.5,
            source_page=claim["source_locator"].get("page"),
            source_text=str(claim.get("raw_value") or value),
            line_key=ref if expected_sku is not None else None,
            component_key=None, evidence_class="EXPLICIT",
        ) for claim, operation_document_id, value in eligible)
        source_truth = CanonicalFieldTruth(
            field_name=target.field_name,
            state=CanonicalTruthState.REVIEW_REQUIRED,
            values=(chosen_value,),
            evidence=evidence,
        )
        written, _requires_review, _rejected = _publish_field(
            session, organization_id=organization_id, operation_id=operation_id,
            target=target, truth=source_truth,
            assurance_by_operation_document=source_to_assurance,
            ambiguous_component_binding=False,
        )
        published += written
    return published


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
