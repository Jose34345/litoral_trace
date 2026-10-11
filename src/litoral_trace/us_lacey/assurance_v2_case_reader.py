"""Tenant-scoped, fail-closed Assurance V2 shipment-case read projection.

Read-only shadow integration: never translates legacy 'MATCHED' to human
authority, never issues export authorization, never copies historical evidence
into a new shipment's original-document observations.
"""
from __future__ import annotations

from dataclasses import replace
from uuid import UUID

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    AssuranceDocument, AssuranceV2Decision, AssuranceV2DecisionSource,
    AssuranceV2MemoryLink, UsLaceyEvidenceClaim, UsLaceySupplierEvidence,
    UsLaceyAssuranceV2ClaimSnapshot,
    UsLaceyEngineShipmentRun, UsLaceyOperation, UsLaceyOperationDocument, UsLaceyOperationField,
    UsLaceyPpqPlantLine, UsLaceyRegulatoryAssessmentSnapshot,
    UsLaceySourceSetMember, UsLaceySourceSetRevision,
    UsLaceySupplier, UsLaceySupplierProduct, UsLaceyOperationProductLink,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.assurance_v2_memory import (
    _current_claim_snapshot, canonical_claim_snapshot_digest,
    evaluate_reuse, verified_reuse_application,
)
from litoral_trace.us_lacey.assurance_v2_claim_bridge import (
    ResolvedDocumentBinding, to_authority_candidate_snapshot,
)
from litoral_trace.us_lacey.assurance_v2_reuse_guard import current_claim_values_for_reuse
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.exporters.assurance_v2 import AssuranceCase
from litoral_trace.us_lacey.ppq505 import PPQ505_SHIPMENT_REFERENCE

_REUSABLE_FIELDS = frozenset({"genus", "species", "country_of_harvest"})
_LINE_BOUND_ASSOCIATION_STATUSES = frozenset({
    "EXPLICIT_SKU", "SOURCE_LOCAL_LINE", "SOURCE_ROW",
    "DOCUMENT_EXACT_SINGLE_SKU",
})


def _is_line_bound(claim: dict) -> bool:
    """Return whether the immutable source-claim contract binds a line."""
    return claim.get("association_status") in _LINE_BOUND_ASSOCIATION_STATUSES


def _current_review_source_choices(
    observations: tuple[dict, ...] | None, *, field_name: str,
    line_reference: str, identities: tuple[dict, ...],
) -> list[dict]:
    """Show current PDF candidates but NEVER choose or authorize them here."""
    valid_identities = tuple(
        item for item in identities
        if item["line_reference"] == line_reference
        and item["status"] in {"VERIFIED", "HUMAN_CONFIRMED"}
    )
    results = []
    for claim in observations or ():
        if (claim.get("field_name") != field_name
                or claim.get("support_status") != "SUPPORTED"
                or claim.get("authority_status") != "CANDIDATE"
                or not claim.get("normalized_value")):
            continue
        if line_reference == PPQ505_SHIPMENT_REFERENCE:
            if claim.get("entity_type") != "SHIPMENT" or claim.get("association_status") != "UNBOUND":
                continue
        elif (len(valid_identities) != 1
              or claim.get("association_status") != "DOCUMENT_EXACT_SINGLE_SKU"
              or claim.get("line_reference") != line_reference
              or claim.get("sku") != valid_identities[0]["sku"]):
            continue
        results.append({
            "id": "source:" + str(claim["id"]),
            "value": claim["normalized_value"],
            "document_id": str(claim["assurance_document_id"]),
            "page": (claim.get("source_locator") or {}).get("page"),
            "locator": claim.get("source_locator"),
        })
    return results


def read_tenant_assurance_case(
    *, organization_id: int, operation_public_id: str,
    session_factory=get_us_lacey_db_session,
) -> AssuranceCase | None:
    """Resolve the public ID only under the authenticated organization's RLS."""
    try:
        public_id = UUID(str(operation_public_id))
    except (ValueError, TypeError, AttributeError):
        return None
    db = session_factory()
    try:
        set_tenant_db_context(db, int(organization_id))
        return _case(db, int(organization_id), public_id)
    finally:
        db.close()



def _source_claim_observations(
    db: Session, *, organization_id: int, operation_id: int,
    revision: UsLaceySourceSetRevision, documents: list[dict],
) -> tuple[dict, ...] | None:
    """Read a source-set-fenced observation stream, never final authority.

    A forged, obsolete or cross-tenant claim/document reference is discarded.
    Legacy canonical fields do not become V2 decisions because an extraction
    candidate has a plausible value or a verified literal source anchor.
    """
    current_documents = {
        (int(doc["operation_document_id"]), int(doc["id"])): doc
        for doc in documents
    }
    sidecars = db.scalars(select(UsLaceyAssuranceV2ClaimSnapshot).where(
        UsLaceyAssuranceV2ClaimSnapshot.organization_id == organization_id,
        UsLaceyAssuranceV2ClaimSnapshot.operation_id == operation_id,
        UsLaceyAssuranceV2ClaimSnapshot.source_set_revision_id == revision.id,
    )).all()
    if sidecars:
        # An explicitly persisted V2 result wins over a legacy Engine run.
        # Ambiguous or failed sidecars must abstain rather than resurrecting
        # stale claims from that older run.
        if len(sidecars) != 1:
            return None
        sidecar = sidecars[0]
        snapshot = sidecar.snapshot_json
        if (
            sidecar.source_set_fingerprint != revision.source_set_fingerprint
            or sidecar.status != "CANDIDATE_ONLY"
            or not isinstance(snapshot, dict)
            or snapshot.get("status") != sidecar.status
            or snapshot.get("document_count", len(snapshot.get("documents") or ())) != sidecar.document_count
            or snapshot.get("claim_count") != sidecar.claim_count
        ):
            return None
        snapshots = (snapshot,)
    else:
        runs = db.scalars(select(UsLaceyEngineShipmentRun).where(
            UsLaceyEngineShipmentRun.organization_id == organization_id,
            UsLaceyEngineShipmentRun.operation_id == operation_id,
        ).order_by(UsLaceyEngineShipmentRun.id.desc()).limit(8)).all()
        snapshots = tuple(
            run.resolution_json.get("assurance_v2_claim_snapshot")
            if isinstance(run.resolution_json, dict) else None
            for run in runs
        )
    for snapshot in snapshots:
        if not isinstance(snapshot, dict) or snapshot.get("schema_version") != "assurance.claim.snapshot.v1":
            continue
        if (
            int(snapshot.get("source_set_revision_id") or -1) != revision.id
            or snapshot.get("source_set_fingerprint") != revision.source_set_fingerprint
            or snapshot.get("status") != "CANDIDATE_ONLY"
            or len(snapshot.get("documents") or ()) != len(current_documents)
        ):
            continue
        observations: list[dict] = []
        seen = set()
        for source in snapshot.get("documents", ()):
            if not isinstance(source, dict):
                return None
            key = (int(source.get("operation_document_id") or -1),
                   int(source.get("assurance_document_id") or -1))
            document = current_documents.get(key)
            if key in seen:
                return None
            seen.add(key)
            if (
                document is None
                or source.get("original_sha256") != document["sha256"]
                or int(source.get("document_version_number") or -1) != int(document["version"])
                or int(source.get("organization_id") or -1) != organization_id
                or int(source.get("operation_id") or -1) != operation_id
                or int(source.get("source_set_revision_id") or -1) != revision.id
                or source.get("source_set_fingerprint") != revision.source_set_fingerprint
                or source.get("schema_version") != "assurance.claim.snapshot.v1"
                or source.get("source_claim_contract_version") != "assurance.claim.v1"
            ):
                return None
            if source.get("status") == "FAILED":
                return None
            if source.get("claim_count") != len(source.get("claims") or ()):
                return None
            binding = ResolvedDocumentBinding(
                organization_id=organization_id,
                operation_id=operation_id,
                source_set_revision_id=revision.id,
                source_set_fingerprint=revision.source_set_fingerprint,
                generation=revision.generation,
                assurance_document_id=key[1],
                operation_document_id=key[0],
                document_version_number=int(document["version"]),
                original_sha256=document["sha256"],
            )
            for candidate in source.get("claims", ()):
                if not isinstance(candidate, dict):
                    return None
                adapted = to_authority_candidate_snapshot(
                    candidate, binding=binding,
                    current_document_hashes={
                        int(doc["id"]): doc["sha256"]
                        for doc in current_documents.values()
                    },
                )
                observation = adapted.as_authority_input()
                observation.update({
                    "id": str(candidate["claim_id"]),
                    "document_id": document["id"],
                    "original_value": adapted.raw_value,
                    "authority": "CANDIDATE",
                })
                observations.append(observation)
        if seen != set(current_documents) or snapshot.get("claim_count") != len(observations):
            return None
        return tuple(observations)
    return None


def _current_regulatory_rules(
    db: Session, *, organization_id: int, operation_id: int,
    revision: UsLaceySourceSetRevision,
) -> tuple[str, tuple[dict, ...]]:
    """Only existing CURRENT rule assessments from this exact source revision.

    An assessment from another shipment, revision or ruleset never grants filing
    readiness. This is read-only; missing assessments must remain UNVERIFIED.
    """
    from litoral_trace.us_lacey.regulatory.rules import RULESET_VERSION
    from litoral_trace.us_lacey.regulatory_assessment_snapshot import RegulatoryAssessmentView
    from litoral_trace.us_lacey.regulatory.assurance_v2_adapter import rules_from_current_assessment

    row = db.scalar(select(UsLaceyRegulatoryAssessmentSnapshot).where(
        UsLaceyRegulatoryAssessmentSnapshot.organization_id == organization_id,
        UsLaceyRegulatoryAssessmentSnapshot.operation_id == operation_id,
        UsLaceyRegulatoryAssessmentSnapshot.source_set_revision_id == revision.id,
        UsLaceyRegulatoryAssessmentSnapshot.source_set_fingerprint == revision.source_set_fingerprint,
        UsLaceyRegulatoryAssessmentSnapshot.ruleset_version == RULESET_VERSION,
        UsLaceyRegulatoryAssessmentSnapshot.status == "CURRENT",
    ).order_by(UsLaceyRegulatoryAssessmentSnapshot.id.desc()))
    if row is None:
        return "US_LACEY_ASSESSMENT_UNVERIFIED", ()
    view = RegulatoryAssessmentView(
        status=row.status, generation=row.generation,
        source_set_fingerprint=row.source_set_fingerprint,
        ruleset_version=row.ruleset_version,
        input_fingerprint=row.input_fingerprint,
        assessment_count=row.assessment_count,
        indeterminate_count=row.indeterminate_count,
        payload=dict(row.payload_json or {}),
    )
    rules = rules_from_current_assessment(
        view, current_source_set_fingerprint=revision.source_set_fingerprint,
        current_ruleset_version=RULESET_VERSION,
    )
    if not rules or len(rules) != row.assessment_count:
        return "US_LACEY_ASSESSMENT_UNVERIFIED", ()
    return RULESET_VERSION, rules


def _case(db: Session, org_id: int, public_id: UUID) -> AssuranceCase | None:
    op = db.scalar(select(UsLaceyOperation).where(
        UsLaceyOperation.organization_id == org_id,
        UsLaceyOperation.public_id == public_id,
    ))
    if op is None:
        return None
    revision = db.scalar(select(UsLaceySourceSetRevision).where(
        UsLaceySourceSetRevision.organization_id == org_id,
        UsLaceySourceSetRevision.operation_id == op.id,
        UsLaceySourceSetRevision.is_current.is_(True),
    ).order_by(UsLaceySourceSetRevision.generation.desc()))
    if revision is None:
        return None

    documents: list[dict] = []
    valid_document_ids: set[int] = set()
    current_source_versions: set[tuple[int, int, int]] = set()
    for link, assurance, vault in db.execute(
        select(UsLaceyOperationDocument, AssuranceDocument, VaultDocument)
        .join(UsLaceySourceSetMember, and_(
            UsLaceySourceSetMember.organization_id == UsLaceyOperationDocument.organization_id,
            UsLaceySourceSetMember.operation_document_id == UsLaceyOperationDocument.id,
            UsLaceySourceSetMember.source_set_revision_id == revision.id,
        ))
        .join(AssuranceDocument, and_(
            AssuranceDocument.organization_id == UsLaceyOperationDocument.organization_id,
            AssuranceDocument.id == UsLaceyOperationDocument.assurance_document_id,
        ))
        .join(VaultDocument, and_(
            VaultDocument.organization_id == AssuranceDocument.organization_id,
            VaultDocument.id == AssuranceDocument.vault_document_id,
        ))
        .where(UsLaceyOperationDocument.organization_id == org_id)
        .order_by(UsLaceyOperationDocument.id)
    ):
        valid_document_ids.add(assurance.id)
        current_source_versions.add((int(assurance.id), int(link.id), int(link.version_number)))
        documents.append({
            "id": str(assurance.id), "version": str(link.version_number),
            "operation_document_id": int(link.id),
            "sha256": vault.sha256, "role": link.document_role,
            "name": getattr(vault, "original_filename", None) or getattr(vault, "filename", None) or str(assurance.public_id),
            "document_public_id": str(assurance.public_id),
            "raw_available": vault.status == "available",
        })

    try:
        source_observations = _source_claim_observations(
            db, organization_id=org_id, operation_id=op.id,
            revision=revision, documents=documents,
        )
        source_claim_error = "SOURCE_CLAIMS_UNAVAILABLE" if source_observations is None else None
    except ValueError:
        source_observations = None
        source_claim_error = "SOURCE_CLAIMS_INVALID"
    claim_snapshot = (
        _current_claim_snapshot(db, organization_id=org_id, operation_id=op.id,
                                revision=revision)
        if source_observations is not None else None
    )
    if source_observations is not None and claim_snapshot is None:
        source_observations = None
        source_claim_error = "SOURCE_CLAIMS_INVALID"
    snapshot_digest = (
        canonical_claim_snapshot_digest(claim_snapshot)
        if claim_snapshot is not None else None
    )
    raw_claims = tuple(
        claim for source in claim_snapshot["documents"] for claim in source["claims"]
    ) if claim_snapshot is not None else ()

    decisions = db.scalars(select(AssuranceV2Decision).where(
        AssuranceV2Decision.organization_id == org_id,
        AssuranceV2Decision.operation_id == op.id,
        AssuranceV2Decision.source_set_revision_id == revision.id,
    ).order_by(AssuranceV2Decision.id)).all()
    decision_sources = db.scalars(select(AssuranceV2DecisionSource).where(
        AssuranceV2DecisionSource.organization_id == org_id,
        AssuranceV2DecisionSource.decision_id.in_([d.id for d in decisions] or [-1]),
    )).all()
    docs_by_decision: dict[int, list[AssuranceV2DecisionSource]] = {}
    for row in decision_sources:
        docs_by_decision.setdefault(row.decision_id, []).append(row)
    superseded = {d.supersedes_decision_id for d in decisions if d.supersedes_decision_id is not None}
    authoritative = {
        (d.line_reference or PPQ505_SHIPMENT_REFERENCE, d.field_name): d
        for d in decisions if d.id not in superseded and d.action in {"ACCEPT", "CORRECT"}
        and docs_by_decision.get(d.id)
        and all((int(source.assurance_document_id),
                 int(source.operation_document_id),
                 int(source.document_version_number)) in current_source_versions
                for source in docs_by_decision[d.id])
    }

    lines = tuple(db.scalars(select(UsLaceyPpqPlantLine.line_reference).where(
        UsLaceyPpqPlantLine.organization_id == org_id,
        UsLaceyPpqPlantLine.operation_id == op.id,
    ).order_by(UsLaceyPpqPlantLine.ordinal)).all())
    line_set = set(lines)
    bindings = db.execute(
        select(UsLaceyOperationProductLink, UsLaceySupplierProduct, UsLaceySupplier)
        .join(UsLaceySupplierProduct, and_(
            UsLaceySupplierProduct.id == UsLaceyOperationProductLink.supplier_product_id,
            UsLaceySupplierProduct.organization_id == UsLaceyOperationProductLink.organization_id,
        ))
        .join(UsLaceySupplier, and_(
            UsLaceySupplier.id == UsLaceySupplierProduct.supplier_id,
            UsLaceySupplier.organization_id == UsLaceySupplierProduct.organization_id,
        ))
        .where(
            UsLaceyOperationProductLink.organization_id == org_id,
            UsLaceyOperationProductLink.operation_id == op.id,
            UsLaceyOperationProductLink.source_set_revision_id == revision.id,
        )
    ).all()
    identities = tuple({
        "line_reference": link.line_reference,
        "supplier_id": supplier.id,
        "product_id": product.id,
        "sku": product.sku,
        "status": (
            "HUMAN_CONFIRMED" if link.link_method == "HUMAN_CONFIRMED"
            and supplier.status in {"ACTIVE", "VERIFIED"}
            and product.status in {"ACTIVE", "VERIFIED"}
            else "REVIEW_REQUIRED"
        ),
    } for link, product, supplier in bindings if link.line_reference in line_set)

    fields: list[dict] = []
    exceptions: list[dict] = []
    if source_claim_error is not None:
        exceptions.append({
            "id": "source:claims-invalid" if source_claim_error == "SOURCE_CLAIMS_INVALID"
            else "source:claims-unavailable",
            "reason": source_claim_error, "status": "OPEN", "blocking": True,
        })
    reuses: list[dict] = []
    for field in db.scalars(select(UsLaceyOperationField).where(
        UsLaceyOperationField.organization_id == org_id,
        UsLaceyOperationField.operation_id == op.id,
    ).order_by(UsLaceyOperationField.id)):
        line = field.merchandise_line_reference or PPQ505_SHIPMENT_REFERENCE
        key = (line, field.field_name)
        decision = authoritative.get(key)
        is_current_document = field.source_assurance_document_id in valid_document_ids
        proven_decision = (
            decision is not None and field.human_value
            and field.human_value == decision.selected_value
            and is_current_document
            and field.source_assurance_document_id in {
                row.assurance_document_id for row in docs_by_decision[decision.id]
            }
        )
        proposed = (field.normalized_value or field.original_value or "").strip()
        verified = None
        verified_record = None
        if (
            field.field_name in _REUSABLE_FIELDS and line in line_set
            and not field.human_value and revision.status == "FINALIZED"
            and snapshot_digest is not None
        ):
            current_values = current_claim_values_for_reuse(
                raw_claims, field_name=field.field_name, line_reference=line,
            )
            if current_values is not None:
                verified = verified_reuse_application(
                    db, organization_id=org_id, operation_id=op.id,
                    source_set_revision_id=revision.id, line_reference=line,
                    field_name=field.field_name, current_claim_values=current_values,
                    current_claim_snapshot_digest=snapshot_digest,
                )
                if verified is not None:
                    memory = db.scalar(select(AssuranceV2MemoryLink).where(
                        AssuranceV2MemoryLink.organization_id == org_id,
                        AssuranceV2MemoryLink.public_id == verified.memory_public_id,
                        AssuranceV2MemoryLink.evidence_claim_id == verified.evidence_claim_id,
                        AssuranceV2MemoryLink.assurance_document_id == verified.origin_document_id,
                    ))
                    claim = db.scalar(select(UsLaceyEvidenceClaim).where(
                        UsLaceyEvidenceClaim.organization_id == org_id,
                        UsLaceyEvidenceClaim.id == verified.evidence_claim_id,
                    ))
                    evidence = db.scalar(select(UsLaceySupplierEvidence).where(
                        UsLaceySupplierEvidence.organization_id == org_id,
                        UsLaceySupplierEvidence.id == claim.evidence_id,
                    )) if claim is not None else None
                    origin = db.scalar(select(AssuranceDocument).where(
                        AssuranceDocument.organization_id == org_id,
                        AssuranceDocument.id == verified.origin_document_id,
                    ))
                    vault = db.scalar(select(VaultDocument).where(
                        VaultDocument.organization_id == org_id,
                        VaultDocument.id == origin.vault_document_id,
                    )) if origin is not None else None
                    identity = next((row for row in identities
                                     if row["line_reference"] == line), None)
                    if (
                        memory is not None and evidence is not None and vault is not None
                        and identity is not None and evidence.status == "VERIFIED"
                        and evidence.document_hash == vault.sha256
                        and evidence.supplier_product_id == identity["product_id"]
                    ):
                        verified_record = {
                            "id": str(verified.memory_public_id),
                            "status": "VERIFIED",
                            "field_name": field.field_name,
                            "line_reference": line,
                            "value": verified.value,
                            "evidence_id": str(verified.evidence_claim_id),
                            "origin_document_id": str(verified.origin_document_id),
                            "origin_decision_id": str(verified.origin_decision_public_id),
                            "source_document_hash": vault.sha256,
                            "supplier_id": identity["supplier_id"],
                            "product_id": identity["product_id"],
                            "verified_by": f"user:{verified.actor_user_id}",
                            "audit_event_id": verified.audit_event_id,
                            "valid_until": evidence.valid_until.isoformat(),
                            "revoked_at": None,
                            "provenance": "REUSED_HISTORICAL",
                            "source_claim_snapshot_digest": verified.source_claim_snapshot_digest,
                        }
                        reuses.append(verified_record)
                    else:
                        exceptions.append({
                            "id": f"reuse:provenance:{field.id}",
                            "reason": "REUSE_PROVENANCE_INVALID", "status": "OPEN",
                            "blocking": True,
                        })
                else:
                    reuse = evaluate_reuse(
                        db, organization_id=org_id, operation_id=op.id,
                        source_set_revision_id=revision.id, line_reference=line,
                        field_name=field.field_name, current_claim_values=current_values,
                    )
                    if reuse.eligible:
                        reuses.append({
                            "id": str(reuse.memory_public_id), "status": "ELIGIBLE",
                            "field_name": field.field_name, "value": reuse.value,
                            "line_reference": line, "evidence_id": str(reuse.evidence_claim_id),
                            "origin_document_id": str(reuse.origin_document_id),
                            "origin_decision_id": str(reuse.origin_decision_public_id),
                            "provenance": "REUSE_PROPOSED_NOT_CANONICAL",
                        })
        chosen = (
            str(field.human_value or "").strip() if proven_decision
            else verified_record["value"] if verified_record
            # A not-required conditional field must not leak an unreviewed
            # parser/calculation artifact (e.g. 0% recycled) into the
            # regulatory output, where any populated field requires evidence.
            else "" if field.field_status == "NOT_REQUIRED"
            else proposed
        )
        authority = (
            "HUMAN_CONFIRMED" if proven_decision else
            "VERIFIED_REUSE" if verified_record else "CANDIDATE"
        )
        evidence_ids = [
            f"v2decision-source:{row.id}" for row in docs_by_decision.get(decision.id, ())
        ] if proven_decision else []
        if verified_record:
            evidence_ids = [verified_record["evidence_id"]]
        item = {
            "id": str(field.id), "name": field.field_name,
            "line_reference": line, "value": chosen,
            "authority": authority,
            "status": "VERIFIED_REUSE" if verified_record else field.field_status,
            "source_locator": field.source_locator if is_current_document and not verified_record else None,
            "document_ids": (
                [str(field.source_assurance_document_id)] if is_current_document and not verified_record else []
            ),
            "evidence_ids": evidence_ids,
            "decision_id": str(decision.public_id) if proven_decision else None,
            "reuse_id": verified_record["id"] if verified_record else None,
        }
        fields.append(item)
        if not proven_decision and not verified_record and field.field_status != "NOT_REQUIRED":
            exceptions.append({
                "id": f"field:{field.id}",
                "line_reference": line,
                "field_name": field.field_name,
                "reason": "Human V2 authority or source evidence is not yet verified.",
                "status": "OPEN", "blocking": True,
                "candidates": ([{
                    "id": f"legacy:{field.id}", "value": proposed,
                    "document_id": str(field.source_assurance_document_id)
                    if is_current_document else None,
                    "locator": field.source_locator,
                    "page": field.source_page,
                }] if proposed else []) + _current_review_source_choices(
                    source_observations, field_name=field.field_name,
                    line_reference=line, identities=identities,
                ),
            })
        if not chosen and source_observations:
            detected = [claim for claim in source_observations if (
                claim.get("field_name") == field.field_name
                and claim.get("support_status") == "SUPPORTED"
                and claim.get("normalized_value")
            )]
            if detected:
                # A supported document literal without a safe entity/line
                # binding must remain a visible abstention, never disappear
                # between the claim stream and the authority workspace.
                exceptions.append({
                    "id": f"source:unpublished:{field.id}",
                    "field_name": field.field_name,
                    "line_reference": line,
                    "reason": (
                        "SOURCE_CLAIM_UNBOUND" if any(
                            not _is_line_bound(claim) for claim in detected
                        ) else "SUPPORTED_CLAIM_UNPUBLISHED"
                    ),
                    "status": "OPEN", "blocking": True,
                    "candidates": [{
                        "id": claim.get("id"),
                        "value": claim.get("normalized_value"),
                        "document_id": str(claim.get("assurance_document_id")),
                        "page": (claim.get("source_locator") or {}).get("page"),
                        "locator": claim.get("source_locator"),
                    } for claim in detected],
                })
    ruleset_version, regulatory_rules = _current_regulatory_rules(
        db, organization_id=org_id, operation_id=op.id, revision=revision,
    )
    output_decisions = tuple({
        "id": str(d.public_id), "actor_id": d.actor_user_id,
        "field_name": d.field_name,
        "line_reference": d.line_reference or PPQ505_SHIPMENT_REFERENCE,
        "value": d.selected_value, "reason": d.reason, "action": d.action,
        "decided_at": d.decided_at.isoformat(),
    } for d in decisions)
    case = AssuranceCase(
        organization_id=org_id,
        operation_id=str(op.public_id),
        source_set_fingerprint=revision.source_set_fingerprint,
        source_set_revision=revision.id,
        current_source_set_fingerprint=revision.source_set_fingerprint,
        current_source_set_revision=revision.id,
        ruleset_version=ruleset_version,
        source_set_generation=revision.generation,
        documents=tuple(documents),
        lines=lines,
        fields=tuple(fields),
        candidate_observations=source_observations or (),
        exceptions=tuple(exceptions),
        decisions=output_decisions,
        reuses=tuple(reuses),
        identities=identities,
        rules=regulatory_rules,
        # No approval is inferred from fields; only an explicit immutable
        # authenticated export-authorizing operation event can fill this.
        export_authorization={},
    )
    from litoral_trace.us_lacey.assurance_v2_export_authorization import authorization_from_event
    authorization = authorization_from_event(
        db, organization_id=org_id, operation_id=op.id, case=case,
    )
    return replace(case, export_authorization=authorization) if authorization else case
