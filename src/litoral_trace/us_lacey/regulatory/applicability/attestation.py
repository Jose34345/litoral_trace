"""Evidence-bound human attestation for Lacey *preliminary* applicability.

An immutable, tenant-scoped audit event records reviewed assumptions and the
exact current source-set fingerprint. This NEVER changes canonical declaration
scope, operation readiness, regulatory gates, or filing/export authority.
"""
from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from uuid import UUID
import hashlib
import re

from sqlalchemy import select

from litoral_trace.db.models import (
    AssuranceDocument, UsLaceyOperation, UsLaceyOperationDocument,
    UsLaceyOperationField, UsLaceyOperationEvent, UsLaceySourceSetMember,
    UsLaceySourceSetRevision, VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.audit_trail import (
    OperationActorType, OperationEventType, append_operation_event,
)
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.document_evidence_class import classify_document
from litoral_trace.us_lacey.regulatory.applicability.triage import (
    HTS10, REQUIRED_TYPES, NOT_REQUIRED_TYPES, SPECIAL_TYPES,
    preview_applicability,
)

KIND = "LACEY_APPLICABILITY_EVIDENCE_ATTESTATION_V1"


class ApplicabilityAttestationError(ValueError):
    pass


@dataclass(frozen=True)
class ApplicabilitySource:
    id: int
    filename: str
    classification: str
    is_primary_candidate: bool
    page_available: bool
    source_set_fingerprint: str = ""


@dataclass(frozen=True)
class RecordedApplicability:
    current: bool
    entry_type: str
    bamboo_origin: str
    hts10: str
    preview_status: str
    filename: str
    botanical_filename: str
    source_page: int
    reviewed_by: str
    source_set_fingerprint: str


def _current_context(db, organization_id: int, operation_public_id: str):
    try:
        op_id = UUID(str(operation_public_id))
    except (ValueError, TypeError):
        return None, None
    op = db.scalar(select(UsLaceyOperation).where(
        UsLaceyOperation.organization_id == organization_id,
        UsLaceyOperation.public_id == op_id,
    ))
    if op is None:
        return None, None
    revision = db.scalar(select(UsLaceySourceSetRevision).where(
        UsLaceySourceSetRevision.organization_id == organization_id,
        UsLaceySourceSetRevision.operation_id == op.id,
        UsLaceySourceSetRevision.is_current.is_(True),
        UsLaceySourceSetRevision.status == "FINALIZED",
    ))
    return op, revision


def _source_rows(db, *, organization_id: int, operation_id: int, revision_id: int):
    return db.execute(
        select(UsLaceyOperationDocument, VaultDocument)
        .join(UsLaceySourceSetMember,
              UsLaceySourceSetMember.operation_document_id == UsLaceyOperationDocument.id)
        .join(AssuranceDocument,
              AssuranceDocument.id == UsLaceyOperationDocument.assurance_document_id)
        .join(VaultDocument, VaultDocument.id == AssuranceDocument.vault_document_id)
        .where(
            UsLaceyOperationDocument.organization_id == organization_id,
            UsLaceyOperationDocument.operation_id == operation_id,
            UsLaceyOperationDocument.is_current.is_(True),
            UsLaceySourceSetMember.organization_id == organization_id,
            UsLaceySourceSetMember.source_set_revision_id == revision_id,
            UsLaceySourceSetMember.assurance_document_id == UsLaceyOperationDocument.assurance_document_id,
            AssuranceDocument.organization_id == organization_id,
            VaultDocument.organization_id == organization_id,
        )
    ).all()


def _source_class(link, vault):
    return classify_document(str(vault.original_filename), str(link.document_role))


def _usable_source(link, vault):
    return (str(vault.status) == "available" and vault.deleted_at is None
            and _source_class(link, vault).code != "DERIVED_PREPARATION")


def read_applicability_evidence(
    *, organization_id: int, operation_public_id: str, session_factory=None,
) -> tuple[tuple[ApplicabilitySource, ...], RecordedApplicability | None]:
    db = (session_factory or get_us_lacey_db_session)()
    try:
        set_tenant_db_context(db, int(organization_id))
        op, revision = _current_context(db, int(organization_id), operation_public_id)
        if op is None or revision is None:
            return (), None
        rows = _source_rows(db, organization_id=int(organization_id),
                            operation_id=op.id, revision_id=revision.id)
        sources = tuple(
            ApplicabilitySource(
                id=int(link.assurance_document_id),
                filename=str(vault.original_filename),
                classification=_source_class(link, vault).label,
                is_primary_candidate=_usable_source(link, vault),
                page_available=str(vault.content_type).lower() == "application/pdf"
                                and _usable_source(link, vault),
                source_set_fingerprint=str(revision.source_set_fingerprint),
            )
            for link, vault in rows
        )
        audit = db.scalars(select(UsLaceyOperationEvent).where(
            UsLaceyOperationEvent.organization_id == int(organization_id),
            UsLaceyOperationEvent.operation_id == op.id,
            UsLaceyOperationEvent.event_type == "HUMAN_REVIEW",
        ).order_by(UsLaceyOperationEvent.id.desc()).limit(80)).all()
        for item in audit:
            value = dict(item.details or {})
            if value.get("kind") != KIND:
                continue
            source_id = int(value.get("source_document_id") or 0)
            matching = next(((link, vault) for link, vault in rows
                             if int(link.assurance_document_id) == source_id), None)
            botanical_id = int(value.get("botanical_document_id") or 0)
            botanical = next(((link, vault) for link, vault in rows if int(link.assurance_document_id) == botanical_id), None)
            botanical_current = (not botanical_id or (botanical is not None
                and str(botanical[1].sha256) == value.get("botanical_source_sha256")
                and str(botanical[0].version_number) == value.get("botanical_source_version")
                and _usable_source(*botanical)))
            current = (
                botanical_current
                and value.get("source_set_fingerprint") == revision.source_set_fingerprint
                and matching is not None
                and str(matching[1].sha256) == value.get("source_sha256")
                and str(matching[0].version_number) == value.get("source_version")
                and _usable_source(*matching)
            )
            return sources, RecordedApplicability(
                current=current,
                entry_type=str(value.get("entry_type") or ""),
                bamboo_origin=str(value.get("bamboo_origin") or ""),
                hts10=str(value.get("hts10") or ""),
                preview_status=str(value.get("preview_status") or ""),
                filename="" if matching is None else str(matching[1].original_filename),
                botanical_filename="" if botanical is None else str(botanical[1].original_filename),
                source_page=int(value.get("source_page") or 1),
                reviewed_by=str(item.actor_identity or ""),
                source_set_fingerprint=str(value.get("source_set_fingerprint") or ""),
            )
        return sources, None
    finally:
        db.close()


def record_applicability_attestation(
    *, organization_id: int, operation_public_id: str, reviewer_user_id: int,
    entry_type: str, bamboo_origin: str, source_document_id: int,
    source_page: int, explicitly_confirmed: bool, botanical_document_id: int = 0,
    botanical_page: int = 1, session_factory=None,
) -> dict:
    if not explicitly_confirmed:
        raise ApplicabilityAttestationError("Inspect the original and explicitly confirm this review.")
    if str(entry_type) not in REQUIRED_TYPES | NOT_REQUIRED_TYPES | SPECIAL_TYPES:
        raise ApplicabilityAttestationError("Choose a valid CBP entry type.")
    if not (1 <= int(source_page) <= 10000 and int(source_document_id) > 0):
        raise ApplicabilityAttestationError("Choose a linked source and a valid PDF page.")
    if botanical_document_id and not (1 <= int(botanical_page) <= 10000):
        raise ApplicabilityAttestationError("Choose a valid botanical PDF page.")
    if str(bamboo_origin) not in {"CULTIVATED", "WILD", "UNKNOWN", ""}:
        raise ApplicabilityAttestationError("Invalid bamboo cultivation selection.")
    db = (session_factory or get_us_lacey_db_session)()
    try:
        set_tenant_db_context(db, int(organization_id))
        op, revision = _current_context(db, int(organization_id), operation_public_id)
        if op is None or revision is None:
            raise ApplicabilityAttestationError("Current finalized document revision required.")
        rows = _source_rows(db, organization_id=int(organization_id),
                            operation_id=op.id, revision_id=revision.id)
        found = next(((link, vault) for link, vault in rows
                      if int(link.assurance_document_id) == int(source_document_id)), None)
        if found is None or not _usable_source(*found):
            raise ApplicabilityAttestationError("Source is unavailable, stale or derived. Inspect a primary source.")
        link, vault = found
        if str(vault.content_type).lower() != "application/pdf":
            raise ApplicabilityAttestationError("Choose an original PDF page, not a non-PDF attachment.")

        fields = db.scalars(select(UsLaceyOperationField).where(
            UsLaceyOperationField.organization_id == int(organization_id),
            UsLaceyOperationField.operation_id == op.id,
        )).all()
        hts_fields = [field for field in fields if str(field.field_name) == "hts_code"]
        if (len(hts_fields) != 1 or
                str(hts_fields[0].field_status) != "CONFIRMED" or
                not str(hts_fields[0].human_value or "").strip()):
            raise ApplicabilityAttestationError(
                "Confirm the current HTS as a reviewed field before recording applicability."
            )
        detail = SimpleNamespace(fields=[
            SimpleNamespace(field_name=f.field_name,
                            effective_value=f.human_value if str(f.field_status) == "CONFIRMED" else None,
                            proposed_value=f.normalized_value or f.original_value)
            for f in fields
        ])
        preview = preview_applicability(detail, entry_type=entry_type, bamboo_origin=bamboo_origin)
        if not HTS10.fullmatch(preview.hts10):
            raise ApplicabilityAttestationError("Verify a current 10-digit HTS before recording this review.")
        if preview.is_bamboo and not bamboo_origin:
            raise ApplicabilityAttestationError("Select cultivated, wild or unknown origin for bamboo.")
        if not preview.is_bamboo and bamboo_origin:
            raise ApplicabilityAttestationError("Bamboo status cannot be assigned to non-bamboo merchandise.")
        botanical_link = botanical_vault = None
        if preview.is_bamboo and bamboo_origin in {"CULTIVATED", "WILD"}:
            botanical = next(((row_link, row_vault) for row_link, row_vault in rows
                              if int(row_link.assurance_document_id) == int(botanical_document_id)), None)
            if botanical is None or not _usable_source(*botanical):
                raise ApplicabilityAttestationError(
                    "Select a current original supplier PDF supporting bamboo origin."
                )
            botanical_link, botanical_vault = botanical
            if (_source_class(botanical_link, botanical_vault).code != "SUPPLIER_SOURCE"
                    or str(botanical_vault.content_type).lower() != "application/pdf"):
                raise ApplicabilityAttestationError(
                    "Cultivated or wild bamboo origin needs supplier-origin PDF evidence."
                )
        elif botanical_document_id:
            raise ApplicabilityAttestationError("Botanical evidence may only be attached to an evidenced origin claim.")
        details = {
            "kind": KIND,
            "source_set_fingerprint": str(revision.source_set_fingerprint),
            "source_document_id": int(source_document_id),
            "source_sha256": str(vault.sha256),
            "source_version": str(link.version_number),
            "source_page": int(source_page),
            "botanical_document_id": int(botanical_document_id),
            "botanical_source_sha256": "" if botanical_vault is None else str(botanical_vault.sha256),
            "botanical_source_version": "" if botanical_link is None else str(botanical_link.version_number),
            "botanical_page": int(botanical_page) if botanical_vault is not None else 0,
            "entry_type": str(entry_type),
            "bamboo_origin": str(bamboo_origin),
            "hts10": str(preview.hts10),
            "preview_status": str(preview.status),
            "catalog_version": str(preview.catalog_version),
            "advisory_only": True,
            "filing_authorization": False,
        }
        material = repr(sorted(details.items())).encode("utf-8")
        event_key = "lacey-applicability-v1:" + hashlib.sha256(
            str(reviewer_user_id).encode() + b":" + material
        ).hexdigest()
        append_operation_event(
            db,
            organization_id=int(organization_id),
            operation_id=op.id,
            actor_type=OperationActorType.USER,
            actor_identity=str(int(reviewer_user_id)),
            event_type=OperationEventType.HUMAN_REVIEW,
            details=details,
            event_key=event_key,
        )
        db.commit()
        return details
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
