"""Minimal supplier identity resolution for document-first Assurance workflows.

Only deterministic, trusted extracted fields are allowed to create or link a
supplier. A valid normalized CUIT is the creation key. Name-only evidence may
reuse an existing exact normalized identity but never creates a new supplier,
which keeps the workflow conservative and avoids fuzzy writes.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
import unicodedata
from typing import Callable
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from litoral_trace.assurance.normalization import NormalizationError, normalize_cuit
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import (
    AssuranceDocument,
    AssuranceSupplier,
    DocumentEntityLink,
    DocumentExtractionRun,
    ExtractedDocumentField,
)
from litoral_trace.db.tenant import set_tenant_db_context


SessionFactory = Callable[[], Session | None]


class AssuranceSupplierError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SupplierResolutionResult:
    supplier_public_id: UUID | None
    created: bool
    enriched: bool
    linked: bool
    needs_review: bool
    reason: str


def normalize_supplier_name(value: object) -> str:
    """Normalize exact business names without introducing fuzzy matching."""
    text = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    text = "".join(character for character in text if not unicodedata.combining(character))
    tokens = re.findall(r"[a-z0-9]+", text)
    if not tokens:
        return ""

    # Legal suffixes are frequently written both as ``S.A.`` and ``SA``.
    # Collapse only consecutive one-letter alphabetic tokens; this keeps the
    # comparison deterministic while avoiding generic fuzzy-name behaviour.
    normalized: list[str] = []
    index = 0
    while index < len(tokens):
        if len(tokens[index]) == 1 and tokens[index].isalpha():
            run: list[str] = []
            while (
                index < len(tokens)
                and len(tokens[index]) == 1
                and tokens[index].isalpha()
            ):
                run.append(tokens[index])
                index += 1
            normalized.append("".join(run))
            continue
        normalized.append(tokens[index])
        index += 1
    return " ".join(normalized)


def _unique_values(fields: list[ExtractedDocumentField], field_name: str) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                str(field.normalized_value or "").strip()
                for field in fields
                if field.field_name == field_name
                and str(field.normalized_value or "").strip()
            }
        )
    )


class AssuranceSupplierService:
    def __init__(self, *, session_factory: SessionFactory | None = None) -> None:
        self._session_factory = session_factory or get_db_session

    def resolve_document(
        self,
        *,
        organization_id: int,
        assurance_public_id: UUID | str,
    ) -> SupplierResolutionResult:
        org_id = int(organization_id)
        public_id = (
            assurance_public_id
            if isinstance(assurance_public_id, UUID)
            else UUID(str(assurance_public_id))
        )
        session = self._session_factory()
        if session is None:
            raise AssuranceSupplierError("No se pudo abrir una sesión para resolver proveedor.")
        set_tenant_db_context(session, org_id)
        try:
            document = session.scalar(
                select(AssuranceDocument).where(
                    AssuranceDocument.organization_id == org_id,
                    AssuranceDocument.public_id == public_id,
                )
            )
            if document is None:
                raise AssuranceSupplierError("Documento Assurance no encontrado.")

            latest_run = session.scalar(
                select(DocumentExtractionRun)
                .where(
                    DocumentExtractionRun.organization_id == org_id,
                    DocumentExtractionRun.assurance_document_id == document.id,
                )
                .order_by(DocumentExtractionRun.id.desc())
            )
            if latest_run is None:
                return SupplierResolutionResult(None, False, False, False, False, "no_extraction_run")

            trusted_fields = session.scalars(
                select(ExtractedDocumentField).where(
                    ExtractedDocumentField.organization_id == org_id,
                    ExtractedDocumentField.assurance_document_id == document.id,
                    ExtractedDocumentField.extraction_run_id == latest_run.id,
                    or_(
                        ExtractedDocumentField.auto_accepted.is_(True),
                        ExtractedDocumentField.needs_review.is_(False),
                    ),
                    ExtractedDocumentField.field_name.in_(("issuer_cuit", "supplier")),
                )
            ).all()
            cuits = _unique_values(trusted_fields, "issuer_cuit")
            names = _unique_values(trusted_fields, "supplier")

            if len(cuits) > 1 or len(names) > 1:
                return SupplierResolutionResult(
                    None, False, False, False, True, "ambiguous_document_identity"
                )

            raw_cuit = cuits[0] if cuits else None
            display_name = names[0] if names else None
            normalized_name = normalize_supplier_name(display_name) if display_name else None

            cuit: str | None = None
            if raw_cuit is not None:
                try:
                    cuit = normalize_cuit(raw_cuit)
                except NormalizationError:
                    return SupplierResolutionResult(
                        None, False, False, False, True, "invalid_normalized_cuit"
                    )

            supplier: AssuranceSupplier | None = None
            created = False
            enriched = False
            needs_review = False

            if cuit is not None:
                supplier = session.scalar(
                    select(AssuranceSupplier).where(
                        AssuranceSupplier.organization_id == org_id,
                        AssuranceSupplier.cuit == cuit,
                    )
                )
                if supplier is None:
                    candidate = AssuranceSupplier(
                        organization_id=org_id,
                        cuit=cuit,
                        display_name=display_name,
                        normalized_name=normalized_name,
                        status="AUTO_CREATED",
                        source_assurance_document_id=document.id,
                    )
                    try:
                        # A savepoint makes simultaneous first-seen documents for
                        # the same CUIT converge on the unique tenant identity.
                        with session.begin_nested():
                            session.add(candidate)
                            session.flush()
                        supplier = candidate
                        created = True
                    except IntegrityError:
                        supplier = session.scalar(
                            select(AssuranceSupplier).where(
                                AssuranceSupplier.organization_id == org_id,
                                AssuranceSupplier.cuit == cuit,
                            )
                        )
                        if supplier is None:
                            raise
                if supplier is not None and normalized_name:
                    if supplier.normalized_name is None:
                        supplier.display_name = display_name
                        supplier.normalized_name = normalized_name
                        enriched = not created
                    elif supplier.normalized_name != normalized_name:
                        supplier.status = "NEEDS_REVIEW"
                        needs_review = True
            elif normalized_name:
                matches = session.scalars(
                    select(AssuranceSupplier).where(
                        AssuranceSupplier.organization_id == org_id,
                        AssuranceSupplier.normalized_name == normalized_name,
                    )
                ).all()
                if len(matches) == 1:
                    supplier = matches[0]
                elif len(matches) > 1:
                    return SupplierResolutionResult(
                        None, False, False, False, True, "ambiguous_name_match"
                    )
                else:
                    return SupplierResolutionResult(
                        None, False, False, False, True, "name_only_cannot_create"
                    )
            else:
                return SupplierResolutionResult(
                    None, False, False, False, False, "no_supplier_evidence"
                )

            assert supplier is not None
            reference = f"supplier:{supplier.public_id}"
            existing_link = session.scalar(
                select(DocumentEntityLink).where(
                    DocumentEntityLink.organization_id == org_id,
                    DocumentEntityLink.assurance_document_id == document.id,
                    DocumentEntityLink.entity_type == "SUPPLIER",
                    DocumentEntityLink.entity_reference == reference,
                )
            )
            linked = existing_link is None
            if linked:
                session.add(
                    DocumentEntityLink(
                        organization_id=org_id,
                        assurance_document_id=document.id,
                        entity_type="SUPPLIER",
                        entity_reference=reference,
                        link_confidence=1.0 if cuit else 0.95,
                        link_method="EXACT_CUIT" if cuit else "NORMALIZED_NAME",
                        human_confirmed=False,
                    )
                )
            session.commit()
            return SupplierResolutionResult(
                supplier.public_id,
                created,
                enriched,
                linked,
                needs_review,
                "resolved_with_cuit" if cuit else "resolved_with_exact_name",
            )
        except (ValueError, AssuranceSupplierError):
            session.rollback()
            raise
        except Exception as exc:
            session.rollback()
            raise AssuranceSupplierError("No se pudo resolver el proveedor documental.") from exc
        finally:
            session.close()
