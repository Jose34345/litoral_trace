"""A strictly bounded *human* Article / Component review from original packing evidence.

A shipment description is NOT an Article / Component claim. It may be shown
as context only when independently linked plant-mass fields identify one
current packing document and plant line. The reviewer must supply the article
value and their documentary reasoning; nothing is approved automatically.
"""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import func, select

from litoral_trace.db.models import (
    UsLaceyOperationDocument, UsLaceyOperationField, UsLaceyPpqPlantLine,
    UsLaceyProcessingJob, UsLaceySourceSetRevision,
)
from litoral_trace.us_lacey.document_evidence_class import classify_document
from litoral_trace.us_lacey.review_source_authority import reviewed_source_class


@dataclass(frozen=True)
class ArticleOriginalContext:
    description_field_id: int
    document_id: int
    page: int
    locator: str
    description: str
    filename: str


def contexts_for_detail(detail) -> dict[int, ArticleOriginalContext]:
    """Advisory display only. The mutation repeats all checks in PostgreSQL."""
    fields = tuple(getattr(detail, "fields", ()))
    articles = [f for f in fields if f.field_name == "article_component" and f.status == "REVIEW"]
    if len(articles) != 1:
        return {}
    article = articles[0]
    if not article.source_filename or classify_document(article.source_filename).code != "DERIVED_PREPARATION":
        return {}
    description = [f for f in fields if f.field_name == "merchandise_description"
                   and f.scope == "SHIPMENT" and f.source_assurance_document_id
                   and f.effective_value and f.source_locator and f.source_page]
    lines = {f.line_reference for f in fields if f.field_name == "article_component"}
    if len(description) != 1 or len(lines) != 1:
        return {}
    source = description[0]
    if classify_document(source.source_filename).code not in ("UNVERIFIED_COMMERCIAL", "COMMERCIAL_SOURCE"):
        return {}
    # Link the original shipment description to this PLANT LINE via the
    # independently source-linked and identical packing mass + unit.
    mass = [f for f in fields if f.line_reference == article.line_reference
            and f.field_name in ("plant_quantity", "metric_unit")
            and f.source_assurance_document_id == source.source_assurance_document_id
            and f.source_locator and f.effective_value]
    if {f.field_name for f in mass} != {"plant_quantity", "metric_unit"}:
        return {}
    return {article.id: ArticleOriginalContext(
        description_field_id=source.id,
        document_id=source.source_assurance_document_id,
        page=source.source_page,
        locator=source.source_locator,
        description=source.effective_value,
        filename=source.source_filename,
    )}


def validate_article_original_context(
    session, *, organization_id: int, operation_id: int, article_field,
    description_field_id: int, expected_locator: str,
) -> ArticleOriginalContext:
    """Fail closed on stale membership, multiple lines, other orgs or changed source."""
    def refuse():
        raise ValueError("Original context changed or cannot be linked to this plant line. Reload the review.")

    if article_field.field_name != "article_component" or article_field.field_scope != "PLANT_LINE":
        refuse()
    if article_field.field_status != "REVIEW" or article_field.reviewed_at is not None or article_field.human_value:
        refuse()
    if not article_field.plant_line_id or not article_field.merchandise_line_reference:
        refuse()
    original_class = reviewed_source_class(
        session, organization_id=organization_id, operation_id=operation_id,
        assurance_document_id=article_field.source_assurance_document_id,
    )
    if original_class is None or original_class.code != "DERIVED_PREPARATION":
        refuse()
    # In this first P0, never infer cross-line allocations for multiple
    # products from a shipment-level description.
    line_count = session.scalar(select(func.count(UsLaceyPpqPlantLine.id)).where(
        UsLaceyPpqPlantLine.organization_id == organization_id,
        UsLaceyPpqPlantLine.operation_id == operation_id,
    )) or 0
    if line_count != 1:
        refuse()
    revision = session.scalar(select(UsLaceySourceSetRevision).where(
        UsLaceySourceSetRevision.organization_id == organization_id,
        UsLaceySourceSetRevision.operation_id == operation_id,
        UsLaceySourceSetRevision.is_current.is_(True),
        UsLaceySourceSetRevision.status == "FINALIZED",
    ).with_for_update())
    if revision is None:
        refuse()
    active = session.scalar(select(func.count(UsLaceyProcessingJob.id)).where(
        UsLaceyProcessingJob.organization_id == organization_id,
        UsLaceyProcessingJob.operation_id == operation_id,
        UsLaceyProcessingJob.status.in_(("QUEUED", "RUNNING", "RETRY", "FAILED")),
    )) or 0
    if active:
        refuse()
    source = session.scalar(select(UsLaceyOperationField).where(
        UsLaceyOperationField.organization_id == organization_id,
        UsLaceyOperationField.operation_id == operation_id,
        UsLaceyOperationField.id == description_field_id,
        UsLaceyOperationField.field_name == "merchandise_description",
        UsLaceyOperationField.field_scope == "SHIPMENT",
    ).with_for_update())
    if (source is None or not source.source_assurance_document_id
        or source.field_status not in {"REVIEW", "FOUND", "SUPPORTED", "MATCHED"}
        or not source.source_page or not source.source_locator
        or source.source_locator != expected_locator
        or not (source.normalized_value or source.original_value)
        or not source.source_locator.startswith("canonical:")):
        refuse()
    doc_id = int(source.source_assurance_document_id)
    source_class = reviewed_source_class(
        session, organization_id=organization_id, operation_id=operation_id,
        assurance_document_id=doc_id,
    )
    if source_class is None or source_class.code not in ("COMMERCIAL_SOURCE", "UNVERIFIED_COMMERCIAL"):
        refuse()
    membership = session.scalar(select(UsLaceyOperationDocument).where(
        UsLaceyOperationDocument.organization_id == organization_id,
        UsLaceyOperationDocument.operation_id == operation_id,
        UsLaceyOperationDocument.assurance_document_id == doc_id,
        UsLaceyOperationDocument.is_current.is_(True),
    ))
    if membership is None:
        refuse()
    quantities = session.scalars(select(UsLaceyOperationField).where(
        UsLaceyOperationField.organization_id == organization_id,
        UsLaceyOperationField.operation_id == operation_id,
        UsLaceyOperationField.plant_line_id == article_field.plant_line_id,
        UsLaceyOperationField.merchandise_line_reference == article_field.merchandise_line_reference,
        UsLaceyOperationField.field_name.in_(("plant_quantity", "metric_unit")),
    ).with_for_update()).all()
    if ({f.field_name for f in quantities} != {"plant_quantity", "metric_unit"}
        or any(f.source_assurance_document_id != doc_id
               or f.field_status not in {"REVIEW", "FOUND", "SUPPORTED", "MATCHED"}
               or not (f.normalized_value or f.human_value)
               or not f.source_locator or not f.source_locator.startswith("canonical:")
               for f in quantities)):
        refuse()
    # No interpretation or automatic agreement on "solid bamboo coasters".
    # Human explicitly supplies the value and an explanation.
    return ArticleOriginalContext(
        description_field_id=source.id, document_id=doc_id,
        page=int(source.source_page), locator=source.source_locator,
        description=source.normalized_value or source.original_value,
        filename=source_class.label,
    )
