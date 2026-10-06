"""Exact U.S. Lacey supplier/product identity graph for reusable compliance memory.

Supplier identity is created or reused only from exact tenant-scoped identifiers:
MID, vendor code, exact normalized name plus address, or an explicit human
confirmation. Product identity is supplier plus SKU. Shipment line identity stays
independent from SKU and is connected through an explicit source-set-scoped link.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import re
import unicodedata
from typing import Any, Iterable, Mapping
from uuid import UUID

from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    DocumentEntityLink,
    DocumentExtractionRun,
    ExtractedDocumentField,
    UsLaceyOperationDocument,
    UsLaceyOperationProductLink,
    UsLaceyPpqPlantLine,
    UsLaceySupplier,
    UsLaceySupplierIdentifier,
    UsLaceySupplierProduct,
)


_US_LACEY_SUPPLIER_REF_PREFIX = "us_lacey_supplier:"
_RAW_TABLE_FIELD = re.compile(
    r"^raw\.table\.(?P<table>\d+)\.(?P<header>.+)$",
    re.IGNORECASE,
)
_DATA_ROW = re.compile(
    r"(?:^|;)data_row:(?P<row>\d+)(?:;|$)",
    re.IGNORECASE,
)

_SUPPLIER_NAME_HEADERS = frozenset(
    {
        "supplier",
        "supplier name",
        "vendor",
        "vendor name",
        "seller",
        "seller exporter",
        "shipper",
        "manufacturer",
        "manufacturer exporter",
    }
)
_SUPPLIER_ADDRESS_HEADERS = frozenset(
    {
        "supplier address",
        "vendor address",
        "seller address",
        "shipper address",
        "manufacturer address",
        "address",
    }
)
_MID_HEADERS = frozenset(
    {
        "mid",
        "manufacturer id",
        "manufacturer identification",
        "manufacturer identification code",
    }
)
_VENDOR_CODE_HEADERS = frozenset(
    {
        "vendor code",
        "supplier code",
        "vendor id",
        "supplier id",
    }
)
_LINE_HEADERS = frozenset(
    {
        "line",
        "line number",
        "line no",
        "merchandise line",
        "item line",
    }
)
_SKU_HEADERS = frozenset(
    {
        "sku",
        "product sku",
        "item number",
        "item no",
        "item code",
    }
)
_ALLOWED_DISCOVERY_STATUSES = frozenset(
    {"ACTIVE", "DISCOVERED", "NEEDS_VERIFICATION"}
)


@dataclass(frozen=True, slots=True)
class ProductLinkView:
    line_reference: str
    supplier_public_id: UUID
    supplier_product_public_id: UUID
    product_key: str
    sku: str
    link_method: str

    def as_bridge_dict(self) -> dict[str, object]:
        return {
            "line_reference": self.line_reference,
            "supplier_public_id": str(self.supplier_public_id),
            "supplier_product_public_id": str(self.supplier_product_public_id),
            "product_key": self.product_key,
            "sku": self.sku,
            "link_method": self.link_method,
        }


@dataclass(frozen=True, slots=True)
class IdentityResolutionResult:
    supplier_count: int
    product_count: int
    link_count: int
    ambiguous_supplier_count: int
    explicit_links: tuple[ProductLinkView, ...]


@dataclass(frozen=True, slots=True)
class _IdentifierCandidate:
    identifier_type: str
    normalized_value: str
    display_value: str


@dataclass(frozen=True, slots=True)
class _SupplierCandidate:
    assurance_document_id: int
    display_name: str
    normalized_name: str
    identifiers: tuple[_IdentifierCandidate, ...]


def _clean(value: object | None) -> str:
    return str(value or "").strip()


def _normalize_words(value: object | None) -> str:
    text = unicodedata.normalize("NFKD", _clean(value).casefold())
    text = "".join(
        character for character in text if not unicodedata.combining(character)
    )
    return " ".join(re.findall(r"[a-z0-9]+", text))


def _normalize_header(value: object | None) -> str:
    return _normalize_words(value)


def _normalize_code(value: object | None) -> str:
    return re.sub(r"[^A-Z0-9]", "", _clean(value).upper())


def _normalize_sku(value: object | None) -> str:
    return _clean(value).upper()


def _field_value(field: ExtractedDocumentField) -> str:
    return _clean(field.normalized_value or field.original_value)


def _field_header(field: ExtractedDocumentField) -> tuple[int | None, str]:
    raw = _clean(field.field_name)
    match = _RAW_TABLE_FIELD.match(raw)
    if match:
        return int(match.group("table")), _normalize_header(match.group("header"))
    return None, _normalize_header(raw)


def _field_row(field: ExtractedDocumentField) -> int | None:
    match = _DATA_ROW.search(_clean(field.source_locator))
    return int(match.group("row")) if match else None


def _current_document_ids(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
) -> tuple[int, ...]:
    return tuple(
        int(value)
        for value in session.scalars(
            select(UsLaceyOperationDocument.assurance_document_id).where(
                UsLaceyOperationDocument.organization_id == int(organization_id),
                UsLaceyOperationDocument.operation_id == int(operation_id),
                UsLaceyOperationDocument.is_current.is_(True),
            )
        ).all()
    )


def _latest_extracted_fields(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
) -> tuple[ExtractedDocumentField, ...]:
    document_ids = _current_document_ids(
        session,
        organization_id=organization_id,
        operation_id=operation_id,
    )
    if not document_ids:
        return ()

    runs = session.scalars(
        select(DocumentExtractionRun)
        .where(
            DocumentExtractionRun.organization_id == int(organization_id),
            DocumentExtractionRun.assurance_document_id.in_(document_ids),
        )
        .order_by(
            DocumentExtractionRun.assurance_document_id.asc(),
            DocumentExtractionRun.id.desc(),
        )
    ).all()
    latest_run_by_document: dict[int, int] = {}
    for run in runs:
        latest_run_by_document.setdefault(
            int(run.assurance_document_id),
            int(run.id),
        )
    if not latest_run_by_document:
        return ()

    fields = session.scalars(
        select(ExtractedDocumentField)
        .where(
            ExtractedDocumentField.organization_id == int(organization_id),
            ExtractedDocumentField.extraction_run_id.in_(
                tuple(latest_run_by_document.values())
            ),
        )
        .order_by(
            ExtractedDocumentField.assurance_document_id.asc(),
            ExtractedDocumentField.id.asc(),
        )
    ).all()
    return tuple(fields)


def _row_groups(
    fields: Iterable[ExtractedDocumentField],
) -> dict[
    tuple[int, int | None, int | None],
    dict[str, list[ExtractedDocumentField]],
]:
    grouped: dict[
        tuple[int, int | None, int | None],
        dict[str, list[ExtractedDocumentField]],
    ] = {}
    for field in fields:
        table, header = _field_header(field)
        if not header:
            continue
        key = (
            int(field.assurance_document_id),
            table,
            _field_row(field),
        )
        grouped.setdefault(key, {}).setdefault(header, []).append(field)
    return grouped


def _one_value(
    values: Mapping[str, list[ExtractedDocumentField]],
    aliases: frozenset[str],
) -> str | None:
    candidates = {
        _field_value(field)
        for header, fields in values.items()
        if header in aliases
        for field in fields
        if _field_value(field)
    }
    if len(candidates) != 1:
        return None
    return next(iter(candidates))


def _candidate_for_group(
    key: tuple[int, int | None, int | None],
    values: Mapping[str, list[ExtractedDocumentField]],
) -> _SupplierCandidate | None:
    assurance_document_id, _table, _row = key
    name = _one_value(values, _SUPPLIER_NAME_HEADERS)
    address = _one_value(values, _SUPPLIER_ADDRESS_HEADERS)
    mid_raw = _one_value(values, _MID_HEADERS)
    vendor_raw = _one_value(values, _VENDOR_CODE_HEADERS)

    identifiers: list[_IdentifierCandidate] = []
    mid = _normalize_code(mid_raw)
    if mid:
        identifiers.append(
            _IdentifierCandidate(
                identifier_type="MID",
                normalized_value=mid,
                display_value=_clean(mid_raw),
            )
        )
    vendor = _normalize_code(vendor_raw)
    if vendor:
        identifiers.append(
            _IdentifierCandidate(
                identifier_type="VENDOR_CODE",
                normalized_value=vendor,
                display_value=_clean(vendor_raw),
            )
        )

    normalized_name = _normalize_words(name)
    normalized_address = _normalize_words(address)
    if normalized_name and normalized_address:
        identifiers.append(
            _IdentifierCandidate(
                identifier_type="NAME_ADDRESS",
                normalized_value=f"{normalized_name}|{normalized_address}",
                display_value=f"{_clean(name)} | {_clean(address)}",
            )
        )
    if not identifiers:
        return None

    display_name = _clean(name)
    if not display_name:
        strongest = identifiers[0]
        display_name = (
            f"Supplier {strongest.identifier_type} {strongest.display_value}"
        )
        normalized_name = _normalize_words(display_name)
    if not normalized_name:
        return None

    unique: dict[tuple[str, str], _IdentifierCandidate] = {}
    for item in identifiers:
        unique[(item.identifier_type, item.normalized_value)] = item

    return _SupplierCandidate(
        assurance_document_id=assurance_document_id,
        display_name=display_name,
        normalized_name=normalized_name,
        identifiers=tuple(unique.values()),
    )


def _supplier_candidates(
    fields: Iterable[ExtractedDocumentField],
) -> tuple[_SupplierCandidate, ...]:
    candidates: list[_SupplierCandidate] = []
    seen: set[
        tuple[int, tuple[tuple[str, str], ...]]
    ] = set()
    for key, values in _row_groups(fields).items():
        candidate = _candidate_for_group(key, values)
        if candidate is None:
            continue
        fingerprint = (
            candidate.assurance_document_id,
            tuple(
                sorted(
                    (item.identifier_type, item.normalized_value)
                    for item in candidate.identifiers
                )
            ),
        )
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        candidates.append(candidate)
    return tuple(candidates)


def _supplier_key(identifier: _IdentifierCandidate) -> str:
    raw = f"{identifier.identifier_type}:{identifier.normalized_value}"
    if len(raw) <= 128:
        return raw
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"{identifier.identifier_type}:{digest}"


def _identifier_criteria(
    identifiers: tuple[_IdentifierCandidate, ...],
):
    return or_(
        *(
            and_(
                UsLaceySupplierIdentifier.identifier_type == item.identifier_type,
                UsLaceySupplierIdentifier.normalized_value
                == item.normalized_value,
            )
            for item in identifiers
        )
    )


def _identifier_supplier_ids(
    session: Session,
    *,
    organization_id: int,
    identifiers: tuple[_IdentifierCandidate, ...],
) -> set[int]:
    if not identifiers:
        return set()
    return {
        int(value)
        for value in session.scalars(
            select(UsLaceySupplierIdentifier.supplier_id).where(
                UsLaceySupplierIdentifier.organization_id
                == int(organization_id),
                _identifier_criteria(identifiers),
            )
        ).all()
    }


def _ensure_supplier_identifier(
    session: Session,
    *,
    organization_id: int,
    supplier: UsLaceySupplier,
    item: _IdentifierCandidate,
    assurance_document_id: int | None,
    human_confirmed: bool = False,
) -> UsLaceySupplierIdentifier:
    existing = session.scalar(
        select(UsLaceySupplierIdentifier).where(
            UsLaceySupplierIdentifier.organization_id
            == int(organization_id),
            UsLaceySupplierIdentifier.identifier_type == item.identifier_type,
            UsLaceySupplierIdentifier.normalized_value
            == item.normalized_value,
        )
    )
    if existing is not None:
        if int(existing.supplier_id) != int(supplier.id):
            raise ValueError(
                "exact supplier identifier already belongs to another supplier"
            )
        if human_confirmed and not existing.human_confirmed:
            existing.human_confirmed = True
        return existing

    candidate = UsLaceySupplierIdentifier(
        organization_id=int(organization_id),
        supplier_id=int(supplier.id),
        identifier_type=item.identifier_type,
        normalized_value=item.normalized_value,
        display_value=item.display_value[:512] or None,
        source_assurance_document_id=(
            int(assurance_document_id)
            if assurance_document_id is not None
            else None
        ),
        human_confirmed=bool(human_confirmed),
    )
    try:
        with session.begin_nested():
            session.add(candidate)
            session.flush()
        return candidate
    except IntegrityError:
        existing = session.scalar(
            select(UsLaceySupplierIdentifier).where(
                UsLaceySupplierIdentifier.organization_id
                == int(organization_id),
                UsLaceySupplierIdentifier.identifier_type
                == item.identifier_type,
                UsLaceySupplierIdentifier.normalized_value
                == item.normalized_value,
            )
        )
        if existing is None or int(existing.supplier_id) != int(supplier.id):
            raise
        return existing


def _ensure_document_supplier_link(
    session: Session,
    *,
    organization_id: int,
    assurance_document_id: int,
    supplier: UsLaceySupplier,
    human_confirmed: bool = False,
) -> None:
    reference = f"{_US_LACEY_SUPPLIER_REF_PREFIX}{supplier.public_id}"
    existing = session.scalar(
        select(DocumentEntityLink).where(
            DocumentEntityLink.organization_id == int(organization_id),
            DocumentEntityLink.assurance_document_id
            == int(assurance_document_id),
            DocumentEntityLink.entity_type == "SUPPLIER",
            DocumentEntityLink.entity_reference == reference,
        )
    )
    if existing is not None:
        if human_confirmed and not existing.human_confirmed:
            existing.human_confirmed = True
            existing.link_method = "HUMAN_CONFIRMED"
            existing.link_confidence = 1.0
        return

    session.add(
        DocumentEntityLink(
            organization_id=int(organization_id),
            assurance_document_id=int(assurance_document_id),
            entity_type="SUPPLIER",
            entity_reference=reference,
            link_confidence=1.0,
            link_method=(
                "HUMAN_CONFIRMED"
                if human_confirmed
                else "EXACT_IDENTIFIER"
            ),
            human_confirmed=bool(human_confirmed),
        )
    )


def _upsert_supplier(
    session: Session,
    *,
    organization_id: int,
    candidate: _SupplierCandidate,
    discovery_status: str,
) -> UsLaceySupplier | None:
    existing_ids = _identifier_supplier_ids(
        session,
        organization_id=organization_id,
        identifiers=candidate.identifiers,
    )
    if len(existing_ids) > 1:
        return None

    supplier: UsLaceySupplier | None = None
    if existing_ids:
        supplier = session.scalar(
            select(UsLaceySupplier).where(
                UsLaceySupplier.organization_id == int(organization_id),
                UsLaceySupplier.id == next(iter(existing_ids)),
            )
        )

    if supplier is None:
        strongest = candidate.identifiers[0]
        supplier = UsLaceySupplier(
            organization_id=int(organization_id),
            supplier_key=_supplier_key(strongest),
            display_name=candidate.display_name[:255],
            normalized_name=candidate.normalized_name[:255],
            status=discovery_status,
        )
        try:
            with session.begin_nested():
                session.add(supplier)
                session.flush()
        except IntegrityError:
            supplier = session.scalar(
                select(UsLaceySupplier).where(
                    UsLaceySupplier.organization_id
                    == int(organization_id),
                    UsLaceySupplier.supplier_key
                    == _supplier_key(strongest),
                )
            )
            if supplier is None:
                raise

    if (
        discovery_status == "ACTIVE"
        and str(supplier.status)
        in {"DISCOVERED", "NEEDS_VERIFICATION"}
    ):
        supplier.status = "ACTIVE"

    for item in candidate.identifiers:
        _ensure_supplier_identifier(
            session,
            organization_id=organization_id,
            supplier=supplier,
            item=item,
            assurance_document_id=candidate.assurance_document_id,
        )

    _ensure_document_supplier_link(
        session,
        organization_id=organization_id,
        assurance_document_id=candidate.assurance_document_id,
        supplier=supplier,
    )
    return supplier


def _line_sku_observations(
    fields: Iterable[ExtractedDocumentField],
) -> dict[str, set[str]]:
    observations: dict[str, set[str]] = {}
    for _key, values in _row_groups(fields).items():
        line = _one_value(values, _LINE_HEADERS)
        sku = _one_value(values, _SKU_HEADERS)
        normalized_line = _clean(line)
        normalized_sku = _normalize_sku(sku)
        if not normalized_line or not normalized_sku:
            continue
        observations.setdefault(normalized_line, set()).add(
            normalized_sku
        )
    return observations


def _product_compositions(
    payload: Mapping[str, Any],
) -> tuple[tuple[str, str | None], ...]:
    values: dict[str, str | None] = {}
    for source in payload.get("sources", ()):
        if not isinstance(source, Mapping):
            continue
        for table in source.get("tables", ()):
            if not isinstance(table, Mapping):
                continue
            for composition in table.get("compositions", ()):
                if not isinstance(composition, Mapping):
                    continue
                sku = _normalize_sku(composition.get("sku"))
                if not sku:
                    continue
                product_name = (
                    _clean(composition.get("product_name")) or None
                )
                values.setdefault(sku, product_name)
    return tuple(sorted(values.items()))


def _ensure_product(
    session: Session,
    *,
    organization_id: int,
    supplier: UsLaceySupplier,
    sku: str,
    product_name: str | None,
    status: str,
) -> UsLaceySupplierProduct:
    product_key = f"SKU:{_normalize_sku(sku)}"
    product = session.scalar(
        select(UsLaceySupplierProduct).where(
            UsLaceySupplierProduct.organization_id
            == int(organization_id),
            UsLaceySupplierProduct.supplier_id == int(supplier.id),
            UsLaceySupplierProduct.product_key == product_key,
        )
    )
    if product is None:
        product = UsLaceySupplierProduct(
            organization_id=int(organization_id),
            supplier_id=int(supplier.id),
            product_key=product_key,
            sku=_clean(sku)[:128],
            display_name=(
                product_name[:255] if product_name else None
            ),
            normalized_name=(
                _normalize_words(product_name)[:255]
                if product_name
                else _normalize_words(sku)[:255]
            ),
            status=status,
        )
        try:
            with session.begin_nested():
                session.add(product)
                session.flush()
        except IntegrityError:
            product = session.scalar(
                select(UsLaceySupplierProduct).where(
                    UsLaceySupplierProduct.organization_id
                    == int(organization_id),
                    UsLaceySupplierProduct.supplier_id
                    == int(supplier.id),
                    UsLaceySupplierProduct.product_key == product_key,
                )
            )
            if product is None:
                raise
    elif (
        status == "ACTIVE"
        and str(product.status)
        in {"DISCOVERED", "NEEDS_VERIFICATION"}
    ):
        product.status = "ACTIVE"
    return product


def _ensure_exact_product_link(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
    source_set_revision_id: int,
    line_reference: str,
    product: UsLaceySupplierProduct,
) -> UsLaceyOperationProductLink | None:
    line = _clean(line_reference)
    existing = session.scalar(
        select(UsLaceyOperationProductLink).where(
            UsLaceyOperationProductLink.organization_id
            == int(organization_id),
            UsLaceyOperationProductLink.source_set_revision_id
            == int(source_set_revision_id),
            UsLaceyOperationProductLink.line_reference == line,
        )
    )
    if existing is not None:
        if int(existing.supplier_product_id) != int(product.id):
            return None
        return existing

    candidate = UsLaceyOperationProductLink(
        organization_id=int(organization_id),
        operation_id=int(operation_id),
        source_set_revision_id=int(source_set_revision_id),
        line_reference=line,
        supplier_product_id=int(product.id),
        link_method="EXACT_SKU",
        confirmed_by_user_id=None,
        confirmed_at=None,
    )
    try:
        with session.begin_nested():
            session.add(candidate)
            session.flush()
        return candidate
    except IntegrityError:
        existing = session.scalar(
            select(UsLaceyOperationProductLink).where(
                UsLaceyOperationProductLink.organization_id
                == int(organization_id),
                UsLaceyOperationProductLink.source_set_revision_id
                == int(source_set_revision_id),
                UsLaceyOperationProductLink.line_reference == line,
            )
        )
        if (
            existing is None
            or int(existing.supplier_product_id) != int(product.id)
        ):
            return None
        return existing


def operation_product_link_views(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
    source_set_revision_id: int,
) -> tuple[ProductLinkView, ...]:
    rows = session.execute(
        select(
            UsLaceyOperationProductLink,
            UsLaceySupplierProduct,
            UsLaceySupplier,
        )
        .join(
            UsLaceySupplierProduct,
            and_(
                UsLaceySupplierProduct.id
                == UsLaceyOperationProductLink.supplier_product_id,
                UsLaceySupplierProduct.organization_id
                == UsLaceyOperationProductLink.organization_id,
            ),
        )
        .join(
            UsLaceySupplier,
            and_(
                UsLaceySupplier.id
                == UsLaceySupplierProduct.supplier_id,
                UsLaceySupplier.organization_id
                == UsLaceySupplierProduct.organization_id,
            ),
        )
        .where(
            UsLaceyOperationProductLink.organization_id
            == int(organization_id),
            UsLaceyOperationProductLink.operation_id == int(operation_id),
            UsLaceyOperationProductLink.source_set_revision_id
            == int(source_set_revision_id),
            UsLaceySupplier.status.in_(("ACTIVE", "VERIFIED")),
            UsLaceySupplierProduct.status.in_(("ACTIVE", "VERIFIED")),
        )
        .order_by(UsLaceyOperationProductLink.id.asc())
    ).all()

    return tuple(
        ProductLinkView(
            line_reference=str(link.line_reference),
            supplier_public_id=supplier.public_id,
            supplier_product_public_id=product.public_id,
            product_key=str(product.product_key),
            sku=str(product.sku or ""),
            link_method=str(link.link_method),
        )
        for link, product, supplier in rows
        if _clean(product.sku)
    )


def resolve_operation_identity_memory(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
    product_payload: Mapping[str, Any],
    source_set_revision_id: int,
    discovery_status: str = "ACTIVE",
) -> IdentityResolutionResult:
    """Resolve exact suppliers, supplier/SKU products and line/product links.

    This boundary never creates evidence claims. Backfill passes DISCOVERED so
    historical extraction can populate identity rows without making them eligible
    for automatic evidence reuse.
    """
    org_id = int(organization_id)
    op_id = int(operation_id)
    revision_id = int(source_set_revision_id)
    status = str(discovery_status or "").strip().upper()
    if status not in _ALLOWED_DISCOVERY_STATUSES:
        raise ValueError("unsupported discovery_status")

    fields = _latest_extracted_fields(
        session,
        organization_id=org_id,
        operation_id=op_id,
    )
    candidates = _supplier_candidates(fields)

    resolved_suppliers: dict[int, UsLaceySupplier] = {}
    ambiguous_supplier_count = 0
    for candidate in candidates:
        supplier_ids = _identifier_supplier_ids(
            session,
            organization_id=org_id,
            identifiers=candidate.identifiers,
        )
        if len(supplier_ids) > 1:
            ambiguous_supplier_count += 1
            continue
        supplier = _upsert_supplier(
            session,
            organization_id=org_id,
            candidate=candidate,
            discovery_status=status,
        )
        if supplier is None:
            ambiguous_supplier_count += 1
            continue
        resolved_suppliers[int(supplier.id)] = supplier

    if len(resolved_suppliers) != 1:
        return IdentityResolutionResult(
            supplier_count=len(resolved_suppliers),
            product_count=0,
            link_count=0,
            ambiguous_supplier_count=ambiguous_supplier_count,
            explicit_links=(),
        )

    supplier = next(iter(resolved_suppliers.values()))
    line_refs = {
        _clean(value)
        for value in session.scalars(
            select(UsLaceyPpqPlantLine.line_reference).where(
                UsLaceyPpqPlantLine.organization_id == org_id,
                UsLaceyPpqPlantLine.operation_id == op_id,
            )
        ).all()
        if _clean(value)
    }

    observations = _line_sku_observations(fields)
    lines_by_sku: dict[str, set[str]] = {}
    for line_reference, skus in observations.items():
        if line_reference not in line_refs or len(skus) != 1:
            continue
        sku = next(iter(skus))
        lines_by_sku.setdefault(sku, set()).add(line_reference)

    product_count = 0
    for sku, product_name in _product_compositions(product_payload):
        exact_lines = lines_by_sku.get(_normalize_sku(sku), set())
        exact_line = (
            next(iter(exact_lines))
            if len(exact_lines) == 1
            else None
        )

        product_status = status
        if status == "ACTIVE" and exact_line is None:
            product_status = "NEEDS_VERIFICATION"

        product = _ensure_product(
            session,
            organization_id=org_id,
            supplier=supplier,
            sku=sku,
            product_name=product_name,
            status=product_status,
        )
        product_count += 1

        if exact_line is not None:
            _ensure_exact_product_link(
                session,
                organization_id=org_id,
                operation_id=op_id,
                source_set_revision_id=revision_id,
                line_reference=exact_line,
                product=product,
            )

    session.flush()
    views = operation_product_link_views(
        session,
        organization_id=org_id,
        operation_id=op_id,
        source_set_revision_id=revision_id,
    )
    return IdentityResolutionResult(
        supplier_count=len(resolved_suppliers),
        product_count=product_count,
        link_count=len(views),
        ambiguous_supplier_count=ambiguous_supplier_count,
        explicit_links=views,
    )


def confirm_operation_product_link(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
    source_set_revision_id: int,
    line_reference: str,
    supplier_product_id: int,
    user_id: int,
) -> UsLaceyOperationProductLink:
    """Record an explicit human-confirmed line/product relationship."""
    org_id = int(organization_id)
    product = session.scalar(
        select(UsLaceySupplierProduct).where(
            UsLaceySupplierProduct.organization_id == org_id,
            UsLaceySupplierProduct.id == int(supplier_product_id),
        )
    )
    if product is None:
        raise ValueError("supplier product not found")

    line = _clean(line_reference)
    existing = session.scalar(
        select(UsLaceyOperationProductLink).where(
            UsLaceyOperationProductLink.organization_id == org_id,
            UsLaceyOperationProductLink.operation_id == int(operation_id),
            UsLaceyOperationProductLink.source_set_revision_id
            == int(source_set_revision_id),
            UsLaceyOperationProductLink.line_reference == line,
        )
    )
    if (
        existing is not None
        and int(existing.supplier_product_id) != int(product.id)
    ):
        raise ValueError(
            "shipment line is already bound to another supplier product"
        )

    now = datetime.now(timezone.utc)
    if existing is None:
        existing = UsLaceyOperationProductLink(
            organization_id=org_id,
            operation_id=int(operation_id),
            source_set_revision_id=int(source_set_revision_id),
            line_reference=line,
            supplier_product_id=int(product.id),
            link_method="HUMAN_CONFIRMED",
            confirmed_by_user_id=int(user_id),
            confirmed_at=now,
        )
        session.add(existing)
    else:
        existing.link_method = "HUMAN_CONFIRMED"
        existing.confirmed_by_user_id = int(user_id)
        existing.confirmed_at = now

    if str(product.status) in {"DISCOVERED", "NEEDS_VERIFICATION"}:
        product.status = "ACTIVE"

    supplier = session.scalar(
        select(UsLaceySupplier).where(
            UsLaceySupplier.organization_id == org_id,
            UsLaceySupplier.id == int(product.supplier_id),
        )
    )
    if (
        supplier is not None
        and str(supplier.status)
        in {"DISCOVERED", "NEEDS_VERIFICATION"}
    ):
        supplier.status = "ACTIVE"

    session.flush()
    return existing


__all__ = [
    "IdentityResolutionResult",
    "ProductLinkView",
    "confirm_operation_product_link",
    "operation_product_link_views",
    "resolve_operation_identity_memory",
]
