"""Field-specific documentary admissibility for direct or batch review.

Only a verified document *role* can grant this narrowly scoped admissibility.
Names are negative mismatch signals, never positive proof of authority.
A candidate passing this gate still needs exact source ID, current source-set,
PPQ validation, and an explicit human action to become MATCHED.
"""
from __future__ import annotations

import re

from litoral_trace.lacey_engine.source_authority import has_listed_field_source
from litoral_trace.us_lacey.document_evidence_class import classify_document

_SHIPPING_REFERENCE_FIELDS = frozenset({
    "bill_of_lading", "master_bill_of_lading", "house_bill_of_lading",
})
_SHIPPING_REFERENCE_ROLES = frozenset({"BILL_OF_LADING", "ARRIVAL_NOTICE"})
_SUPPLIER_NAME = re.compile(
    r"\b(supplier|botanical|harvest|species)\b.*\b(declaration|certificate)\b"
)


def source_role_supports_field(
    field_name: str,
    document_role: str | None,
    filename: str | None = None,
) -> bool:
    """Reject inappropriate shipping sources without inferring a role from a name."""
    key = str(field_name or "").strip().casefold()
    role = str(document_role or "").strip().upper()
    if key == "estimated_arrival_date":
        return classify_document(filename, role).code not in {
            "SUPPLIER_SOURCE", "UNVERIFIED_SUPPLIER", "ROLE_CONFLICT",
        }
    if key not in _SHIPPING_REFERENCE_FIELDS:
        return True
    name = re.sub(r"[^a-z0-9]+", " ", str(filename or "").casefold())
    if _SUPPLIER_NAME.search(name):
        return False
    return role in _SHIPPING_REFERENCE_ROLES


def filename_clearly_incompatible(field_name: str, filename: str | None) -> bool:
    """Presentation hint, not authority. Writer rechecks actual DB role."""
    if str(field_name or "").strip().casefold() not in _SHIPPING_REFERENCE_FIELDS:
        return False
    name = re.sub(r"[^a-z0-9]+", " ", str(filename or "").casefold())
    return bool(_SUPPLIER_NAME.search(name))


def is_preparation_source(document_type: str, filename: str) -> bool:
    """A known preparation worksheet can provide a proposal but no authority."""
    # Engine 2 uses this type for the actual CBP Form 7501. The historical
    # upload-role classifier also uses that string for preparation records.
    role = None if document_type == "CUSTOMS_ENTRY_SUMMARY" else document_type
    return classify_document(filename, role).code == "DERIVED_PREPARATION"


def is_extracted_header_source(
    field_name: str, *, document_type: str, filename: str, source_page: int | None,
    evidence_class: str,
) -> bool:
    """Require a listed document type and literal evidence before proposing support."""
    return (
        (source_page or 0) > 0
        and evidence_class == "EXPLICIT"
        and has_listed_field_source(field_name, document_type)
        and source_role_supports_field(field_name, document_type, filename)
        and not is_preparation_source(document_type, filename)
        and classify_document(filename, document_type).code != "ROLE_CONFLICT"
    )
