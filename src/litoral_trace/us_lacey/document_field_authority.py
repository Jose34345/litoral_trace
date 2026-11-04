"""Field-specific documentary admissibility for direct or batch review.

Only a verified document *role* can grant this narrowly scoped admissibility.
Names are negative mismatch signals, never positive proof of authority.
A candidate passing this gate still needs exact source ID, current source-set,
PPQ validation, and an explicit human action to become MATCHED.
"""
from __future__ import annotations

import re

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
    """Do not treat a botanical/supplier reference as a carrier-issued B/L.

    Other fields keep their existing review policy. This intentionally does
    not infer a positive role from filename (or from an AI candidate).
    """
    key = str(field_name or "").strip().casefold()
    if key not in _SHIPPING_REFERENCE_FIELDS:
        return True
    role = str(document_role or "").strip().upper()
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
