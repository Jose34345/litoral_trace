"""Explain documentary provenance without turning document type into authority.

Source class is a conservative informational descriptor, not an autonomous
claim confirmation. Filename heuristics NEVER overrule an explicit derivative
role or promote a preparation worksheet to primary evidence.
"""
from __future__ import annotations
from dataclasses import dataclass
import re

@dataclass(frozen=True)
class SourceClass:
    code: str
    label: str
    message: str
    inferred: bool

_DERIVED_ROLES = {"ENTRY_WORKSHEET", "CUSTOMS_ENTRY_SUMMARY", "PREPARATION_WORKSHEET", "WORKSHEET"}
_COMMERCIAL_ROLES = {"COMMERCIAL_INVOICE", "PACKING_LIST", "BILL_OF_LADING", "ARRIVAL_NOTICE"}
_SUPPLIER_ROLES = {"SUPPLIER_DECLARATION", "SPECIFICATION", "CERTIFICATE"}

def classify_document(filename: str, document_role: str | None = None) -> SourceClass:
    role = str(document_role or "").strip().upper()
    name = re.sub(r"[^a-z0-9]+", " ", str(filename or "").lower())
    derived_name = bool(re.search(r"\b(entry preparation|preparation worksheet|entry worksheet|working copy|draft declaration|filing worksheet|worksheet)\b", name))
    if role in _DERIVED_ROLES or derived_name:
        return SourceClass("DERIVED_PREPARATION", "Derived worksheet",
            "Working/preparation record; trace each claimed value to independent original evidence.", bool(derived_name and role not in _DERIVED_ROLES))
    commercial_name = bool(re.search(r"\b(commercial invoice|packing list|bill of lading)\b", name))
    supplier_name = bool(re.search(
        r"\b(supplier|botanical|harvest|species)\b.*\b(declaration|certificate)\b",
        name,
    ))
    # An explicit role and a strongly contradictory filename are not proof of
    # authority; require original-document inspection rather than guessing.
    if (role in _COMMERCIAL_ROLES and supplier_name) or (
        role in _SUPPLIER_ROLES and commercial_name
    ):
        return SourceClass("ROLE_CONFLICT", "Document type needs review",
            "Assigned document role conflicts with the filename; inspect the original.", True)
    if role in _COMMERCIAL_ROLES:
        return SourceClass("COMMERCIAL_SOURCE", "Commercial source",
            "Original commercial document category. Verify that this page actually supports the field.", False)
    if role in _SUPPLIER_ROLES:
        return SourceClass("SUPPLIER_SOURCE", "Supplier evidence",
            "Supplier-origin information; confirm authorship, validity and the product/line scope.", False)
    # Filenames only suggest a category. Inferred classes do not authorize
    # acceptance, bulk confirmation, or regulatory support.
    if supplier_name:
        return SourceClass("UNVERIFIED_SUPPLIER", "Likely supplier evidence",
            "Suggested by filename only; original provenance and declared role need verification.", True)
    if commercial_name:
        return SourceClass("UNVERIFIED_COMMERCIAL", "Likely commercial source",
            "Classification suggested by filename; document provenance is not yet verified.", True)
    return SourceClass("UNCLASSIFIED", "Source type unverified",
        "Inspect the document origin and decide whether it supports the field.", False)
