from __future__ import annotations
from .domain import DocumentType

_AUTHORITY = {
    "bill_of_lading": {DocumentType.BILL_OF_LADING: 30, DocumentType.CUSTOMS_ENTRY_SUMMARY: 20, DocumentType.PACKING_LIST: 10},
    "container_number": {DocumentType.BILL_OF_LADING: 25, DocumentType.PACKING_LIST: 25, DocumentType.COMMERCIAL_INVOICE: 10},
    "species": {DocumentType.SPECIES_DECLARATION: 30, DocumentType.SUPPLIER_DECLARATION: 25, DocumentType.BILL_OF_LADING: 20, DocumentType.COMMERCIAL_INVOICE: 15},
    "genus": {DocumentType.SPECIES_DECLARATION: 30, DocumentType.SUPPLIER_DECLARATION: 25, DocumentType.BILL_OF_LADING: 20, DocumentType.COMMERCIAL_INVOICE: 15},
    "country_of_harvest": {DocumentType.HARVEST_DECLARATION: 30, DocumentType.SUPPLIER_DECLARATION: 25},
}


def authority(field_key: str, document_type: DocumentType) -> float:
    return float(_AUTHORITY.get(field_key, {}).get(document_type, 5))
