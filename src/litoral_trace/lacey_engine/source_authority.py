from __future__ import annotations

from .domain import DocumentType

# Source authority is field-specific.  A packing list can be excellent container
# evidence and poor harvest evidence; a supplier declaration is the inverse.  These
# scores affect ranking only and never turn an inference into a fact.
_AUTHORITY = {
    "bill_of_lading": {
        DocumentType.BILL_OF_LADING: 40,
        DocumentType.CUSTOMS_ENTRY_SUMMARY: 25,
        DocumentType.ARRIVAL_NOTICE: 20,
        DocumentType.PACKING_LIST: 10,
    },
    "container_number": {
        DocumentType.BILL_OF_LADING: 35,
        DocumentType.PACKING_LIST: 32,
        DocumentType.CUSTOMS_ENTRY_SUMMARY: 30,
        DocumentType.COMMERCIAL_INVOICE: 15,
    },
    "estimated_arrival_date": {
        DocumentType.ARRIVAL_NOTICE: 40,
        DocumentType.BILL_OF_LADING: 30,
        DocumentType.CUSTOMS_ENTRY_SUMMARY: 25,
    },
    "filing_entry_reference": {
        DocumentType.CUSTOMS_ENTRY_SUMMARY: 45,
        DocumentType.ISF: 25,
        DocumentType.BILL_OF_LADING: 10,
    },
    "manufacturer_id": {
        DocumentType.CUSTOMS_ENTRY_SUMMARY: 45,
        DocumentType.ISF: 35,
        DocumentType.COMMERCIAL_INVOICE: 20,
        DocumentType.SUPPLIER_DECLARATION: 20,
    },
    "importer_name": {
        DocumentType.CUSTOMS_ENTRY_SUMMARY: 40,
        DocumentType.BILL_OF_LADING: 35,
        DocumentType.COMMERCIAL_INVOICE: 32,
        DocumentType.PACKING_LIST: 25,
    },
    "consignee_name": {
        DocumentType.BILL_OF_LADING: 40,
        DocumentType.ARRIVAL_NOTICE: 35,
        DocumentType.COMMERCIAL_INVOICE: 30,
        DocumentType.PACKING_LIST: 25,
    },
    "importer_address": {
        DocumentType.CUSTOMS_ENTRY_SUMMARY: 40,
        DocumentType.COMMERCIAL_INVOICE: 30,
        DocumentType.BILL_OF_LADING: 25,
    },
    "consignee_address": {
        DocumentType.BILL_OF_LADING: 40,
        DocumentType.ARRIVAL_NOTICE: 35,
        DocumentType.COMMERCIAL_INVOICE: 30,
    },
    "description": {
        DocumentType.COMMERCIAL_INVOICE: 40,
        DocumentType.BILL_OF_LADING: 35,
        DocumentType.CUSTOMS_ENTRY_SUMMARY: 30,
        DocumentType.PACKING_LIST: 20,
        DocumentType.SUPPLIER_DECLARATION: 15,
    },
    "hts_code": {
        DocumentType.CUSTOMS_ENTRY_SUMMARY: 45,
        DocumentType.COMMERCIAL_INVOICE: 35,
        DocumentType.ISF: 30,
        DocumentType.BILL_OF_LADING: 15,
    },
    "entered_value": {
        DocumentType.CUSTOMS_ENTRY_SUMMARY: 45,
        DocumentType.COMMERCIAL_INVOICE: 40,
        DocumentType.PACKING_LIST: 10,
    },
    "article_component": {
        DocumentType.SUPPLIER_DECLARATION: 40,
        DocumentType.SPECIES_DECLARATION: 38,
        DocumentType.COMMERCIAL_INVOICE: 25,
        DocumentType.PACKING_LIST: 20,
    },
    "species": {
        DocumentType.SPECIES_DECLARATION: 45,
        DocumentType.HARVEST_DECLARATION: 42,
        DocumentType.SUPPLIER_DECLARATION: 40,
        DocumentType.COMMERCIAL_INVOICE: 20,
        DocumentType.BILL_OF_LADING: 15,
    },
    "genus": {
        DocumentType.SPECIES_DECLARATION: 45,
        DocumentType.HARVEST_DECLARATION: 42,
        DocumentType.SUPPLIER_DECLARATION: 40,
        DocumentType.COMMERCIAL_INVOICE: 20,
        DocumentType.BILL_OF_LADING: 15,
    },
    "country_of_harvest": {
        DocumentType.HARVEST_DECLARATION: 50,
        DocumentType.SUPPLIER_DECLARATION: 45,
        DocumentType.SPECIES_DECLARATION: 35,
    },
    "plant_quantity": {
        DocumentType.SUPPLIER_DECLARATION: 45,
        DocumentType.HARVEST_DECLARATION: 40,
        DocumentType.SPECIES_DECLARATION: 35,
    },
    "metric_unit": {
        DocumentType.SUPPLIER_DECLARATION: 45,
        DocumentType.HARVEST_DECLARATION: 40,
        DocumentType.SPECIES_DECLARATION: 35,
    },
    "percent_recycled": {
        DocumentType.SUPPLIER_DECLARATION: 45,
        DocumentType.COMMERCIAL_INVOICE: 20,
    },
}


def authority(field_key: str, document_type: DocumentType) -> float:
    return float(_AUTHORITY.get(field_key, {}).get(document_type, 5))
