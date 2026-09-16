"""XML serializer for the U.S. Lacey declaration export projection.

This builder serializes the Phase E preparation dataset requested for LAWGS bulk
workflows. It does not perform a remote APHIS submission and should not be treated
as evidence that LAWGS accepted a filing.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET

from litoral_trace.us_lacey.exporters.export_snapshot import LaceyExportSnapshot


def _text(parent: ET.Element, tag: str, value: str) -> ET.Element:
    element = ET.SubElement(parent, tag)
    element.text = str(value or "")
    return element


def build_lawgs_xml(snapshot: LaceyExportSnapshot) -> bytes:
    """Return a well-formed UTF-8 Lacey Act declaration XML document."""
    root = ET.Element("LaceyActDeclaration")
    header = ET.SubElement(root, "Header")
    _text(header, "EntryNumber", snapshot.header.entry_number)
    _text(header, "Importer", snapshot.header.importer)
    _text(header, "EstimatedDateOfArrival", snapshot.header.estimated_date_of_arrival)
    _text(header, "OperationId", snapshot.operation_id)
    _text(header, "ClientReference", snapshot.client_reference)

    plant_lines = ET.SubElement(root, "PlantLines")
    for line in snapshot.plant_lines:
        plant_line = ET.SubElement(plant_lines, "PlantLine")
        _text(plant_line, "LineReference", line.line_reference)
        _text(plant_line, "HTSNumber", line.hts_number)
        _text(plant_line, "EnteredValue", line.entered_value)
        _text(plant_line, "ArticleComponent", line.article_component)
        scientific_name = ET.SubElement(plant_line, "PlantScientificName")
        _text(scientific_name, "Genus", line.genus)
        _text(scientific_name, "Species", line.species)
        _text(plant_line, "CountryOfHarvest", line.country_of_harvest)
        quantity = ET.SubElement(plant_line, "Quantity")
        _text(quantity, "Amount", line.quantity)
        _text(quantity, "Unit", line.unit)

    return ET.tostring(root, encoding="utf-8", xml_declaration=True)
