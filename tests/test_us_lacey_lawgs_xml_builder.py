from __future__ import annotations

import xml.etree.ElementTree as ET

from litoral_trace.us_lacey.exporters.export_snapshot import (
    LaceyExportHeader,
    LaceyExportPlantLine,
    LaceyExportSnapshot,
)
from litoral_trace.us_lacey.exporters.lawgs_xml_builder import (
    LAWGS_MERCHANDISE_FIELD_ORDER,
    LAWGS_XML_NAMESPACE,
    build_lawgs_xml,
)


OPERATION_ID = "11111111-2222-3333-4444-555555555555"
NS = {"lawgs": LAWGS_XML_NAMESPACE}


def _snapshot(
    *,
    plant_lines: tuple[LaceyExportPlantLine, ...] | None = None,
) -> LaceyExportSnapshot:
    return LaceyExportSnapshot(
        operation_id=OPERATION_ID,
        client_reference="PO-1438",
        header=LaceyExportHeader(
            entry_number="123-4567890-1",
            importer="Wood Brokerage International",
            estimated_date_of_arrival="2026-09-20",
        ),
        plant_lines=plant_lines
        if plant_lines is not None
        else (
            LaceyExportPlantLine(
                line_reference="001",
                hts_number="9401.69.2010",
                entered_value="$2,432.00",
                article_component="Seats made with MDF",
                genus="SPECIAL",
                species="COMPOSITE",
                country_of_harvest="Malaysia",
                quantity="4.50E+2",
                unit="kg",
                percent_recycled="0",
            ),
        ),
    )


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def test_lawgs_xml_uses_merchandise_map_namespace_root_and_exact_field_order():
    xml_data = build_lawgs_xml(_snapshot())

    assert xml_data.startswith(
        b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
    )
    assert b"<LaceyActDeclaration" not in xml_data
    assert (
        b'<ns1:merchandiseList xmlns:ns1="http://lawgs.aphis.usda.gov">'
        in xml_data
    )

    root = ET.fromstring(xml_data)
    assert root.tag == f"{{{LAWGS_XML_NAMESPACE}}}merchandiseList"

    rows = root.findall("lawgs:merchandise", NS)
    assert len(rows) == 1
    row = rows[0]
    assert tuple(_local_name(child.tag) for child in row) == LAWGS_MERCHANDISE_FIELD_ORDER

    assert row.findtext("lawgs:lineNumber", namespaces=NS) == "001"
    assert row.findtext("lawgs:htsusNumber", namespaces=NS) == "9401692010"
    assert row.findtext("lawgs:enteredValue", namespaces=NS) == "2432.00"
    assert row.findtext("lawgs:articleComponent", namespaces=NS) == "Seats made with MDF"
    assert row.findtext("lawgs:genus", namespaces=NS) == "SPECIAL"
    assert row.findtext("lawgs:species", namespaces=NS) == "COMPOSITE"
    assert row.findtext("lawgs:country", namespaces=NS) == "Malaysia"
    assert row.findtext("lawgs:quantityMaterial", namespaces=NS) == "450"
    assert row.findtext("lawgs:unit", namespaces=NS) == "kg"
    assert row.findtext("lawgs:percentRecycled", namespaces=NS) == "0"


def test_lawgs_xml_is_merchandise_only_and_never_serializes_snapshot_header():
    xml_data = build_lawgs_xml(_snapshot())

    assert b"123-4567890-1" not in xml_data
    assert b"Wood Brokerage International" not in xml_data
    assert b"2026-09-20" not in xml_data
    assert b"PO-1438" not in xml_data
    assert OPERATION_ID.encode() not in xml_data

    root = ET.fromstring(xml_data)
    assert root.find(".//lawgs:merchandise", NS) is not None


def test_lawgs_xml_formats_numeric_values_without_scientific_notation():
    snapshot = _snapshot(
        plant_lines=(
            LaceyExportPlantLine(
                line_reference="7",
                hts_number="4407-99-0190",
                entered_value="1.234E+4",
                article_component="Sawn wood",
                genus="Pinus",
                species="taeda",
                country_of_harvest="Argentina",
                quantity="2.900E+0",
                unit="kg",
                percent_recycled="",
            ),
        )
    )

    xml_data = build_lawgs_xml(snapshot)
    row = ET.fromstring(xml_data).find("lawgs:merchandise", NS)
    assert row is not None
    assert row.findtext("lawgs:htsusNumber", namespaces=NS) == "4407990190"
    assert row.findtext("lawgs:enteredValue", namespaces=NS) == "12340"
    assert row.findtext("lawgs:quantityMaterial", namespaces=NS) == "2.900"
    assert row.find("lawgs:percentRecycled", NS) is None
    assert b"E+" not in xml_data
    assert b"E-" not in xml_data


def test_incomplete_snapshot_produces_well_formed_deterministic_xml_without_crashing():
    snapshot = LaceyExportSnapshot(
        operation_id=OPERATION_ID,
        client_reference="",
        header=LaceyExportHeader(
            entry_number="",
            importer="",
            estimated_date_of_arrival="",
        ),
        plant_lines=(
            LaceyExportPlantLine(
                line_reference=None,  # type: ignore[arg-type]
                hts_number=None,  # type: ignore[arg-type]
                entered_value=None,  # type: ignore[arg-type]
                article_component=None,  # type: ignore[arg-type]
                genus=None,  # type: ignore[arg-type]
                species=None,  # type: ignore[arg-type]
                country_of_harvest=None,  # type: ignore[arg-type]
                quantity=None,  # type: ignore[arg-type]
                unit=None,  # type: ignore[arg-type]
                percent_recycled=None,  # type: ignore[arg-type]
            ),
        ),
    )

    xml_data = build_lawgs_xml(snapshot)
    root = ET.fromstring(xml_data)
    row = root.find("lawgs:merchandise", NS)
    assert row is not None

    # Required map elements remain in XSD sequence as empty elements. LAWGS may
    # reject the business row as incomplete, but the exporter itself must never
    # corrupt or truncate the XML document.
    assert tuple(_local_name(child.tag) for child in row) == LAWGS_MERCHANDISE_FIELD_ORDER[:-1]
    assert all((child.text or "") == "" for child in row)
    assert row.find("lawgs:percentRecycled", NS) is None


def test_empty_snapshot_still_produces_parseable_merchandise_list():
    xml_data = build_lawgs_xml(_snapshot(plant_lines=()))

    root = ET.fromstring(xml_data)
    assert root.tag == f"{{{LAWGS_XML_NAMESPACE}}}merchandiseList"
    assert root.findall("lawgs:merchandise", NS) == []


def test_xml_escaping_preserves_article_text_after_parse():
    snapshot = _snapshot(
        plant_lines=(
            LaceyExportPlantLine(
                line_reference="1",
                hts_number="9401692010",
                entered_value="100.00",
                article_component='Oak & maple <seat> "A"',
                genus="Quercus",
                species="alba",
                country_of_harvest="United States",
                quantity="12",
                unit="kg",
            ),
        )
    )

    row = ET.fromstring(build_lawgs_xml(snapshot)).find("lawgs:merchandise", NS)
    assert row is not None
    assert (
        row.findtext("lawgs:articleComponent", namespaces=NS)
        == 'Oak & maple <seat> "A"'
    )
