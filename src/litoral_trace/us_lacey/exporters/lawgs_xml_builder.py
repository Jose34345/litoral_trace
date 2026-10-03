"""Pure serializer for LAWGS Merchandise XML uploads.

LAWGS' batch XML option imports merchandise rows into an already-created
Plant and Plant Product Declaration. Shipment/header data belongs to the LAWGS
declaration workflow itself and is intentionally not serialized here.

The compatibility profile used here is:
    namespace: http://lawgs.aphis.usda.gov
    root:      merchandiseList
    row:       merchandise

The serializer performs formatting only. It does not query persistence, decide
regulatory applicability, or manufacture missing declaration facts.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re
import xml.etree.ElementTree as ET

from litoral_trace.us_lacey.exporters.export_snapshot import LaceyExportSnapshot


LAWGS_XML_NAMESPACE = "http://lawgs.aphis.usda.gov"
LAWGS_XML_PROFILE = "lawgs-merchandise-xml-v1"

# The Excel XML Map is sequence-sensitive.
LAWGS_MERCHANDISE_FIELD_ORDER = (
    "lineNumber",
    "htsusNumber",
    "enteredValue",
    "articleComponent",
    "genus",
    "species",
    "country",
    "quantityMaterial",
    "unit",
    "percentRecycled",
)

_REQUIRED_ROW_FIELDS = frozenset(
    {
        "lineNumber",
        "htsusNumber",
        "enteredValue",
        "articleComponent",
        "genus",
        "species",
        "country",
        "quantityMaterial",
        "unit",
    }
)

_CURRENCY_DECORATION = re.compile(r"[,$\s]")


def _xml_name(local_name: str) -> str:
    # xml.etree reserves prefixes matching ns\d+ for its namespace registry.
    # The observed LAWGS Excel XML Map uses ns1, so construct that lexical
    # prefix explicitly and declare it once on the document root.
    return f"ns1:{local_name}"


def _clean_text(value: object | None) -> str:
    return str(value or "").strip()


def _plain_decimal(value: object | None, *, currency: bool = False) -> str:
    """Return fixed-point decimal text without inventing a value."""

    raw = _clean_text(value)
    if not raw:
        return ""

    candidate = (
        _CURRENCY_DECORATION.sub("", raw)
        if currency
        else raw.replace(",", "").strip()
    )
    try:
        decimal_value = Decimal(candidate)
    except (InvalidOperation, ValueError):
        return raw

    if not decimal_value.is_finite():
        return raw

    # Removes exponent notation while preserving meaningful source scale.
    return format(decimal_value, "f")


def _htsus(value: object | None) -> str:
    """Remove presentation separators only when the result is numeric."""

    raw = _clean_text(value)
    if not raw:
        return ""

    digits = re.sub(r"[.\-\s]", "", raw)
    if digits.isdigit():
        return digits
    return raw


def _append_field(
    parent: ET.Element,
    tag: str,
    value: object | None,
    *,
    omit_if_blank: bool = False,
) -> None:
    text = _clean_text(value)
    if omit_if_blank and not text:
        return

    element = ET.SubElement(parent, _xml_name(tag))
    if text:
        element.text = text


def _serialize_merchandise_row(parent: ET.Element, line: object) -> None:
    row = ET.SubElement(parent, _xml_name("merchandise"))
    values = {
        "lineNumber": _clean_text(getattr(line, "line_reference", "")),
        "htsusNumber": _htsus(getattr(line, "hts_number", "")),
        "enteredValue": _plain_decimal(
            getattr(line, "entered_value", ""),
            currency=True,
        ),
        "articleComponent": _clean_text(getattr(line, "article_component", "")),
        "genus": _clean_text(getattr(line, "genus", "")),
        "species": _clean_text(getattr(line, "species", "")),
        "country": _clean_text(getattr(line, "country_of_harvest", "")),
        "quantityMaterial": _plain_decimal(getattr(line, "quantity", "")),
        "unit": _clean_text(getattr(line, "unit", "")),
        "percentRecycled": _plain_decimal(
            getattr(line, "percent_recycled", ""),
        ),
    }

    for tag in LAWGS_MERCHANDISE_FIELD_ORDER:
        _append_field(
            row,
            tag,
            values[tag],
            # Percent recycled is optional in LAWGS. Required merchandise
            # fields remain present as empty elements when upstream data is
            # incomplete so the XML stays structurally deterministic.
            omit_if_blank=tag not in _REQUIRED_ROW_FIELDS,
        )


def build_lawgs_xml(snapshot: LaceyExportSnapshot) -> bytes:
    """Serialize an immutable snapshot as a LAWGS Merchandise XML document.

    Missing business values do not crash serialization. Required mapped elements
    remain present but empty; optional percentRecycled is omitted when absent.
    LAWGS can therefore report an incomplete row without receiving malformed XML.
    """

    root = ET.Element(
        _xml_name("merchandiseList"),
        {"xmlns:ns1": LAWGS_XML_NAMESPACE},
    )
    for line in tuple(snapshot.plant_lines or ()):
        _serialize_merchandise_row(root, line)

    body = ET.tostring(root, encoding="utf-8", xml_declaration=False)
    declaration = b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
    return declaration + body
