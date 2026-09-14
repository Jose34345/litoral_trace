from __future__ import annotations

import pytest
from fpdf import FPDF

from litoral_trace.assurance.parsers import parse_pdf
from litoral_trace.us_lacey.ppq505 import validate_ppq_value
from litoral_trace.us_lacey.projection import _is_candidate_admissible


def _vertical_bol_pdf() -> bytes:
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=10)
    for label, value in (
        ("Bill of Lading", "VSL-SAV-260913-01"),
        ("Vessel", "MV Synthetic Test"),
        ("Consignee", "Synthetic Timber LLC"),
        ("POD", "Savannah"),
        ("ETA", "2026-10-02"),
        ("Pieces", "100"),
        ("Container", "FSCU7231845"),
    ):
        pdf.cell(65, 10, label, border=1)
        pdf.cell(105, 10, value, border=1, new_x="LMARGIN", new_y="NEXT")
    return bytes(pdf.output())


def test_real_pdf_vertical_kv_preserves_bol_value_not_following_labels():
    parsed = parse_pdf(_vertical_bol_pdf())
    assert parsed.tables
    values = [row["Bill of Lading"] for table in parsed.tables for row in table.rows
              if row.get("Bill of Lading")]
    assert values == ["VSL-SAV-260913-01"]


@pytest.mark.parametrize("value", ["Bill of Lading", "Vessel", "Consignee", "POD", "ETA", "Pieces"])
def test_bol_admission_and_public_validation_reject_labels(value):
    assert not _is_candidate_admissible("bill_of_lading", value)
    assert validate_ppq_value("bill_of_lading", value).status.value != "VALID"


def test_bol_identifier_is_preserved_by_both_semantic_boundaries():
    value = "VSL-SAV-260913-01"
    assert _is_candidate_admissible("bill_of_lading", value)
    assert validate_ppq_value("bill_of_lading", value).normalized_value == value
