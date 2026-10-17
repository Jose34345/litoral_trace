"""Date punctuation recovery must be exact, deterministic and never infer dates."""
import pytest

from litoral_trace.us_lacey.ppq505 import validate_ppq_value, PpqValidationStatus


@pytest.mark.parametrize("raw,expected", [
    ("2026 11 29", "2026-11-29"),
    ("2026 12 08", "2026-12-08"),
    ("2026-11-29", "2026-11-29"),
    ("11/29/2026", "2026-11-29"),
])
def test_exact_arrival_date_from_pdf_source_span(raw, expected):
    result = validate_ppq_value("estimated_arrival_date", raw)
    assert result.status is PpqValidationStatus.VALID
    assert result.normalized_value == expected
    assert raw != ""


@pytest.mark.parametrize("raw", [
    "2026 02 29", "2026 13 01", "2026 11",
    "2026 11 29 2027", "Nov 29 2026", "2026 11 XX",
])
def test_arrival_date_never_guesses_ambiguous_or_impossible_values(raw):
    result = validate_ppq_value("estimated_arrival_date", raw)
    assert result.status is PpqValidationStatus.INVALID
