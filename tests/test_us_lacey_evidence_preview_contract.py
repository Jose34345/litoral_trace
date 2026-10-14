from litoral_trace.us_lacey.regulatory.applicability.attestation import ApplicabilitySource
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_evidence_cards_distinguish_primary_supplier_derived_and_unavailable_sources() -> None:
    from litoral_trace.web.us_lacey_evidence_cards import evidence_cards

    cards = evidence_cards((
        ApplicabilitySource(1, "Arrival Notice.pdf", "Commercial source", True, True, "rev-a"),
        ApplicabilitySource(2, "Cultivation Certificate.pdf", "Supplier evidence", True, True, "rev-a"),
        ApplicabilitySource(3, "Entry worksheet.pdf", "Derived worksheet", False, False, "rev-a"),
        ApplicabilitySource(4, "Expired original.pdf", "Source type unverified", False, False, "rev-a"),
    ))

    assert cards[0].review_scope == "Customs entry evidence"
    assert cards[0].availability == "Evidence available"
    assert cards[1].review_scope == "Supplier cultivation evidence"
    assert cards[1].is_supplier_evidence is True
    assert cards[2].availability == "Derived worksheet"
    assert cards[2].is_selectable is False
    assert cards[3].availability == "Original unavailable"
    assert all(card.source_set_fingerprint == "rev-a" for card in cards)


def test_derived_worksheet_is_never_presented_as_primary_evidence() -> None:
    from litoral_trace.web.us_lacey_evidence_cards import evidence_cards

    card = evidence_cards((
        ApplicabilitySource(3, "Entry worksheet.pdf", "Derived worksheet", False, False, "rev-a"),
    ))[0]

    assert card.is_selectable is False
    assert card.review_scope == "Supporting context only"


def test_ephemeral_evidence_session_contract_has_no_persistent_pdf_cache() -> None:
    js = (ROOT / "src/litoral_trace/static/js/us_lacey_source_inspector.js").read_text()

    assert "class EvidenceSession" in js
    assert "AbortController" in js
    assert 'credentials:"same-origin"' in js
    assert 'cache:"no-store"' in js
    assert "URL.revokeObjectURL" in js
    assert "sessionStorage" not in js
    assert "localStorage" not in js
    for status in ("401", "403", "404", "409", "503"):
        assert status in js
