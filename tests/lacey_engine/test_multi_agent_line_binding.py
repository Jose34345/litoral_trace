from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5, uuid4

from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    SpecialistRole,
)
from litoral_trace.lacey_engine.multi_agent.line_binding import (
    RowLocator,
    bind_line_item,
    bind_line_items,
    derive_line_item_key,
    line_item_binding_accuracy,
    source_locator_key,
)


def _envelope(
    field_key: str,
    source_text: str,
    *,
    value: str = "fixture",
    document_type: DocumentType = DocumentType.COMMERCIAL_INVOICE,
) -> CandidateEnvelope:
    candidate = AICandidate(
        field_key=field_key,
        value=value,
        normalized_value=value,
        evidence_class=EvidenceClass.EXPLICIT,
        page=1,
        source_text=source_text,
        confidence=0.9,
        provider="fixture",
        model="fixture",
    )
    return CandidateEnvelope(
        candidate=candidate,
        document_id=uuid5(NAMESPACE_URL, document_type.value),
        document_type=document_type,
        specialist=(
            SpecialistRole.BOTANICAL
            if document_type in {DocumentType.BOTANICAL_DECLARATION, DocumentType.SUPPLIER_ORIGIN}
            else SpecialistRole.COMMERCIAL_LINES
        ),
        agent_run_id=uuid4(),
        line_item_key=None,
        source_span_id=None,
    )


def test_sku_binding_has_priority_and_aligns_commercial_and_botanical_rows():
    commercial = _envelope(
        "hts_code",
        "1 ACT-TRAY-18 Acacia solid-wood serving trays 4419.90.9000 420 18,900.00",
        value="4419.90.9000",
    )
    botanical = _envelope(
        "species",
        "ACT-TRAY-18 Serving tray / solid wood Acacia mangium Vietnam 315 KG",
        value="mangium",
        document_type=DocumentType.BOTANICAL_DECLARATION,
    )

    assert derive_line_item_key(commercial) == "SKU:ACT-TRAY-18"
    assert derive_line_item_key(botanical) == "SKU:ACT-TRAY-18"


def test_explicit_sku_label_beats_line_number():
    envelope = _envelope(
        "entered_value",
        "Line 7 SKU: TEK-SRV-04 Entered Value 11,200.00",
        value="11,200.00",
    )

    assert derive_line_item_key(envelope) == "SKU:TEK-SRV-04"


def test_line_number_is_second_priority_when_no_sku_is_available():
    envelope = _envelope(
        "description",
        "Line 12 commercial description polished serving tray",
        value="polished serving tray",
    )

    assert derive_line_item_key(envelope) == "LINE:12"


def test_engine2_row_locator_precedes_fingerprint():
    envelope = _envelope(
        "description",
        "Acacia solid wood serving tray food service grade",
        value="Acacia solid wood serving tray",
    )
    locator = RowLocator(table_id="commercial-lines", page=1, row_index=3)

    key = derive_line_item_key(envelope, row_locator=locator)

    assert key.startswith("ROW:")
    assert key.endswith(":P1:TCOMMERCIAL-LINES:R3")


def test_deterministic_fingerprint_is_last_resort_for_rich_row_evidence():
    source = "Acacia solid wood serving tray 4419 90 9000 420 pieces 18900 USD"
    first = _envelope("hts_code", source, value="4419.90.9000")
    second = _envelope("entered_value", source, value="18,900.00")

    assert derive_line_item_key(first).startswith("FP:")
    assert derive_line_item_key(first) == derive_line_item_key(second)


def test_sparse_or_global_evidence_remains_explicitly_unbound():
    sparse = _envelope("species", "Acacia", value="Acacia", document_type=DocumentType.BOTANICAL_DECLARATION)
    global_field = _envelope(
        "description",
        "Importer Northstar Kitchen Imports LLC",
        value="Importer Northstar Kitchen Imports LLC",
    )
    # Change the field to a genuinely global key without changing AICandidate itself.
    global_candidate = AICandidate(
        field_key="importer_name",
        value="Northstar Kitchen Imports LLC",
        normalized_value="Northstar Kitchen Imports LLC",
        evidence_class=EvidenceClass.EXPLICIT,
        page=1,
        source_text="Northstar Kitchen Imports LLC",
        confidence=0.9,
        provider="fixture",
        model="fixture",
    )
    global_envelope = CandidateEnvelope(
        candidate=global_candidate,
        document_id=global_field.document_id,
        document_type=DocumentType.ENTRY_WORKSHEET,
        specialist=SpecialistRole.CUSTOMS_IDENTITY,
        agent_run_id=uuid4(),
        line_item_key=None,
        source_span_id=None,
    )

    assert derive_line_item_key(sparse) is None
    assert derive_line_item_key(global_envelope) is None


def test_bind_line_items_returns_new_frozen_envelopes_and_metric():
    first = _envelope(
        "hts_code",
        "1 ACT-TRAY-18 Acacia tray 4419.90.9000 420 18,900.00",
        value="4419.90.9000",
    )
    second = _envelope(
        "description",
        "Line 2 rubberwood kitchen cutting board",
        value="rubberwood kitchen cutting board",
    )
    third = _envelope(
        "entered_value",
        "Teak salad server commercial row without explicit locator 11200 USD",
        value="11200",
    )
    row_locators = {
        source_locator_key(third): RowLocator(table_id="invoice", page=1, row_index=4)
    }

    bound = bind_line_items((first, second, third), row_locators=row_locators)

    assert first.line_item_key is None
    assert [item.line_item_key for item in bound] == [
        "SKU:ACT-TRAY-18",
        "LINE:2",
        next(item.line_item_key for item in bound if item is bound[2]),
    ]
    assert bound[2].line_item_key is not None and bound[2].line_item_key.startswith("ROW:")
    assert line_item_binding_accuracy(
        ("SKU:ACT-TRAY-18", "LINE:2", bound[2].line_item_key),
        bound,
    ) == 1.0


def test_bind_line_item_never_overwrites_with_an_invented_value():
    envelope = _envelope("species", "Acacia", value="Acacia", document_type=DocumentType.BOTANICAL_DECLARATION)
    bound = bind_line_item(envelope)
    assert bound.line_item_key is None
