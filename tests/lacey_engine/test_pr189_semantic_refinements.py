from litoral_trace.lacey_engine.domain import (
    AdmittedCandidate,
    DocumentResolution,
    DocumentType,
    EvidenceClass,
    FieldStatus,
    LayoutBlock,
    ParsedLayout,
    Provenance,
    RawCandidate,
    ResolvedField,
)
from litoral_trace.lacey_engine.layout_parser import layout_from_matrix_rows
from litoral_trace.lacey_engine.pipeline import _extract
from litoral_trace.lacey_engine.shipment import (
    SHIPMENT_TOTAL_ENTERED_VALUE,
    ReconciliationState,
    ShipmentDocumentInput,
    ShipmentReadiness,
    process_shipment,
)


def _document(identifier, values, document_type=DocumentType.BILL_OF_LADING):
    fields = {}
    for index, (key, item) in enumerate(values.items()):
        value, label = item if isinstance(item, tuple) else (item, key)
        block = LayoutBlock(f"{identifier}-{index}", 1, None, value, "TEXT_LINE")
        raw = RawCandidate(
            key,
            value,
            value,
            block,
            EvidenceClass.EXPLICIT,
            "test",
            "1",
            label=label,
        )
        candidate = AdmittedCandidate(
            raw,
            Provenance(
                f"{identifier}.pdf",
                1,
                None,
                block.block_id,
                value,
                "test",
                "1",
                EvidenceClass.EXPLICIT,
            ),
            90,
            document_type,
        )
        fields[key] = ResolvedField(
            key,
            FieldStatus.MATCHED,
            value,
            candidate,
            (candidate,),
        )
    return DocumentResolution(
        f"{identifier}.pdf",
        "test",
        document_type,
        1,
        ParsedLayout((), 1),
        (),
        fields,
    )


def _shipment(*docs):
    return process_shipment(
        documents=[
            ShipmentDocumentInput(str(index), f"{index}.pdf", resolution=doc)
            for index, doc in enumerate(docs)
        ]
    )


def test_plant_declaration_quantity_and_unit_keep_same_row_identity():
    layout = layout_from_matrix_rows(
        [
            "Article / Component",
            "Genus",
            "Species",
            "Country of Harvest",
            "Plant Quantity",
            "Unit",
        ],
        [
            ["Frame", "Pinus", "radiata", "NEW ZEALAND", "1440", "KG"],
            ["Panel", "Eucalyptus", "grandis", "BRAZIL", "360", "KG"],
        ],
    )

    found = _extract(layout)
    quantities = {
        candidate.source_block.row_index: candidate.normalized_value
        for candidate in found["plant_quantity"]
    }
    units = {
        candidate.source_block.row_index: candidate.normalized_value.upper()
        for candidate in found["metric_unit"]
    }

    assert quantities == {1: "1440", 2: "360"}
    assert units == {1: "KG", 2: "KG"}
    assert quantities.keys() == units.keys()


def test_generic_commercial_unit_column_never_becomes_ppq_metric_unit():
    layout = layout_from_matrix_rows(
        ["SKU", "Description", "Quantity", "Unit", "Unit Price"],
        [["CHAIR-01", "Wood dining chair", "12", "EA", "1550"]],
    )

    found = _extract(layout)

    assert found["metric_unit"] == []
    assert found["plant_quantity"] == []


def test_shipment_total_entered_value_is_not_a_line_candidate_when_allocations_exist():
    result = _shipment(
        _document(
            "total",
            {
                "bill_of_lading": "MAEU1890001",
                "entered_value": ("USD 18600", "Shipment Total Entered Value"),
            },
        ),
        _document("line1", {"entered_value": ("USD 14880", "Line 1")}),
        _document("line2", {"entered_value": ("USD 3720", "Line 2")}),
    )

    assert result.canonical_fields["entered_value"].state is ReconciliationState.SUPPORTED_MULTIPLE
    assert result.canonical_fields[SHIPMENT_TOTAL_ENTERED_VALUE].state is ReconciliationState.SUPPORTED
    assert {value.value for value in result.canonical_fields["entered_value"].values} == {
        "USD 14880",
        "USD 3720",
    }
    assert not any(
        issue.issue_type == "ENTERED_VALUE_ALLOCATION_MISMATCH"
        for issue in result.issues
    )


def test_entered_value_allocation_sum_mismatch_is_blocking_inconsistency():
    result = _shipment(
        _document(
            "total",
            {
                "bill_of_lading": "MAEU1890002",
                "entered_value": ("USD 18600", "Shipment Total Entered Value"),
            },
        ),
        _document("line1", {"entered_value": ("USD 14880", "Line 1")}),
        _document("line2", {"entered_value": ("USD 3700", "Line 2")}),
    )

    issue = next(
        issue
        for issue in result.issues
        if issue.issue_type == "ENTERED_VALUE_ALLOCATION_MISMATCH"
    )
    assert issue.severity == "HIGH"
    assert result.readiness is ShipmentReadiness.BLOCKED
