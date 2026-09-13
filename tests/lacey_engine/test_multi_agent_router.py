from __future__ import annotations

import json
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from litoral_trace.lacey_engine.multi_agent.contracts import DocumentType, SpecialistRole
from litoral_trace.lacey_engine.multi_agent.router import (
    RoutingClassification,
    build_routing_plan,
    route_document,
    router_document_accuracy,
)


_PACKET_FIXTURE = Path(__file__).parents[1] / "fixtures" / "lacey_router_packet_7_docs.json"


def _packet() -> dict[str, object]:
    return json.loads(_PACKET_FIXTURE.read_text(encoding="utf-8"))


def test_router_document_accuracy_on_generated_seven_pdf_packet():
    packet = _packet()
    routed_documents = []
    expected: dict[tuple[UUID, int], DocumentType] = {}

    for item in packet["documents"]:
        filename = item["filename"]
        document_id = uuid5(NAMESPACE_URL, filename)
        expected_type = DocumentType(item["expected_type"])
        page_texts = {int(page): text for page, text in item["pages"].items()}

        routed = route_document(
            document_id=document_id,
            filename=filename,
            page_texts=page_texts,
        )

        assert len(routed) == 1
        assert routed[0].document_type is expected_type
        assert routed[0].confidence >= 0.88
        assert routed[0].signals
        routed_documents.extend(routed)
        for page in page_texts:
            expected[(document_id, page)] = expected_type

    assert router_document_accuracy(expected, routed_documents) == 1.0


def test_routing_plan_fans_documents_out_to_expected_specialists():
    expectations = {
        DocumentType.COMMERCIAL_INVOICE: (
            SpecialistRole.COMMERCIAL_LINES,
            SpecialistRole.CUSTOMS_IDENTITY,
        ),
        DocumentType.ENTRY_WORKSHEET: (
            SpecialistRole.CUSTOMS_IDENTITY,
            SpecialistRole.COMMERCIAL_LINES,
        ),
        DocumentType.BILL_OF_LADING: (
            SpecialistRole.LOGISTICS,
            SpecialistRole.CUSTOMS_IDENTITY,
        ),
        DocumentType.BOTANICAL_DECLARATION: (SpecialistRole.BOTANICAL,),
        DocumentType.SUPPLIER_ORIGIN: (SpecialistRole.BOTANICAL,),
        DocumentType.PACKING_LIST: (SpecialistRole.COMMERCIAL_LINES,),
        DocumentType.ARRIVAL_NOTICE: (SpecialistRole.LOGISTICS,),
    }

    for document_type, specialists in expectations.items():
        routed = route_document(
            document_id=uuid5(NAMESPACE_URL, document_type.value),
            filename=f"{document_type.value}.pdf",
            page_texts={1: _minimal_text(document_type)},
        )[0]
        plan = build_routing_plan((routed,))
        assert plan.specialists_for(routed) == specialists


def test_ambiguous_classifier_is_called_only_below_deterministic_threshold():
    class FakeClassifier:
        def __init__(self) -> None:
            self.calls = 0

        def classify(self, **kwargs) -> RoutingClassification:
            self.calls += 1
            return RoutingClassification(
                DocumentType.SUPPLIER_ORIGIN,
                0.93,
                ("bounded_fallback",),
            )

    fallback = FakeClassifier()
    ambiguous = route_document(
        document_id=uuid5(NAMESPACE_URL, "ambiguous"),
        filename="evidence.pdf",
        page_texts={1: "Material provenance evidence for shipment LT-TEST."},
        ambiguous_classifier=fallback,
    )

    assert fallback.calls == 1
    assert ambiguous[0].document_type is DocumentType.SUPPLIER_ORIGIN
    assert ambiguous[0].signals == ("llm:bounded_fallback",)

    obvious = route_document(
        document_id=uuid5(NAMESPACE_URL, "obvious"),
        filename="invoice.pdf",
        page_texts={1: "COMMERCIAL INVOICE\nInvoice No. INV-1\nCommercial Line Items\nEntered Value"},
        ambiguous_classifier=fallback,
    )

    assert obvious[0].document_type is DocumentType.COMMERCIAL_INVOICE
    assert fallback.calls == 1


def test_router_splits_mixed_document_pdf_by_page_type():
    routed = route_document(
        document_id=uuid5(NAMESPACE_URL, "mixed-pdf"),
        filename="packet.pdf",
        page_texts={
            1: "COMMERCIAL INVOICE\nInvoice No. INV-1\nCommercial Line Items\nEntered Value",
            2: "PACKING LIST\nPackage Detail\nCartons Pieces\nNet Wt. Gross Wt.",
        },
    )

    assert [(item.document_type, item.pages) for item in routed] == [
        (DocumentType.COMMERCIAL_INVOICE, (1,)),
        (DocumentType.PACKING_LIST, (2,)),
    ]


def _minimal_text(document_type: DocumentType) -> str:
    return {
        DocumentType.COMMERCIAL_INVOICE: "COMMERCIAL INVOICE\nInvoice No. INV-1\nCommercial Line Items\nEntered Value",
        DocumentType.ENTRY_WORKSHEET: "U.S. ENTRY WORKSHEET\nEntry / Filing Reference X\nEntry Summary Lines\nImporter Number 1",
        DocumentType.BILL_OF_LADING: "OCEAN BILL OF LADING\nShipper Consignee\nPort of Loading X\nPort of Discharge Y",
        DocumentType.BOTANICAL_DECLARATION: "BOTANICAL / LACEY SUPPORTING DECLARATION\nGenus Species\nCountry of Harvest\nPlant Quantity",
        DocumentType.SUPPLIER_ORIGIN: "SUPPLIER MATERIAL ORIGIN STATEMENT\nStatement of Material Origin\nScientific Name\nHarvest Country",
        DocumentType.PACKING_LIST: "PACKING LIST\nPackage Detail\nCartons Pieces\nNet Wt. Gross Wt.",
        DocumentType.ARRIVAL_NOTICE: "ARRIVAL NOTICE\nCarrier Reference X\nETA tomorrow\nAvailability Subject to customs release",
        DocumentType.UNKNOWN: "",
    }[document_type]
