from contextlib import closing
from io import BytesIO
from types import SimpleNamespace

from openpyxl import load_workbook

from litoral_trace.us_lacey import review


def test_preparation_workbook_keeps_formula_like_customer_values_as_literal_text(monkeypatch):
    operation = SimpleNamespace(client_reference="=1+1", status="COMPLETED")
    candidate = SimpleNamespace(
        id=7,
        decision="REJECTED",
        original_value='="literal evidence"',
        normalized_value="Raw evidence",
        source_assurance_document_id=3,
        decided_at=None,
    )
    monkeypatch.setattr(
        review, "_export_rows", lambda **_: (operation, (), {}, (), (candidate,))
    )

    payload = review.export_us_lacey_xlsx(organization_id=1, operation_public_id="synthetic")
    with closing(load_workbook(BytesIO(payload), read_only=True)) as workbook:
        reference = workbook["Read Me"]["B2"]
        evidence = workbook["Exceptions"]["D2"]
        assert (reference.value, reference.data_type) == ("=1+1", "s")
        assert (evidence.value, evidence.data_type) == ('="literal evidence"', "s")
        assert workbook["Exceptions"]["E2"].value == "Raw evidence"
