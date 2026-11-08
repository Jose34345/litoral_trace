from __future__ import annotations

import os
from decimal import Decimal
import xml.etree.ElementTree as ET
from uuid import uuid4

import pytest
from sqlalchemy import select

from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentExtractionRun,
    ExtractedDocumentField,
    ReconciliationIssue,
    UsLaceyPpqPlantLine,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import (
    get_us_lacey_db_session,
    reset_us_lacey_engine_state,
)
from litoral_trace.us_lacey.exporters.export_snapshot import consolidate_export_snapshot
from litoral_trace.us_lacey.exporters.lawgs_xml_builder import (
    LAWGS_XML_NAMESPACE,
    build_lawgs_xml,
)
from litoral_trace.us_lacey.operations import UsLaceyOperationService
from litoral_trace.us_lacey.projection import project_assurance_document_to_us_lacey
from litoral_trace.us_lacey.workflow import create_us_lacey_customer_operation
from litoral_trace.web.us_lacey_operational_views import _review_field_groups
from tests.test_us_lacey_worker_postgres_integration import (
    _activate_account,
    _register_active_customer,
)


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_DATABASE_URL")
    or not os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL"),
    reason="requires the isolated U.S. PostgreSQL integration database",
)


def _add_extracted_document(
    *,
    organization_id: int,
    user_id: int,
    filename: str,
    fields: tuple[tuple[str, str, str], ...],
) -> int:
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, organization_id)
        token = uuid4().hex
        vault = VaultDocument(
            organization_id=organization_id,
            created_by_user_id=user_id,
            original_filename=filename,
            content_type="application/pdf",
            size_bytes=1024,
            sha256=(token * 4)[:64],
            object_key=f"us-lacey/golden-pack2/{organization_id}/{token}",
            storage_backend="s3",
            storage_bucket="us-lacey-ci-private",
            document_type="OTHER_EVIDENCE",
            status="available",
        )
        session.add(vault)
        session.flush()

        assurance = AssuranceDocument(
            organization_id=organization_id,
            vault_document_id=vault.id,
            semantic_document_type="UNKNOWN",
            type_confidence=0.99,
            processing_status="NEEDS_REVIEW",
        )
        session.add(assurance)
        session.flush()

        run = DocumentExtractionRun(
            organization_id=organization_id,
            assurance_document_id=assurance.id,
            engine="assurance-deterministic-parser",
            engine_version="pack2-golden-v1",
            status="SUCCEEDED",
        )
        session.add(run)
        session.flush()

        for field_name, value, locator in fields:
            session.add(
                ExtractedDocumentField(
                    organization_id=organization_id,
                    assurance_document_id=assurance.id,
                    extraction_run_id=run.id,
                    field_name=field_name,
                    original_value=value,
                    normalized_value=value,
                    value_type="cell",
                    confidence=0.98,
                    confidence_level="HIGH",
                    source_page=1,
                    source_locator=locator,
                    auto_accepted=False,
                    needs_review=True,
                )
            )
        session.commit()
        return assurance.id
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


PACK2 = (
    (
        "01_Commercial_Invoice.pdf",
        "COMMERCIAL_INVOICE",
        (
            ("raw.table.1.Line", "1", "table:1;data_row:1;column:1"),
            ("raw.table.1.SKU", "CO-18", "table:1;data_row:1;column:2"),
            (
                "raw.table.1.Description",
                "Sawn cedar boards - Cedrela odorata",
                "table:1;data_row:1;column:3",
            ),
            ("raw.table.1.HTS", "4407.99.0190", "table:1;data_row:1;column:4"),
            ("raw.table.1.Qty", "18.000 m3 / 900 pcs", "table:1;data_row:1;column:5"),
            ("raw.table.1.Unit Price", "2700.00/m3", "table:1;data_row:1;column:6"),
            ("raw.table.1.Amount", "48600.00", "table:1;data_row:1;column:7"),
            ("raw.table.2.Entered Value", "48600.00", "table:2;data_row:1;column:1"),
            ("raw.table.3.Container", "TGHU5519023", "table:3;data_row:1;column:1"),
            ("raw.table.3.BOL", "ACE-MIA-260913-77", "table:3;data_row:1;column:2"),
        ),
    ),
    (
        "02_Bill_of_Lading.pdf",
        "BILL_OF_LADING",
        (
            ("raw.table.1.BOL", "ACE-MIA-260913-77", "table:1;data_row:1;column:1"),
            ("raw.table.1.Container", "TGHU5519023", "table:1;data_row:1;column:2"),
            ("raw.table.1.Consignee", "Timberline Imports LLC", "table:1;data_row:1;column:3"),
            ("raw.table.1.ETA", "2026-09-29", "table:1;data_row:1;column:4"),
        ),
    ),
    (
        "03_Packing_List.pdf",
        "PACKING_LIST",
        (
            ("raw.table.1.BOL", "ACE-MIA-260913-77", "table:1;data_row:1;column:1"),
            ("raw.table.1.Container", "TGHU5519023", "table:1;data_row:1;column:2"),
        ),
    ),
    (
        "04_Botanical_Declaration.pdf",
        "BOTANICAL_DECLARATION",
        (
            ("raw.table.1.Genus", "Cedrela", "table:1;data_row:1;column:1"),
            ("raw.table.1.Species", "odorata", "table:1;data_row:1;column:2"),
            ("raw.table.1.Country of Harvest", "Peru", "table:1;data_row:1;column:3"),
            ("raw.table.1.Plant Quantity", "18.000", "table:1;data_row:1;column:4"),
            ("raw.table.1.Unit", "m3", "table:1;data_row:1;column:5"),
        ),
    ),
    (
        "05_Supplier_Origin_Declaration.pdf",
        "SUPPLIER_DECLARATION",
        (
            ("raw.table.1.Genus", "Cedrela", "table:1;data_row:1;column:1"),
            ("raw.table.1.Species", "odorata", "table:1;data_row:1;column:2"),
            ("raw.table.1.Country of Harvest", "Brazil", "table:1;data_row:1;column:3"),
            ("raw.table.1.Plant Quantity", "18.000", "table:1;data_row:1;column:4"),
            ("raw.table.1.Unit", "m3", "table:1;data_row:1;column:5"),
        ),
    ),
    (
        "06_Entry_Worksheet.pdf",
        "CUSTOMS_ENTRY",
        (
            ("raw.table.1.Line", "1", "table:1;data_row:1;column:1"),
            ("raw.table.1.HTS", "4407.99.0190", "table:1;data_row:1;column:2"),
            (
                "raw.table.1.Description",
                "Cedrela odorata sawn boards",
                "table:1;data_row:1;column:3",
            ),
            ("raw.table.1.Entered Value", "49050.00", "table:1;data_row:1;column:4"),
        ),
    ),
    (
        "07_Arrival_Notice.pdf",
        "ARRIVAL_NOTICE",
        (
            ("raw.table.1.BOL", "ACE-MIA-260913-77", "table:1;data_row:1;column:1"),
            ("raw.table.1.Container", "TGHU5519023", "table:1;data_row:1;column:2"),
            ("raw.table.1.Consignee", "Timberline Imports LLC", "table:1;data_row:1;column:3"),
            ("raw.table.1.ETA", "2026-10-01", "table:1;data_row:1;column:4"),
        ),
    ),
)


def test_golden_fixture_pack_2_projects_exact_three_intentional_conflicts():
    reset_us_lacey_engine_state()
    registered, _email, suffix = _register_active_customer()
    _activate_account(registered.organization_id)

    operation = create_us_lacey_customer_operation(
        organization_id=registered.organization_id,
        user_id=registered.user_id,
        client_reference=f"GOLDEN-PACK2-{suffix}",
        line_references=(),
    )
    service = UsLaceyOperationService()
    operation_id = service.get_internal_id(
        organization_id=registered.organization_id,
        operation_public_id=operation.public_id,
    )

    for filename, role, fields in PACK2:
        document_id = _add_extracted_document(
            organization_id=registered.organization_id,
            user_id=registered.user_id,
            filename=filename,
            fields=fields,
        )
        service.attach_document(
            organization_id=registered.organization_id,
            operation_public_id=operation.public_id,
            assurance_document_id=document_id,
            document_role=role,
        )
        project_assurance_document_to_us_lacey(
            organization_id=registered.organization_id,
            operation_id=operation_id,
            assurance_document_id=document_id,
        )

    detail = service.get_detail(
        organization_id=registered.organization_id,
        operation_public_id=operation.public_id,
    )

    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, registered.organization_id)
        plant_lines = session.scalars(
            select(UsLaceyPpqPlantLine).where(
                UsLaceyPpqPlantLine.organization_id == registered.organization_id,
                UsLaceyPpqPlantLine.operation_id == operation_id,
            )
        ).all()
        assert [line.line_reference for line in plant_lines] == ["1"]

        open_issues = session.scalars(
            select(ReconciliationIssue)
            .where(
                ReconciliationIssue.organization_id == registered.organization_id,
                ReconciliationIssue.operation_reference == f"us_lacey:{operation.public_id}",
                ReconciliationIssue.status == "OPEN",
            )
            .order_by(ReconciliationIssue.id)
        ).all()
        assert len(open_issues) == 3
        issue_values = {
            issue.field_name: {str(issue.left_value), str(issue.right_value)}
            for issue in open_issues
        }
    finally:
        session.rollback()
        session.close()
    assert issue_values["estimated_arrival_date"] == {
        "2026-09-29",
        "2026-10-01",
    }
    assert {
        Decimal(value)
        for value in issue_values["entered_value_reconciliation"]
    } == {
        Decimal("48600.00"),
        Decimal("49050.00"),
    }
    assert issue_values["country_of_harvest"] == {"Peru", "Brazil"}

    conflict_fields = {
        field.field_name: field
        for field in detail.fields
        if field.status == "CONFLICT"
    }
    assert set(conflict_fields) == {
        "estimated_arrival_date",
        "entered_value",
        "country_of_harvest",
    }

    assert {
        candidate.normalized_value or candidate.original_value
        for candidate in conflict_fields["estimated_arrival_date"].candidates
    } == {"2026-09-29", "2026-10-01"}
    assert {
        Decimal(candidate.normalized_value or candidate.original_value)
        for candidate in conflict_fields["entered_value"].candidates
    } == {Decimal("48600.00"), Decimal("49050.00")}
    assert {
        candidate.normalized_value or candidate.original_value
        for candidate in conflict_fields["country_of_harvest"].candidates
    } == {"Peru", "Brazil"}

    plant_field_names = {
        field.field_name
        for field in detail.fields
        if field.line_reference == "1"
    }
    assert {"genus", "species", "country_of_harvest"}.issubset(plant_field_names)

    attention_fields, _auto_supported, _settled = _review_field_groups(detail)
    attention_conflicts = {
        field.field_name
        for field in attention_fields
        if field.status == "CONFLICT"
    }
    assert attention_conflicts == {
        "estimated_arrival_date",
        "entered_value",
        "country_of_harvest",
    }

    reset_us_lacey_engine_state()



def _golden_full_cycle_fields(
    table: int,
    rows: tuple[tuple[str, ...], ...],
    headers: tuple[str, ...],
) -> tuple[tuple[str, str, str], ...]:
    fields: list[tuple[str, str, str]] = []
    for row_index, values in enumerate(rows, start=1):
        assert len(values) == len(headers)
        for column_index, (header, value) in enumerate(zip(headers, values), start=1):
            fields.append(
                (
                    f"raw.table.{table}.{header}",
                    value,
                    f"table:{table};data_row:{row_index};column:{column_index}",
                )
            )
    return tuple(fields)


_GOLDEN_FULL_CYCLE_LINES = (
    (
        "1",
        "ACT-TRAY-18",
        "4419.90.9000",
        "Serving tray / solid wood",
        "Acacia",
        "mangium",
        "Vietnam",
        "315",
        "KG",
        "18900.00",
    ),
    (
        "2",
        "RUB-CB-32",
        "4419.90.8000",
        "Cutting board / solid wood",
        "Hevea",
        "brasiliensis",
        "Vietnam",
        "510",
        "KG",
        "17760.00",
    ),
    (
        "3",
        "TEK-SRV-04",
        "4421.99.9880",
        "Salad servers / solid wood",
        "Tectona",
        "grandis",
        "Vietnam",
        "145",
        "KG",
        "11200.00",
    ),
)


GOLDEN_FULL_CYCLE = (
    (
        "01_Commercial_Invoice.pdf",
        "COMMERCIAL_INVOICE",
        (
            *_golden_full_cycle_fields(
                1,
                (
                    (
                        "Northstar Kitchen Imports LLC",
                        "104 Harbor Commerce Blvd., Savannah, GA 31401, United States",
                        "VNMKHOM123HCM",
                    ),
                ),
                ("Importer Name", "Importer Address", "Manufacturer ID"),
            ),
            *_golden_full_cycle_fields(
                2,
                tuple(
                    (line, sku, article, hts, value)
                    for line, sku, hts, article, _genus, _species, _country, _qty, _unit, value
                    in _GOLDEN_FULL_CYCLE_LINES
                ),
                ("Line", "SKU", "Description", "HTSUS", "Amount"),
            ),
        ),
    ),
    (
        "02_Ocean_Bill_of_Lading.pdf",
        "BILL_OF_LADING",
        _golden_full_cycle_fields(
            1,
            (
                (
                    "MAEU2609240001",
                    "MSCU1234566",
                    "Atlantic Home Goods Distribution Inc.",
                    "825 Portside Logistics Pkwy., Pooler, GA 31322, United States",
                    "WOODEN KITCHENWARE AND HOUSEHOLD ARTICLES; Acacia serving trays; rubberwood cutting boards; teak salad-server pairs.",
                ),
            ),
            ("BOL", "Container", "Consignee", "Consignee Address", "Cargo Description"),
        ),
    ),
    (
        "03_US_Entry_Worksheet.pdf",
        "CUSTOMS_ENTRY",
        (
            *_golden_full_cycle_fields(
                1,
                (
                    (
                        "123-4567890-1",
                        "10/03/2026",
                        "MAEU2609240001",
                        "MSCU1234566",
                        "VNMKHOM123HCM",
                        "Northstar Kitchen Imports LLC",
                        "104 Harbor Commerce Blvd., Savannah, GA 31401, United States",
                        "Atlantic Home Goods Distribution Inc.",
                        "825 Portside Logistics Pkwy., Pooler, GA 31322, United States",
                    ),
                ),
                (
                    "Entry Reference",
                    "Estimated Arrival Date",
                    "BOL",
                    "Container",
                    "Manufacturer ID",
                    "Importer Name",
                    "Importer Address",
                    "Consignee",
                    "Consignee Address",
                ),
            ),
            *_golden_full_cycle_fields(
                2,
                tuple(
                    (line, hts, article, value)
                    for line, _sku, hts, article, _genus, _species, _country, _qty, _unit, value
                    in _GOLDEN_FULL_CYCLE_LINES
                ),
                ("Line", "HTSUS", "Description", "Entered Value"),
            ),
        ),
    ),
    (
        "04_Botanical_Lacey_Supporting_Declaration.pdf",
        "BOTANICAL_DECLARATION",
        _golden_full_cycle_fields(
            1,
            tuple(
                (article, genus, species, country, qty, unit, "0")
                for _line, _sku, _hts, article, genus, species, country, qty, unit, _value
                in _GOLDEN_FULL_CYCLE_LINES
            ),
            (
                "Article Component",
                "Genus",
                "Species",
                "Country of Harvest",
                "Plant Quantity",
                "Unit",
                "Percent Recycled",
            ),
        ),
    ),
    (
        "05_Packing_List.pdf",
        "PACKING_LIST",
        _golden_full_cycle_fields(
            1,
            (
                ("1", "ACTTRAY18", "Acacia solid-wood serving trays"),
                ("PAL", "PAL", "Heat-treated export pallet"),
                ("2", "RUBCB32", "Rubberwood kitchen cutting boards"),
                ("AUX", "AUX-02", "Corner protectors / dunnage"),
                ("3", "TEKSRV04", "Teak salad-server pairs"),
                ("CART", "BOX", "Carton / box packaging only"),
            ),
            ("Row", "HTS", "Description"),
        ),
    ),
    (
        "06_Supplier_Material_Origin_Statement.pdf",
        "SUPPLIER_DECLARATION",
        _golden_full_cycle_fields(
            1,
            tuple(
                (sku, article, genus, species, country)
                for _line, sku, _hts, article, genus, species, country, _qty, _unit, _value
                in _GOLDEN_FULL_CYCLE_LINES
            ),
            ("SKU", "Article Component", "Genus", "Species", "Country of Harvest"),
        ),
    ),
    (
        "07_Arrival_Notice.pdf",
        "ARRIVAL_NOTICE",
        _golden_full_cycle_fields(
            1,
            (
                (
                    "10/03/2026",
                    "MAEU2609240001",
                    "MSCU1234566",
                    "123-4567890-1",
                    "Atlantic Home Goods Distribution Inc.",
                    "825 Portside Logistics Pkwy., Pooler, GA 31322, United States",
                    "Northstar Kitchen Imports LLC",
                    "104 Harbor Commerce Blvd., Savannah, GA 31401, United States",
                ),
            ),
            (
                "ETA",
                "BOL",
                "Container",
                "Entry Reference",
                "Consignee",
                "Consignee Address",
                "Importer Name",
                "Importer Address",
            ),
        ),
    ),
    (
        "08_Product_Composition_BOM.pdf",
        "SUPPLIER_DECLARATION",
        _golden_full_cycle_fields(
            1,
            (
                ("ACT-TRAY-18", "Acacia serving tray", "Tray body", "Acacia mangium", "1", "0.750", "KG"),
                ("RUB-CB-32", "Rubberwood cutting board", "Board body", "Hevea brasiliensis", "1", "0.850", "KG"),
                ("TEK-SRV-04", "Teak salad-server pair", "Serving spoon", "Tectona grandis", "1", "0.290", "KG"),
                ("TEK-SRV-04", "Teak salad-server pair", "Serving fork", "Tectona grandis", "1", "0.290", "KG"),
            ),
            ("SKU", "Product Name", "Component", "Material", "Quantity", "Weight", "Unit"),
        ),
    ),
    (
        "09_Lacey_Act_Plant_Data_Worksheet.pdf",
        "BOTANICAL_DECLARATION",
        (
            *_golden_full_cycle_fields(
                1,
                tuple(
                    (line, hts, article, genus, species, country, qty, unit, value, "0")
                    for line, _sku, hts, article, genus, species, country, qty, unit, value
                    in _GOLDEN_FULL_CYCLE_LINES
                ),
                (
                    "Line",
                    "HTSUS",
                    "Article Component",
                    "Genus",
                    "Species",
                    "Country of Harvest",
                    "Plant Quantity",
                    "Unit",
                    "Entered Value",
                    "Percent Recycled",
                ),
            ),
            *_golden_full_cycle_fields(
                2,
                (
                    (
                        "Northstar Kitchen Imports LLC",
                        "104 Harbor Commerce Blvd., Savannah, GA 31401, United States",
                        "VNMKHOM123HCM",
                        "123-4567890-1",
                        "MAEU2609240001",
                        "MSCU1234566",
                        "10/03/2026",
                        "WOODEN KITCHENWARE AND HOUSEHOLD ARTICLES; Acacia serving trays; rubberwood cutting boards; teak salad-server pairs.",
                    ),
                ),
                (
                    "Importer Name",
                    "Importer Address",
                    "Manufacturer ID",
                    "Entry Reference",
                    "BOL",
                    "Container",
                    "Estimated Arrival Date",
                    "Merchandise Description",
                ),
            ),
        ),
    ),
)


def test_golden_full_cycle_packet_has_under_four_exceptions_and_valid_lawgs_xml():
    """Regression gate for the 9-document packet that previously produced 34 false exceptions."""

    reset_us_lacey_engine_state()
    registered, _email, suffix = _register_active_customer()
    _activate_account(registered.organization_id)

    operation = create_us_lacey_customer_operation(
        organization_id=registered.organization_id,
        user_id=registered.user_id,
        client_reference=f"GOLDEN-FULL-CYCLE-{suffix}",
        line_references=("1", "2", "3"),
    )
    service = UsLaceyOperationService()
    operation_id = service.get_internal_id(
        organization_id=registered.organization_id,
        operation_public_id=operation.public_id,
    )

    for filename, role, fields in GOLDEN_FULL_CYCLE:
        document_id = _add_extracted_document(
            organization_id=registered.organization_id,
            user_id=registered.user_id,
            filename=filename,
            fields=fields,
        )
        service.attach_document(
            organization_id=registered.organization_id,
            operation_public_id=operation.public_id,
            assurance_document_id=document_id,
            document_role=role,
        )
        project_assurance_document_to_us_lacey(
            organization_id=registered.organization_id,
            operation_id=operation_id,
            assurance_document_id=document_id,
        )

    detail = service.get_detail(
        organization_id=registered.organization_id,
        operation_public_id=operation.public_id,
    )
    attention_fields, auto_supported, settled = _review_field_groups(detail)

    assert len(attention_fields) < 4, [
        (field.line_reference, field.field_name, field.status, field.proposed_value)
        for field in attention_fields
    ]
    assert auto_supported or settled

    by_key = {
        (field.line_reference, field.field_name): field
        for field in detail.fields
    }
    expected = {
        "1": ("4419909000", "315", "kg"),
        "2": ("4419908000", "510", "kg"),
        "3": ("4421999880", "145", "kg"),
    }
    for line_ref, (hts, qty, unit) in expected.items():
        hts_field = by_key[(line_ref, "hts_code")]
        qty_field = by_key[(line_ref, "plant_quantity")]
        unit_field = by_key[(line_ref, "metric_unit")]
        assert (hts_field.effective_value or hts_field.proposed_value) == hts
        assert (qty_field.effective_value or qty_field.proposed_value) == qty
        assert (unit_field.effective_value or unit_field.proposed_value) == unit

    assert not {
        str(field.effective_value or field.proposed_value or "").upper()
        for field in detail.fields
        if field.field_name == "hts_code"
    } & {"PAL", "BOX", "CART", "ACTTRAY18", "RUBCB32"}

    snapshot = consolidate_export_snapshot(detail=detail, evidence_snapshot={})
    xml_data = build_lawgs_xml(snapshot)
    root = ET.fromstring(xml_data)
    ns = {"lawgs": LAWGS_XML_NAMESPACE}
    rows = root.findall("lawgs:merchandise", ns)
    assert len(rows) == 3
    assert [row.findtext("lawgs:htsusNumber", namespaces=ns) for row in rows] == [
        "4419909000",
        "4419908000",
        "4421999880",
    ]
    assert [row.findtext("lawgs:quantityMaterial", namespaces=ns) for row in rows] == [
        "315",
        "510",
        "145",
    ]
    assert all(row.findtext("lawgs:unit", namespaces=ns) == "kg" for row in rows)

    reset_us_lacey_engine_state()
