from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import os
from uuid import uuid4

import pytest
from sqlalchemy import inspect, select, update

from litoral_trace.assurance.processing import AssuranceProcessingService
from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentExtractionRun,
    ExtractedDocumentField,
    UsLaceyEvidenceClaim,
    UsLaceyOperation,
    UsLaceyOperationDocument,
    UsLaceyOperationField,
    UsLaceyOperationProductLink,
    UsLaceyPpqPlantLine,
    UsLaceySupplier,
    UsLaceySupplierEvidence,
    UsLaceySupplierIdentifier,
    UsLaceySupplierProduct,
    UsLaceySourceSetRevision,
    User,
    VaultDocument,
)
from litoral_trace.product_intelligence.bom_ingestion import ingest_bom_table
from litoral_trace.assurance.parsers import parse_document
from litoral_trace.us_lacey.product_intelligence_snapshot import build_product_intelligence_snapshot
from litoral_trace.us_lacey.operations import UsLaceyOperationService
from litoral_trace.us_lacey.reusable_evidence import REUSABLE_EVIDENCE_EXTRACTOR
from litoral_trace.us_lacey.reusable_evidence_promotion import (
    apply_reusable_evidence_for_operation,
)
from litoral_trace.us_lacey.review import review_us_lacey_field
from litoral_trace.us_lacey.shipment_product_bridge import build_shipment_product_bridge
from litoral_trace.us_lacey.source_sets import SourceSetClaim, finalize_claim, seal_current_source_set
from tests.us_lacey_engine2_postgres import (
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


LINE = "1"
SKU = "CHAIR-001"
SUPPLIER = "Northwoods Supplier LLC"
ADDRESS = "100 Timber Way, Portland, OR 97201"
MID = "USNORTIM100POR"


def _bom_csv(*, include_botanical: bool) -> bytes:
    headers = [
        "Line",
        "Supplier",
        "Supplier Address",
        "MID",
        "SKU",
        "Product Name",
        "Component",
        "Material",
    ]
    values = [
        LINE,
        SUPPLIER,
        ADDRESS,
        MID,
        SKU,
        "White Oak Chair",
        "Seat frame",
        "White oak",
    ]
    if include_botanical:
        headers += ["Species", "Country of Harvest"]
        values += ["Quercus alba", "US"]
    return (";".join(headers) + "\n" + ";".join(values) + "\n").encode("utf-8")


def _analysis_payload(content: bytes) -> dict:
    parsed = parse_document("shipment-bom.csv", content)
    assert len(parsed.tables) == 1
    ingested = ingest_bom_table(parsed.tables[0], document_id="fixture-bom")
    assert len(ingested.compositions) == 1
    composition = ingested.compositions[0]
    return {
        "schema_version": "product-intelligence-snapshot-v2",
        "sources": [
            {
                "document_id": "fixture-bom",
                "filename": "shipment-bom.csv",
                "tables": [
                    {
                        "name": parsed.tables[0].name,
                        "source": {"row": 1},
                        "compositions": [
                            {
                                "sku": composition.sku,
                                "product_name": composition.product_name,
                                "components": [],
                            }
                        ],
                        "issues": [],
                    }
                ],
            }
        ],
        "issues": [],
    }


def test_bridge_accepts_explicit_line_product_binding_when_line_reference_differs_from_sku():
    payload = _analysis_payload(_bom_csv(include_botanical=True))

    bridge = build_shipment_product_bridge(
        payload,
        line_references=(LINE,),
        explicit_links=(
            {
                "line_reference": LINE,
                "sku": SKU,
                "product_key": f"SKU:{SKU}",
                "link_method": "EXACT_SKU",
            },
        ),
    )

    assert bridge["linked_count"] == 1
    assert bridge["review_count"] == 0
    assert bridge["links"][0]["shipment_line_reference"] == LINE
    assert bridge["links"][0]["line_item_key"] == f"SKU:{SKU}"


class _Download:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def iter_chunks(self, *, chunk_size: int = 1024 * 1024):
        del chunk_size
        yield self._payload


class _FixtureVault:
    def __init__(self, by_public_id: dict[str, bytes]) -> None:
        self.by_public_id = by_public_id

    @contextmanager
    def materialize_verified_download(self, *, organization_id: int, document_id):
        del organization_id
        yield _Download(self.by_public_id[str(document_id)])


def _add_csv_document(
    factory,
    *,
    organization_id: int,
    operation_id: int,
    role: str,
    filename: str,
    content: bytes,
):
    session = tenant_session(factory, organization_id)
    try:
        suffix = uuid4().hex
        vault = VaultDocument(
            organization_id=organization_id,
            original_filename=filename,
            content_type="text/csv",
            size_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            object_key=f"tests/{suffix}",
            storage_backend="s3",
            storage_bucket="tests",
            document_type="OTHER_EVIDENCE",
            status="available",
        )
        session.add(vault)
        session.flush()
        assurance = AssuranceDocument(
            organization_id=organization_id,
            vault_document_id=vault.id,
            semantic_document_type="UNKNOWN",
            processing_status="UPLOADED",
        )
        session.add(assurance)
        session.flush()
        link = UsLaceyOperationDocument(
            organization_id=organization_id,
            operation_id=operation_id,
            assurance_document_id=assurance.id,
            document_role=role,
            version_number=1,
            is_current=True,
        )
        session.add(link)
        operation = session.get(UsLaceyOperation, operation_id)
        operation.document_count = int(operation.document_count or 0) + 1
        session.commit()
        return assurance.id, assurance.public_id, str(vault.public_id)
    finally:
        session.close()


def _create_operation(factory, *, organization_id: int, label: str):
    session = tenant_session(factory, organization_id)
    try:
        operation = UsLaceyOperation(
            organization_id=organization_id,
            client_reference=f"{label}-{uuid4().hex[:8]}",
            status="REVIEW_REQUIRED",
            document_count=0,
            merchandise_line_count=1,
        )
        session.add(operation)
        session.flush()
        plant_line = UsLaceyPpqPlantLine(
            organization_id=organization_id,
            operation_id=operation.id,
            line_reference=LINE,
            ordinal=1,
        )
        session.add(plant_line)
        session.flush()
        session.commit()
        return operation.id, operation.public_id, plant_line.id
    finally:
        session.close()




def _claim_revision(
    factory,
    *,
    organization_id: int,
    revision: UsLaceySourceSetRevision,
) -> SourceSetClaim:
    session = tenant_session(factory, organization_id)
    try:
        claimed_at = session.execute(
            update(UsLaceySourceSetRevision)
            .where(
                UsLaceySourceSetRevision.organization_id == organization_id,
                UsLaceySourceSetRevision.id == revision.id,
                UsLaceySourceSetRevision.is_current.is_(True),
            )
            .values(
                status="FINALIZING",
                claimed_at=datetime.now(timezone.utc),
            )
            .returning(UsLaceySourceSetRevision.claimed_at)
        ).scalar_one()
        session.commit()
    finally:
        session.close()

    return SourceSetClaim(
        revision_id=revision.id,
        generation=revision.generation,
        fingerprint=revision.source_set_fingerprint,
        claimed=True,
        reason="CLAIMED",
        claimed_at=claimed_at,
    )


def _raw_value_for_header(session, *, organization_id: int, assurance_document_id: int, header: str):
    fields = session.scalars(
        select(ExtractedDocumentField).where(
            ExtractedDocumentField.organization_id == organization_id,
            ExtractedDocumentField.assurance_document_id == assurance_document_id,
        )
    ).all()
    suffix = "." + header.casefold()
    matches = [
        row
        for row in fields
        if str(row.field_name).casefold().endswith(suffix)
    ]
    assert len(matches) == 1
    return matches[0]


def _add_review_field(
    factory,
    *,
    organization_id: int,
    operation_id: int,
    plant_line_id: int,
    source_assurance_document_id: int | None,
    field_name: str,
    extracted_value: str | None,
):
    session = tenant_session(factory, organization_id)
    try:
        field = UsLaceyOperationField(
            organization_id=organization_id,
            operation_id=operation_id,
            merchandise_line_reference=LINE,
            field_name=field_name,
            field_scope="PLANT_LINE",
            plant_line_id=plant_line_id,
            original_value=extracted_value,
            normalized_value=extracted_value,
            field_status="REVIEW" if extracted_value else "MISSING",
            confidence=0.98 if extracted_value else 0.0,
            source_assurance_document_id=source_assurance_document_id,
            source_page=1 if source_assurance_document_id else None,
            source_locator=f"fixture:{field_name}" if source_assurance_document_id else None,
            extractor="assurance-fixture" if source_assurance_document_id else None,
            extractor_version="1" if source_assurance_document_id else None,
            validation_status="VALID" if extracted_value else "MISSING",
        )
        session.add(field)
        session.commit()
        return int(field.id)
    finally:
        session.close()


@pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1",
    reason="requires isolated U.S. Lacey PostgreSQL",
)
def test_true_multishipment_reuse_without_cuit_or_manual_supplier_bridge(
    monkeypatch,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    required_tables = {
        "us_lacey_supplier_identifier",
        "us_lacey_operation_product_link",
    }
    if not required_tables.issubset(inspect(engine2_postgres_engine).get_table_names()):
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_075")

    factory = engine2_postgres_session_factory

    # Reuse the existing fixture's organization creation path without inserting
    # any supplier/product identity or operation-product bridge rows ourselves.
    from tests.us_lacey_engine2_postgres import create_test_graph

    org_id, seed_operation_id, _, _, _, _ = create_test_graph(factory)
    seed = tenant_session(factory, org_id)
    try:
        seed.delete(seed.get(UsLaceyOperation, seed_operation_id))
        user = User(
            organization_id=org_id,
            email=f"reuse-{uuid4().hex[:8]}@example.com",
            username=f"reuse_{uuid4().hex[:8]}",
            password_hash="test-only",
            role="cliente",
            full_name="Reuse Reviewer",
            is_active=True,
        )
        seed.add(user)
        seed.commit()
        user_id = int(user.id)
    finally:
        seed.close()

    operation_1_id, operation_1_public_id, line_1_id = _create_operation(
        factory, organization_id=org_id, label="shipment-one"
    )
    shipment_1 = _bom_csv(include_botanical=True)
    doc_1_id, doc_1_public_id, vault_1_public_id = _add_csv_document(
        factory,
        organization_id=org_id,
        operation_id=operation_1_id,
        role="SUPPLIER_DECLARATION",
        filename="shipment-1-bom.csv",
        content=shipment_1,
    )

    processing = AssuranceProcessingService(
        session_factory=factory,
        vault_service=_FixtureVault({vault_1_public_id: shipment_1}),
    )
    shipment_1_processing_status = processing.process(
        organization_id=org_id,
        assurance_public_id=doc_1_public_id,
    )
    assert shipment_1_processing_status in {"EXTRACTED", "NEEDS_REVIEW"}

    session = tenant_session(factory, org_id)
    try:
        species_raw = _raw_value_for_header(
            session,
            organization_id=org_id,
            assurance_document_id=doc_1_id,
            header="Species",
        )
        country_raw = _raw_value_for_header(
            session,
            organization_id=org_id,
            assurance_document_id=doc_1_id,
            header="Country of Harvest",
        )
    finally:
        session.close()

    species_1_id = _add_review_field(
        factory,
        organization_id=org_id,
        operation_id=operation_1_id,
        plant_line_id=line_1_id,
        source_assurance_document_id=doc_1_id,
        field_name="species",
        extracted_value=str(species_raw.normalized_value),
    )
    country_1_id = _add_review_field(
        factory,
        organization_id=org_id,
        operation_id=operation_1_id,
        plant_line_id=line_1_id,
        source_assurance_document_id=doc_1_id,
        field_name="country_of_harvest",
        extracted_value=str(country_raw.normalized_value),
    )

    revision_1 = seal_current_source_set(
        organization_id=org_id,
        operation_id=operation_1_id,
        session_factory=factory,
    )
    claim_1 = _claim_revision(
        factory,
        organization_id=org_id,
        revision=revision_1,
    )
    snapshot_1 = build_product_intelligence_snapshot(
        organization_id=org_id,
        operation_id=operation_1_id,
        claim=claim_1,
        session_factory=factory,
        vault_service=_FixtureVault({vault_1_public_id: shipment_1}),
    )
    assert snapshot_1 is not None
    assert snapshot_1.payload_json["identity_memory"] == {
        "supplier_count": 1,
        "product_count": 1,
        "link_count": 1,
        "ambiguous_supplier_count": 0,
    }
    bridge_1 = snapshot_1.payload_json["shipment_product_bridge"]
    assert bridge_1["linked_count"] == 1
    assert bridge_1["links"][0]["shipment_line_reference"] == LINE
    assert bridge_1["links"][0]["product"]["sku"] == SKU
    assert finalize_claim(
        organization_id=org_id,
        claim=claim_1,
        session_factory=factory,
    )

    import litoral_trace.us_lacey.review as review_module

    monkeypatch.setattr(review_module, "get_us_lacey_db_session", factory)
    review_us_lacey_field(
        organization_id=org_id,
        operation_public_id=operation_1_public_id,
        field_id=species_1_id,
        user_id=user_id,
        user_email="reuse-reviewer@example.com",
        action="edit",
        value="Quercus alba",
    )
    review_us_lacey_field(
        organization_id=org_id,
        operation_public_id=operation_1_public_id,
        field_id=country_1_id,
        user_id=user_id,
        user_email="reuse-reviewer@example.com",
        action="edit",
        value="United States",
    )

    session = tenant_session(factory, org_id)
    try:
        assert session.scalar(
            select(UsLaceySupplier).where(UsLaceySupplier.organization_id == org_id)
        ) is not None
        assert session.scalar(
            select(UsLaceySupplierIdentifier).where(
                UsLaceySupplierIdentifier.organization_id == org_id
            )
        ) is not None
        product = session.scalar(
            select(UsLaceySupplierProduct).where(
                UsLaceySupplierProduct.organization_id == org_id,
                UsLaceySupplierProduct.sku == SKU,
            )
        )
        assert product is not None
        assert session.scalar(
            select(UsLaceyOperationProductLink).where(
                UsLaceyOperationProductLink.organization_id == org_id,
                UsLaceyOperationProductLink.operation_id == operation_1_id,
                UsLaceyOperationProductLink.line_reference == LINE,
            )
        ) is not None
        evidence = session.scalars(
            select(UsLaceySupplierEvidence).where(
                UsLaceySupplierEvidence.organization_id == org_id,
                UsLaceySupplierEvidence.supplier_product_id == product.id,
                UsLaceySupplierEvidence.status == "VERIFIED",
            )
        ).all()
        claims = session.scalars(
            select(UsLaceyEvidenceClaim).where(
                UsLaceyEvidenceClaim.organization_id == org_id,
                UsLaceyEvidenceClaim.evidence_id.in_([row.id for row in evidence]),
            )
        ).all()
        assert {claim.field_name for claim in claims} >= {
            "species",
            "country_of_harvest",
        }
    finally:
        session.close()

    # Shipment 2 has a different source document but the same exact supplier
    # identifier + SKU. Botanical values are absent and must come from memory.
    operation_2_id, operation_2_public_id, line_2_id = _create_operation(
        factory, organization_id=org_id, label="shipment-two"
    )
    shipment_2 = _bom_csv(include_botanical=False)
    doc_2_id, doc_2_public_id, vault_2_public_id = _add_csv_document(
        factory,
        organization_id=org_id,
        operation_id=operation_2_id,
        role="COMMERCIAL_INVOICE",
        filename="shipment-2-bom.csv",
        content=shipment_2,
    )
    processing_2 = AssuranceProcessingService(
        session_factory=factory,
        vault_service=_FixtureVault({vault_2_public_id: shipment_2}),
    )
    shipment_2_processing_status = processing_2.process(
        organization_id=org_id,
        assurance_public_id=doc_2_public_id,
    )
    assert shipment_2_processing_status in {"EXTRACTED", "NEEDS_REVIEW"}

    species_2_id = _add_review_field(
        factory,
        organization_id=org_id,
        operation_id=operation_2_id,
        plant_line_id=line_2_id,
        source_assurance_document_id=None,
        field_name="species",
        extracted_value=None,
    )
    country_2_id = _add_review_field(
        factory,
        organization_id=org_id,
        operation_id=operation_2_id,
        plant_line_id=line_2_id,
        source_assurance_document_id=None,
        field_name="country_of_harvest",
        extracted_value=None,
    )

    revision_2 = seal_current_source_set(
        organization_id=org_id,
        operation_id=operation_2_id,
        session_factory=factory,
    )
    claim_2 = _claim_revision(
        factory,
        organization_id=org_id,
        revision=revision_2,
    )
    snapshot_2 = build_product_intelligence_snapshot(
        organization_id=org_id,
        operation_id=operation_2_id,
        claim=claim_2,
        session_factory=factory,
        vault_service=_FixtureVault({vault_2_public_id: shipment_2}),
    )
    assert snapshot_2 is not None
    assert snapshot_2.payload_json["identity_memory"] == {
        "supplier_count": 1,
        "product_count": 1,
        "link_count": 1,
        "ambiguous_supplier_count": 0,
    }
    bridge_2 = snapshot_2.payload_json["shipment_product_bridge"]
    assert bridge_2["linked_count"] == 1
    assert bridge_2["links"][0]["shipment_line_reference"] == LINE
    assert bridge_2["links"][0]["product"]["sku"] == SKU

    assert apply_reusable_evidence_for_operation(
        organization_id=org_id,
        operation_id=operation_2_id,
        session_factory=factory,
    ) == 2
    assert finalize_claim(
        organization_id=org_id,
        claim=claim_2,
        session_factory=factory,
    )

    detail = UsLaceyOperationService(session_factory=factory).get_detail(
        organization_id=org_id,
        operation_public_id=operation_2_public_id,
    )
    by_id = {field.id: field for field in detail.fields}
    assert by_id[species_2_id].effective_value == "Quercus alba"
    assert by_id[species_2_id].provenance == "reused_evidence"
    assert by_id[country_2_id].effective_value == "United States"
    assert by_id[country_2_id].provenance == "reused_evidence"

    session = tenant_session(factory, org_id)
    try:
        stored = {
            row.id: row
            for row in session.scalars(
                select(UsLaceyOperationField).where(
                    UsLaceyOperationField.organization_id == org_id,
                    UsLaceyOperationField.operation_id == operation_2_id,
                )
            ).all()
        }
        assert stored[species_2_id].extractor == REUSABLE_EVIDENCE_EXTRACTOR
        assert stored[country_2_id].extractor == REUSABLE_EVIDENCE_EXTRACTOR
    finally:
        session.close()
