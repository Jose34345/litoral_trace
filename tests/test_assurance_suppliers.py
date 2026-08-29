from __future__ import annotations

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from litoral_trace.assurance.suppliers import AssuranceSupplierService
from litoral_trace.db.models import (
    AssuranceDocument,
    AssuranceSupplier,
    DocumentEntityLink,
    DocumentExtractionRun,
    ExtractedDocumentField,
    VaultDocument,
)


def _factory():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    VaultDocument.__table__.create(engine, checkfirst=True)
    AssuranceDocument.__table__.create(engine, checkfirst=True)
    DocumentExtractionRun.__table__.create(engine, checkfirst=True)
    ExtractedDocumentField.__table__.create(engine, checkfirst=True)
    DocumentEntityLink.__table__.create(engine, checkfirst=True)
    AssuranceSupplier.__table__.create(engine, checkfirst=True)
    return engine, lambda: Session(engine)


def _seed_document(
    factory,
    *,
    organization_id: int,
    suffix: str,
    cuit: str | None = None,
    supplier: str | None = None,
    accepted: bool = True,
):
    session: Session = factory()
    vault = VaultDocument(
        organization_id=organization_id,
        original_filename=f"operacion-{suffix}.csv",
        content_type="text/csv",
        size_bytes=10,
        sha256=(suffix[0] * 64),
        object_key=f"tenant/{organization_id}/{suffix}.csv",
        storage_backend="s3",
        storage_bucket="test",
        document_type="OTHER_EVIDENCE",
        status="available",
    )
    session.add(vault)
    session.flush()
    document = AssuranceDocument(
        organization_id=organization_id,
        vault_document_id=vault.id,
        semantic_document_type="SPREADSHEET",
        type_confidence=0.99,
        processing_status="EXTRACTED",
    )
    session.add(document)
    session.flush()
    run = DocumentExtractionRun(
        organization_id=organization_id,
        assurance_document_id=document.id,
        engine="test",
        engine_version="1",
        status="SUCCEEDED",
    )
    session.add(run)
    session.flush()

    def add_field(field_name: str, value: str):
        session.add(
            ExtractedDocumentField(
                organization_id=organization_id,
                assurance_document_id=document.id,
                extraction_run_id=run.id,
                field_name=field_name,
                original_value=value,
                normalized_value=value,
                value_type="identifier" if field_name == "issuer_cuit" else "text",
                confidence=0.98 if accepted else 0.70,
                confidence_level="HIGH" if accepted else "MEDIUM",
                source_locator="fixture",
                auto_accepted=accepted,
                needs_review=not accepted,
            )
        )

    if cuit is not None:
        add_field("issuer_cuit", cuit)
    if supplier is not None:
        add_field("supplier", supplier)
    session.commit()
    public_id = document.public_id
    session.close()
    return public_id


def test_high_confidence_cuit_creates_and_links_supplier_idempotently():
    engine, factory = _factory()
    public_id = _seed_document(
        factory,
        organization_id=42,
        suffix="a",
        cuit="30708323108",
        supplier="Forestal Norte SA",
    )
    service = AssuranceSupplierService(session_factory=factory)

    first = service.resolve_document(organization_id=42, assurance_public_id=public_id)
    second = service.resolve_document(organization_id=42, assurance_public_id=public_id)

    assert first.created is True
    assert first.linked is True
    assert first.needs_review is False
    assert first.reason == "resolved_with_cuit"
    assert second.created is False
    assert second.linked is False
    assert second.supplier_public_id == first.supplier_public_id

    with Session(engine) as session:
        supplier = session.scalar(select(AssuranceSupplier))
        links = session.scalars(select(DocumentEntityLink)).all()
        assert session.scalar(select(func.count()).select_from(AssuranceSupplier)) == 1
        assert supplier.cuit == "30708323108"
        assert supplier.display_name == "Forestal Norte SA"
        assert supplier.normalized_name == "forestal norte sa"
        assert supplier.status == "AUTO_CREATED"
        assert len(links) == 1
        assert links[0].entity_type == "SUPPLIER"
        assert links[0].entity_reference == f"supplier:{supplier.public_id}"
        assert links[0].link_method == "EXACT_CUIT"


def test_name_only_reuses_exact_known_supplier_but_never_creates_new_one():
    engine, factory = _factory()
    first_id = _seed_document(
        factory,
        organization_id=42,
        suffix="b",
        cuit="30708323108",
        supplier="Forestal Norte S.A.",
    )
    second_id = _seed_document(
        factory,
        organization_id=42,
        suffix="c",
        supplier="  FORESTAL   NORTE SA ",
    )
    service = AssuranceSupplierService(session_factory=factory)
    created = service.resolve_document(organization_id=42, assurance_public_id=first_id)
    reused = service.resolve_document(organization_id=42, assurance_public_id=second_id)

    assert created.created is True
    assert reused.created is False
    assert reused.linked is True
    assert reused.reason == "resolved_with_exact_name"
    assert reused.supplier_public_id == created.supplier_public_id
    with Session(engine) as session:
        assert session.scalar(select(func.count()).select_from(AssuranceSupplier)) == 1
        assert session.scalar(select(func.count()).select_from(DocumentEntityLink)) == 2

    unknown_id = _seed_document(
        factory,
        organization_id=42,
        suffix="d",
        supplier="Proveedor desconocido",
    )
    unknown = service.resolve_document(organization_id=42, assurance_public_id=unknown_id)
    assert unknown.created is False
    assert unknown.linked is False
    assert unknown.needs_review is True
    assert unknown.reason == "name_only_cannot_create"


def test_unreviewed_supplier_evidence_never_creates_identity():
    engine, factory = _factory()
    public_id = _seed_document(
        factory,
        organization_id=42,
        suffix="e",
        cuit="30708323108",
        supplier="Forestal Norte SA",
        accepted=False,
    )
    result = AssuranceSupplierService(session_factory=factory).resolve_document(
        organization_id=42,
        assurance_public_id=public_id,
    )
    assert result.created is False
    assert result.linked is False
    assert result.reason == "no_supplier_evidence"
    with Session(engine) as session:
        assert session.scalar(select(func.count()).select_from(AssuranceSupplier)) == 0


def test_same_cuit_is_isolated_per_tenant_and_cross_tenant_document_is_invisible():
    engine, factory = _factory()
    doc_a = _seed_document(
        factory,
        organization_id=41,
        suffix="f",
        cuit="30708323108",
        supplier="Proveedor A",
    )
    doc_b = _seed_document(
        factory,
        organization_id=42,
        suffix="g",
        cuit="30708323108",
        supplier="Proveedor B",
    )
    service = AssuranceSupplierService(session_factory=factory)
    result_a = service.resolve_document(organization_id=41, assurance_public_id=doc_a)
    result_b = service.resolve_document(organization_id=42, assurance_public_id=doc_b)
    assert result_a.supplier_public_id != result_b.supplier_public_id

    with Session(engine) as session:
        suppliers = session.scalars(select(AssuranceSupplier).order_by(AssuranceSupplier.organization_id)).all()
        assert [(row.organization_id, row.cuit) for row in suppliers] == [
            (41, "30708323108"),
            (42, "30708323108"),
        ]

    try:
        service.resolve_document(organization_id=41, assurance_public_id=doc_b)
    except Exception as exc:
        assert "Documento Assurance no encontrado" in str(exc)
    else:
        raise AssertionError("Cross-tenant document resolution must fail closed")
