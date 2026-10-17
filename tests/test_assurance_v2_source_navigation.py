"""Source navigation never resolves documents outside current tenant/revision."""
from __future__ import annotations

from sqlalchemy.orm import sessionmaker

from litoral_trace.db.models import VaultDocument
from litoral_trace.web.us_lacey_assurance_v2_runtime import _resolve_current_source_document
from test_assurance_v2_core_memory import (
    session, org_and_actor, shipment, supplier_product,
)


def test_source_resolver_requires_current_tenant_operation_and_document(session):
    org, _ = org_and_actor(session, "SourceLink")
    other, _ = org_and_actor(session, "OtherSourceLink")
    _, product = supplier_product(session, org.id, supplier_key="MID:SOURCE-LINK")
    op, document, _, revision, _ = shipment(
        session, org.id, product.id, reference="SOURCE-LINK-1",
        source_value="Quercus alba",
    )
    vault = session.get(VaultDocument, document.vault_document_id)
    session.commit()
    factory = sessionmaker(bind=session.get_bind(), expire_on_commit=False)

    found = _resolve_current_source_document(
        organization_id=org.id, operation_public_id=str(op.public_id),
        assurance_document_id=str(document.id), session_factory=factory,
    )
    assert found is not None
    assert found["vault_public_id"] == str(vault.public_id)
    assert found["sha256"] == vault.sha256
    assert _resolve_current_source_document(
        organization_id=other.id, operation_public_id=str(op.public_id),
        assurance_document_id=str(document.id), session_factory=factory,
    ) is None
    assert _resolve_current_source_document(
        organization_id=org.id, operation_public_id=str(op.public_id),
        assurance_document_id="999999", session_factory=factory,
    ) is None

    revision.is_current = False
    session.commit()
    assert _resolve_current_source_document(
        organization_id=org.id, operation_public_id=str(op.public_id),
        assurance_document_id=str(document.id), session_factory=factory,
    ) is None
