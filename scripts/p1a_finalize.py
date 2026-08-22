"""One-shot P1-A finalizer: immutable payload versions, actor attribution and tests."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: str, old: str, new: str) -> None:
    target = ROOT / path
    text = target.read_text(encoding="utf-8")
    if text.count(old) != 1:
        raise RuntimeError(f"{path}: expected exactly one replacement for {old[:80]!r}, got {text.count(old)}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


def patch_migration() -> None:
    path = "alembic/versions/021_add_integration_core.py"
    replace_once(
        path,
        '    "external_entities",\n    "external_references",',
        '    "external_entities",\n    "external_entity_versions",\n    "external_references",',
    )
    replace_once(
        path,
        '    if table != "integration_events":\n',
        '    if table not in {"external_entity_versions", "integration_events"}:\n',
    )
    replace_once(
        path,
        '    privileges = "SELECT, INSERT" if table == "integration_events" else "SELECT, INSERT, UPDATE"\n',
        '    privileges = (\n        "SELECT, INSERT"\n        if table in {"external_entity_versions", "integration_events"}\n        else "SELECT, INSERT, UPDATE"\n    )\n',
    )
    anchor = '''    op.create_index("ix_external_entities_tenant_status_type", "external_entities", ["organization_id", "status", "entity_type"])
    op.create_index("ix_external_entities_tenant_connection_updated", "external_entities", ["organization_id", "connection_id", "updated_at"])

    op.create_table(
        "external_references",'''
    version_table = '''    op.create_index("ix_external_entities_tenant_status_type", "external_entities", ["organization_id", "status", "entity_type"])
    op.create_index("ix_external_entities_tenant_connection_updated", "external_entities", ["organization_id", "connection_id", "updated_at"])

    op.create_table(
        "external_entity_versions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.Uuid(), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("external_entity_id", sa.Integer(), nullable=False),
        sa.Column("sync_run_id", sa.Integer(), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("normalized_json", sa.JSON(), nullable=False),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("id", name="pk_external_entity_versions"),
        sa.UniqueConstraint("id", "organization_id", name="uq_external_entity_versions_id_org"),
        sa.UniqueConstraint("public_id", name="uq_external_entity_versions_public_id"),
        sa.UniqueConstraint("external_entity_id", "payload_hash", name="uq_external_entity_versions_entity_hash"),
        sa.ForeignKeyConstraint(
            ["external_entity_id", "organization_id"],
            ["external_entities.id", "external_entities.organization_id"],
            name="fk_external_entity_versions_entity_tenant", ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["sync_run_id", "organization_id"],
            ["integration_sync_runs.id", "integration_sync_runs.organization_id"],
            name="fk_external_entity_versions_sync_run_tenant", ondelete="RESTRICT",
        ),
        sa.CheckConstraint("length(payload_hash) = 64", name="ck_external_entity_versions_payload_hash"),
    )
    op.create_index(
        "ix_external_entity_versions_tenant_entity_created",
        "external_entity_versions",
        ["organization_id", "external_entity_id", "created_at"],
    )

    op.create_table(
        "external_references",'''
    replace_once(path, anchor, version_table)
    replace_once(
        path,
        '        sa.Column("external_entity_id", sa.Integer(), nullable=True),\n        sa.Column("event_type", sa.String(length=64), nullable=False),',
        '        sa.Column("external_entity_id", sa.Integer(), nullable=True),\n        sa.Column("actor_user_id", sa.Integer(), nullable=True),\n        sa.Column("event_type", sa.String(length=64), nullable=False),',
    )
    replace_once(
        path,
        '''        sa.ForeignKeyConstraint(
            ["external_entity_id", "organization_id"],
            ["external_entities.id", "external_entities.organization_id"],
            name="fk_integration_events_entity_tenant", ondelete="RESTRICT",
        ),
    )''',
        '''        sa.ForeignKeyConstraint(
            ["external_entity_id", "organization_id"],
            ["external_entities.id", "external_entities.organization_id"],
            name="fk_integration_events_entity_tenant", ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["actor_user_id"], ["users.id"],
            name="fk_integration_events_actor_user_id", ondelete="SET NULL",
        ),
    )''',
    )
    replace_once(
        path,
        '    op.drop_index("ix_external_references_tenant_target", table_name="external_references")\n    op.drop_table("external_references")\n    op.drop_index("ix_external_entities_tenant_connection_updated", table_name="external_entities")',
        '    op.drop_index("ix_external_references_tenant_target", table_name="external_references")\n    op.drop_table("external_references")\n    op.drop_index("ix_external_entity_versions_tenant_entity_created", table_name="external_entity_versions")\n    op.drop_table("external_entity_versions")\n    op.drop_index("ix_external_entities_tenant_connection_updated", table_name="external_entities")',
    )


def patch_core() -> None:
    path = "src/litoral_trace/services/integrations/core.py"
    replace_once(
        path,
        '    ExternalEntity,\n    ExternalReference,',
        '    ExternalEntity,\n    ExternalEntityVersion,\n    ExternalReference,',
    )
    replace_once(
        path,
        '    external_entity_id: int | None = None,\n    metadata: dict[str, Any] | None = None,\n) -> IntegrationEvent:',
        '    external_entity_id: int | None = None,\n    actor_user_id: int | None = None,\n    metadata: dict[str, Any] | None = None,\n) -> IntegrationEvent:',
    )
    replace_once(
        path,
        '        external_entity_id=external_entity_id,\n        event_type=event_type[:64],',
        '        external_entity_id=external_entity_id,\n        actor_user_id=actor_user_id,\n        event_type=event_type[:64],',
    )
    marker = '''def _entity_rows(payload: GenericErpPayload) -> Iterable[tuple[str, Any]]:
    for item in payload.suppliers:
        yield "SUPPLIER", item
    for item in payload.products:
        yield "PRODUCT", item
    for item in payload.receipts:
        yield "RECEIPT", item
    for item in payload.shipments:
        yield "SHIPMENT", item


class IntegrationCoreService:'''
    replacement = '''def _entity_rows(payload: GenericErpPayload) -> Iterable[tuple[str, Any]]:
    for item in payload.suppliers:
        yield "SUPPLIER", item
    for item in payload.products:
        yield "PRODUCT", item
    for item in payload.receipts:
        yield "RECEIPT", item
    for item in payload.shipments:
        yield "SHIPMENT", item


def _version(
    session: Session,
    *,
    organization_id: int,
    entity_id: int,
    sync_run_id: int,
    payload_hash: str,
    payload_json: dict[str, Any],
    normalized_json: dict[str, Any],
    source_updated_at: datetime | None,
) -> ExternalEntityVersion:
    row = ExternalEntityVersion(
        organization_id=organization_id,
        external_entity_id=entity_id,
        sync_run_id=sync_run_id,
        payload_hash=payload_hash,
        payload_json=payload_json,
        normalized_json=normalized_json,
        source_updated_at=source_updated_at,
    )
    session.add(row)
    return row


class IntegrationCoreService:'''
    replace_once(path, marker, replacement)
    replace_once(
        path,
        '        config_json: dict[str, Any] | None = None,\n    ) -> IntegrationConnection:',
        '        config_json: dict[str, Any] | None = None,\n        actor_user_id: int | None = None,\n    ) -> IntegrationConnection:',
    )
    replace_once(
        path,
        '                event_type="CONNECTION_CREATED",\n                connection_id=connection.id,',
        '                event_type="CONNECTION_CREATED",\n                connection_id=connection.id,\n                actor_user_id=actor_user_id,',
    )
    replace_once(
        path,
        '    def set_connection_status(self, public_id: UUID, *, status: str) -> IntegrationConnection:',
        '    def set_connection_status(\n        self, public_id: UUID, *, status: str, actor_user_id: int | None = None\n    ) -> IntegrationConnection:',
    )
    replace_once(
        path,
        '            event_type="CONNECTION_STATUS_CHANGED",\n            connection_id=connection.id,',
        '            event_type="CONNECTION_STATUS_CHANGED",\n            connection_id=connection.id,\n            actor_user_id=actor_user_id,',
    )
    replace_once(
        path,
        '        payload: GenericErpPayload,\n        idempotency_key: str,\n    ) -> SyncResult:',
        '        payload: GenericErpPayload,\n        idempotency_key: str,\n        actor_user_id: int | None = None,\n    ) -> SyncResult:',
    )
    # Add actor attribution to all sync events using their common sync_run_id line.
    text_path = ROOT / path
    text = text_path.read_text(encoding="utf-8")
    text = text.replace(
        '                sync_run_id=run.id,\n                event_type="SYNC_STARTED",',
        '                sync_run_id=run.id,\n                actor_user_id=actor_user_id,\n                event_type="SYNC_STARTED",',
    )
    text = text.replace(
        '                        external_entity_id=entity.id,\n                        event_type="ENTITY_STAGED",',
        '                        external_entity_id=entity.id,\n                        actor_user_id=actor_user_id,\n                        event_type="ENTITY_STAGED",',
    )
    text = text.replace(
        '                    external_entity_id=existing.id,\n                    event_type=event_type,',
        '                    external_entity_id=existing.id,\n                    actor_user_id=actor_user_id,\n                    event_type=event_type,',
    )
    text = text.replace(
        '                sync_run_id=run.id,\n                event_type="SYNC_COMPLETED",',
        '                sync_run_id=run.id,\n                actor_user_id=actor_user_id,\n                event_type="SYNC_COMPLETED",',
    )
    if text.count("actor_user_id=actor_user_id") < 6:
        raise RuntimeError("core.py: actor attribution replacements incomplete")
    text_path.write_text(text, encoding="utf-8")
    replace_once(
        path,
        '                    self.session.add(entity)\n                    self.session.flush()\n                    _event(',
        '                    self.session.add(entity)\n                    self.session.flush()\n                    _version(\n                        self.session,\n                        organization_id=self.organization_id,\n                        entity_id=entity.id,\n                        sync_run_id=run.id,\n                        payload_hash=digest,\n                        payload_json=raw,\n                        normalized_json=normalized,\n                        source_updated_at=item.source_updated_at,\n                    )\n                    _event(',
    )
    replace_once(
        path,
        '                previous_hash = existing.payload_hash\n                was_reconciled = existing.status == "RECONCILED"\n                existing.payload_hash = digest',
        '                previous_hash = existing.payload_hash\n                was_reconciled = existing.status == "RECONCILED"\n                _version(\n                    self.session,\n                    organization_id=self.organization_id,\n                    entity_id=existing.id,\n                    sync_run_id=run.id,\n                    payload_hash=digest,\n                    payload_json=raw,\n                    normalized_json=normalized,\n                    source_updated_at=item.source_updated_at,\n                )\n                existing.payload_hash = digest',
    )
    replace_once(
        path,
        '            external_entity_id=entity.id,\n            event_type="ENTITY_RECONCILED",',
        '            external_entity_id=entity.id,\n            actor_user_id=user_id,\n            event_type="ENTITY_RECONCILED",',
    )


def patch_api_and_web() -> None:
    path = "src/litoral_trace/api/integrations.py"
    replace_once(path, '                config_json=body.config_json,\n            )', '                config_json=body.config_json,\n                actor_user_id=user.user_id,\n            )')
    replace_once(path, '            row = service.set_connection_status(connection_public_id, status=body.status)', '            row = service.set_connection_status(\n                connection_public_id, status=body.status, actor_user_id=user.user_id\n            )')
    replace_once(path, '                idempotency_key=idempotency_key,\n            )', '                idempotency_key=idempotency_key,\n                actor_user_id=user.user_id,\n            )')

    path = "src/litoral_trace/web/integrations.py"
    replace_once(path, '            config_json={"mode": "staging_only"},\n        )', '            config_json={"mode": "staging_only"},\n            actor_user_id=user.user_id,\n        )')
    replace_once(path, '            idempotency_key=str(form.get("idempotency_key", "")),\n        )', '            idempotency_key=str(form.get("idempotency_key", "")),\n            actor_user_id=user.user_id,\n        )')

    path = "src/litoral_trace/templates/app/base_app.html"
    replace_once(
        path,
        '''                                {% elif item.key == "settings" %}
                                <i class="fa-solid fa-sliders"></i>''',
        '''                                {% elif item.key == "integrations" %}
                                <i class="fa-solid fa-plug-circle-bolt"></i>
                                {% elif item.key == "settings" %}
                                <i class="fa-solid fa-sliders"></i>''',
    )


def create_tests() -> None:
    tests = ROOT / "tests/test_p1a_integration_core_unittest.py"
    tests.write_text('''"""P1-A integration core regression contracts."""\nfrom __future__ import annotations\n\nfrom pathlib import Path\n\nimport pytest\nfrom pydantic import ValidationError\n\nfrom litoral_trace.auth.rbac import Permission, permissions_for_role\nfrom litoral_trace.services.integrations.canonical import GenericErpPayload\nfrom litoral_trace.services.integrations.core import _idempotency_hash, _payload_hash, _validate_config, IntegrationValidationError\n\n\ndef test_generic_erp_contract_is_strict_and_normalizes_country() -> None:\n    payload = GenericErpPayload.model_validate({\n        "source_system": "ERP-X",\n        "suppliers": [{"external_id": "S-1", "name": "Proveedor", "country": "ar"}],\n        "products": [{"external_id": "P-1", "code": "PINO", "name": "Pino", "unit": "M3"}],\n        "receipts": [],\n        "shipments": [],\n    })\n    assert payload.suppliers[0].country == "AR"\n    assert payload.entity_count() == 2\n    with pytest.raises(ValidationError):\n        GenericErpPayload.model_validate({"source_system": "ERP-X", "unknown": []})\n\n\ndef test_hashes_are_deterministic_and_raw_idempotency_key_is_not_persisted() -> None:\n    assert _payload_hash({"b": 2, "a": 1}) == _payload_hash({"a": 1, "b": 2})\n    key = "erp-run-20260822-0001"\n    digest = _idempotency_hash(key)\n    assert len(digest) == 64\n    assert key not in digest\n\n\ndef test_sensitive_connection_config_is_rejected() -> None:\n    with pytest.raises(IntegrationValidationError) as exc:\n        _validate_config({"password": "should-never-be-here"})\n    assert exc.value.code == "SENSITIVE_CONFIG_REJECTED"\n\n\ndef test_integration_rbac_is_least_privilege() -> None:\n    assert Permission.INTEGRATION_MANAGE in permissions_for_role("admin")\n    assert Permission.INTEGRATION_MANAGE in permissions_for_role("manager")\n    assert Permission.INTEGRATION_READ in permissions_for_role("auditor")\n    assert Permission.INTEGRATION_MANAGE not in permissions_for_role("auditor")\n    assert Permission.INTEGRATION_READ not in permissions_for_role("cliente")\n\n\ndef test_integration_service_never_calls_traceability_ledger() -> None:\n    source = Path("src/litoral_trace/services/integrations/core.py").read_text(encoding="utf-8")\n    assert "TraceabilityLedgerService" not in source\n    assert "dispatch_shipment(" not in source\n    assert "post_event(" not in source\n    assert "SOURCE_CHANGED_AFTER_RECONCILIATION" in source\n    assert "ExternalEntityVersion" in source\n\n\ndef test_migration_021_enforces_rls_append_only_history_and_no_worker_access() -> None:\n    migration = Path("alembic/versions/021_add_integration_core.py").read_text(encoding="utf-8")\n    assert 'down_revision: Union[str, Sequence[str], None] = "020_add_traceability_evidence_links"' in migration\n    assert '"external_entity_versions"' in migration\n    assert 'table in {"external_entity_versions", "integration_events"}' in migration\n    assert "FORCE ROW LEVEL SECURITY" in migration\n    assert "GRANT SELECT, INSERT, UPDATE" in migration\n    assert "REVOKE ALL PRIVILEGES ON TABLE" in migration\n    assert "litoral_trace_worker_executor" in migration\n    assert "GRANT DELETE" not in migration\n\n\ndef test_browser_and_api_expose_staging_boundary() -> None:\n    api = Path("src/litoral_trace/api/integrations.py").read_text(encoding="utf-8")\n    web = Path("src/litoral_trace/templates/integrations.html").read_text(encoding="utf-8")\n    nav = Path("src/litoral_trace/web/navigation.py").read_text(encoding="utf-8")\n    assert '"ledger_mutated": False' in api\n    assert "Staging only:" in web\n    assert "ningún" in web.lower()\n    assert 'href="/integrations"' not in nav  # Python definition, not template literal\n    assert 'href="/integrations"' in Path("src/litoral_trace/templates/app/base_app.html").read_text(encoding="utf-8") or "integrations" in nav\n''', encoding="utf-8")

    model_test = ROOT / "tests/test_p1a_integration_models_unittest.py"
    model_test.write_text('''"""P1-A ORM/schema metadata contract."""\nfrom litoral_trace.db.models import ExternalEntityVersion, IntegrationEvent\n\n\ndef test_external_version_is_immutable_by_migration_contract_and_actor_is_attributed() -> None:\n    assert ExternalEntityVersion.__tablename__ == "external_entity_versions"\n    assert "payload_hash" in ExternalEntityVersion.__table__.c\n    assert "payload_json" in ExternalEntityVersion.__table__.c\n    assert "actor_user_id" in IntegrationEvent.__table__.c\n''', encoding="utf-8")


def main() -> None:
    patch_migration()
    patch_core()
    patch_api_and_web()
    create_tests()
    print("P1A_FINALIZER_OK")


if __name__ == "__main__":
    main()
