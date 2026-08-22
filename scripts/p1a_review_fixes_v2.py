"""Apply accepted PR #89 review fixes with function-scoped replacements."""
from __future__ import annotations

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    (ROOT / path).write_text(text, encoding="utf-8")


def exact(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected 1 exact match, found {count}")
    return text.replace(old, new, 1)


def regex(text: str, pattern: str, replacement: str, *, label: str) -> str:
    result, count = re.subn(pattern, replacement, text, count=1, flags=re.S)
    if count != 1:
        raise RuntimeError(f"{label}: expected 1 regex match, found {count}")
    return result


def patch_core() -> None:
    path = "src/litoral_trace/services/integrations/core.py"
    text = read(path)

    text = exact(
        text,
        "class IntegrationPersistenceError(IntegrationError):\n    pass\n\n\n@dataclass(frozen=True)\nclass SyncResult:",
        "class IntegrationPersistenceError(IntegrationError):\n    pass\n\n\n@dataclass(frozen=True)\nclass ConnectionWriteResult:\n    public_id: UUID\n    name: str\n    connector_type: str\n    status: str\n\n\n@dataclass(frozen=True)\nclass ReconciliationWriteResult:\n    public_id: UUID\n    target_type: str\n    target_reference: str\n\n\n@dataclass(frozen=True)\nclass SyncResult:",
        label="DTO insertion",
    )

    text = regex(
        text,
        r"def _validate_config\(config: dict\[str, Any\] \| None\) -> dict\[str, Any\] \| None:\n.*?(?=\ndef _canonical_json)",
        '''def _contains_forbidden_config_key(value: Any) -> bool:\n    if isinstance(value, dict):\n        for key, child in value.items():\n            normalized_key = str(key).lower()\n            if any(fragment in normalized_key for fragment in _FORBIDDEN_CONFIG_FRAGMENTS):\n                return True\n            if _contains_forbidden_config_key(child):\n                return True\n        return False\n    if isinstance(value, (list, tuple)):\n        return any(_contains_forbidden_config_key(child) for child in value)\n    return False\n\n\ndef _validate_config(config: dict[str, Any] | None) -> dict[str, Any] | None:\n    if not config:\n        return None\n    if _contains_forbidden_config_key(config):\n        raise IntegrationValidationError(\n            "SENSITIVE_CONFIG_REJECTED",\n            "La configuración no puede contener secretos; use secret_ref.",\n        )\n    encoded = json.dumps(config, sort_keys=True, default=str)\n    if len(encoded.encode("utf-8")) > 32_768:\n        raise IntegrationValidationError(\n            "CONFIG_TOO_LARGE",\n            "La configuración de integración supera el máximo permitido.",\n        )\n    return config\n\n''',
        label="recursive config validation",
    )

    text = regex(
        text,
        r"def _version\(\n.*?(?=\ndef _entity_rows)",
        '''def _version(\n    session: Session,\n    *,\n    organization_id: int,\n    external_entity_id: int,\n    sync_run_id: int,\n    payload_hash: str,\n    payload_json: dict[str, Any],\n    normalized_json: dict[str, Any],\n    source_updated_at: datetime | None,\n) -> ExternalEntityVersion:\n    existing = session.scalar(\n        select(ExternalEntityVersion).where(\n            ExternalEntityVersion.organization_id == organization_id,\n            ExternalEntityVersion.external_entity_id == external_entity_id,\n            ExternalEntityVersion.payload_hash == payload_hash,\n        )\n    )\n    if existing is not None:\n        return existing\n    row = ExternalEntityVersion(\n        organization_id=organization_id,\n        external_entity_id=external_entity_id,\n        sync_run_id=sync_run_id,\n        payload_hash=payload_hash,\n        payload_json=payload_json,\n        normalized_json=normalized_json,\n        source_updated_at=source_updated_at,\n    )\n    session.add(row)\n    return row\n\n''',
        label="history reuse",
    )

    text = regex(
        text,
        r"(    def create_connection\(\n.*?        actor_user_id: int \| None = None,\n    \) -> )IntegrationConnection:",
        r"\1ConnectionWriteResult:",
        label="create result annotation",
    )
    text = exact(
        text,
        '''            _event(\n                self.session,\n                organization_id=self.organization_id,\n                event_type="CONNECTION_CREATED",\n                connection_id=connection.id,\n                actor_user_id=actor_user_id,\n                metadata={"connector_type": normalized_type},\n            )\n            self.session.commit()\n            return connection''',
        '''            _event(\n                self.session,\n                organization_id=self.organization_id,\n                event_type="CONNECTION_CREATED",\n                connection_id=connection.id,\n                actor_user_id=actor_user_id,\n                metadata={"connector_type": normalized_type},\n            )\n            result = ConnectionWriteResult(\n                public_id=connection.public_id,\n                name=connection.name,\n                connector_type=connection.connector_type,\n                status=connection.status,\n            )\n            self.session.commit()\n            return result''',
        label="create materialization",
    )

    text = regex(
        text,
        r"(    def set_connection_status\(\n.*?        actor_user_id: int \| None = None,\n    \) -> )IntegrationConnection:",
        r"\1ConnectionWriteResult:",
        label="status result annotation",
    )
    text = exact(
        text,
        '''        try:\n            self.session.commit()\n            return connection\n        except SQLAlchemyError as exc:\n            self.session.rollback()\n            raise IntegrationPersistenceError(\n                "No fue posible actualizar la conexión."\n            ) from exc''',
        '''        try:\n            result = ConnectionWriteResult(\n                public_id=connection.public_id,\n                name=connection.name,\n                connector_type=connection.connector_type,\n                status=connection.status,\n            )\n            self.session.commit()\n            return result\n        except SQLAlchemyError as exc:\n            self.session.rollback()\n            raise IntegrationPersistenceError(\n                "No fue posible actualizar la conexión."\n            ) from exc''',
        label="status materialization",
    )

    text = exact(
        text,
        '''                previous_hash = existing.payload_hash\n                was_reconciled = existing.status == "RECONCILED"\n                _version(''',
        '''                previous_hash = existing.payload_hash\n                has_reference = self.session.scalar(\n                    select(ExternalReference.id).where(\n                        ExternalReference.organization_id == self.organization_id,\n                        ExternalReference.external_entity_id == existing.id,\n                    )\n                ) is not None\n                reconciliation_protected = (\n                    existing.status in {"RECONCILED", "CONFLICT"} or has_reference\n                )\n                _version(''',
        label="conflict protection query",
    )
    text = exact(
        text,
        "                if was_reconciled:\n",
        "                if reconciliation_protected:\n",
        label="conflict protection branch",
    )

    text = exact(
        text,
        '''            run.records_conflict = conflict\n            run.status = "PARTIAL" if conflict else "SUCCEEDED"\n            run.finished_at = _now()\n            _event(''',
        '''            run.records_conflict = conflict\n            run.status = "PARTIAL" if conflict else "SUCCEEDED"\n            run.finished_at = _now()\n            self.session.flush()\n            result = SyncResult(\n                public_id=run.public_id,\n                status=run.status,\n                records_seen=run.records_seen,\n                records_created=created,\n                records_updated=updated,\n                records_unchanged=unchanged,\n                records_conflict=conflict,\n            )\n            _event(''',
        label="sync materialization",
    )
    text = exact(
        text,
        '''        return SyncResult(\n            public_id=run.public_id,\n            status=run.status,\n            records_seen=run.records_seen,\n            records_created=created,\n            records_updated=updated,\n            records_unchanged=unchanged,\n            records_conflict=conflict,\n        )\n\n    def reconcile_entity(''',
        '''        return result\n\n    def reconcile_entity(''',
        label="sync post-commit return",
    )

    text = regex(
        text,
        r"(    def reconcile_entity\(\n.*?        user_id: int \| None,\n    \) -> )ExternalReference:",
        r"\1ReconciliationWriteResult:",
        label="reconciliation result annotation",
    )
    text = exact(
        text,
        '''        try:\n            self.session.commit()\n            return reference\n        except SQLAlchemyError as exc:\n            self.session.rollback()\n            raise IntegrationPersistenceError(\n                "No fue posible guardar la reconciliación."\n            ) from exc''',
        '''        try:\n            self.session.flush()\n            result = ReconciliationWriteResult(\n                public_id=reference.public_id,\n                target_type=reference.target_type,\n                target_reference=reference.target_reference,\n            )\n            self.session.commit()\n            return result\n        except SQLAlchemyError as exc:\n            self.session.rollback()\n            raise IntegrationPersistenceError(\n                "No fue posible guardar la reconciliación."\n            ) from exc''',
        label="reconciliation materialization",
    )

    write(path, text)


def patch_web_actor() -> None:
    path = "src/litoral_trace/web/integrations.py"
    text = read(path)
    text = exact(
        text,
        '''            secret_ref=str(form.get("secret_ref", "")) or None,\n            config_json={"mode": "staging_only"},\n        )''',
        '''            secret_ref=str(form.get("secret_ref", "")) or None,\n            config_json={"mode": "staging_only"},\n            actor_user_id=user.user_id,\n        )''',
        label="web connection actor",
    )
    text = exact(
        text,
        '''            payload=payload,\n            idempotency_key=str(form.get("idempotency_key", "")),\n        )''',
        '''            payload=payload,\n            idempotency_key=str(form.get("idempotency_key", "")),\n            actor_user_id=user.user_id,\n        )''',
        label="web sync actor",
    )
    write(path, text)


def patch_tests() -> None:
    path = "tests/test_p1a_integration_core_unittest.py"
    text = read(path)
    text = exact(
        text,
        '''    assert exc.value.code == "SENSITIVE_CONFIG_REJECTED"\n\n    with pytest.raises(IntegrationValidationError) as exc:\n        _validate_secret_ref''',
        '''    assert exc.value.code == "SENSITIVE_CONFIG_REJECTED"\n\n    with pytest.raises(IntegrationValidationError) as exc:\n        _validate_config({"auth": {"credentials": [{"api_key": "nested-secret"}]}})\n    assert exc.value.code == "SENSITIVE_CONFIG_REJECTED"\n\n    with pytest.raises(IntegrationValidationError) as exc:\n        _validate_secret_ref''',
        label="nested secret regression",
    )
    text = exact(
        text,
        '''    assert "ExternalEntityVersion" in source\n''',
        '''    assert "ExternalEntityVersion" in source\n    assert 'existing.status in {"RECONCILED", "CONFLICT"} or has_reference' in source\n    assert "ConnectionWriteResult" in source\n    assert "ReconciliationWriteResult" in source\n''',
        label="review invariant assertions",
    )
    text = exact(
        text,
        '''    assert "actor_user_id=user.user_id" in source\n''',
        '''    assert "actor_user_id=user.user_id" in source\n    web_source = Path("src/litoral_trace/web/integrations.py").read_text(encoding="utf-8")\n    assert web_source.count("actor_user_id=user.user_id") >= 2\n''',
        label="web actor assertion",
    )
    write(path, text)


def patch_workflow_heads() -> None:
    for path in (
        ".github/workflows/release-integration-gates.yml",
        ".github/workflows/ux10g-postgres-web-stabilization-gate.yml",
        ".github/workflows/v1-final-release-acceptance.yml",
    ):
        text = read(path)
        if "020_add_traceability_evidence_links" not in text:
            raise RuntimeError(f"{path}: old canonical head not found")
        write(path, text.replace("020_add_traceability_evidence_links", "022_add_integration_history"))


def main() -> None:
    patch_core()
    patch_web_actor()
    patch_tests()
    patch_workflow_heads()
    print("P1A_REVIEW_FIXES_V2_APPLIED")


if __name__ == "__main__":
    main()
