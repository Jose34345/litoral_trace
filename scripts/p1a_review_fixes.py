"""Apply the four accepted PR #89 review fixes deterministically."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: str, old: str, new: str) -> None:
    target = ROOT / path
    text = target.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected 1 match, found {count}: {old[:80]!r}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


def patch_core() -> None:
    path = "src/litoral_trace/services/integrations/core.py"

    replace_once(
        path,
        '''class IntegrationPersistenceError(IntegrationError):
    pass


@dataclass(frozen=True)
class SyncResult:''',
        '''class IntegrationPersistenceError(IntegrationError):
    pass


@dataclass(frozen=True)
class ConnectionWriteResult:
    public_id: UUID
    name: str
    connector_type: str
    status: str


@dataclass(frozen=True)
class ReconciliationWriteResult:
    public_id: UUID
    target_type: str
    target_reference: str


@dataclass(frozen=True)
class SyncResult:''',
    )

    replace_once(
        path,
        '''def _validate_config(config: dict[str, Any] | None) -> dict[str, Any] | None:
    if not config:
        return None
    serialized_keys = " ".join(str(key).lower() for key in config.keys())
    if any(fragment in serialized_keys for fragment in _FORBIDDEN_CONFIG_FRAGMENTS):
        raise IntegrationValidationError(
            "SENSITIVE_CONFIG_REJECTED",
            "La configuración no puede contener secretos; use secret_ref.",
        )
    encoded = json.dumps(config, sort_keys=True, default=str)
    if len(encoded.encode("utf-8")) > 32_768:
        raise IntegrationValidationError(
            "CONFIG_TOO_LARGE",
            "La configuración de integración supera el máximo permitido.",
        )
    return config
''',
        '''def _contains_forbidden_config_key(value: Any) -> bool:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized_key = str(key).lower()
            if any(fragment in normalized_key for fragment in _FORBIDDEN_CONFIG_FRAGMENTS):
                return True
            if _contains_forbidden_config_key(child):
                return True
        return False
    if isinstance(value, (list, tuple)):
        return any(_contains_forbidden_config_key(child) for child in value)
    return False


def _validate_config(config: dict[str, Any] | None) -> dict[str, Any] | None:
    if not config:
        return None
    if _contains_forbidden_config_key(config):
        raise IntegrationValidationError(
            "SENSITIVE_CONFIG_REJECTED",
            "La configuración no puede contener secretos; use secret_ref.",
        )
    encoded = json.dumps(config, sort_keys=True, default=str)
    if len(encoded.encode("utf-8")) > 32_768:
        raise IntegrationValidationError(
            "CONFIG_TOO_LARGE",
            "La configuración de integración supera el máximo permitido.",
        )
    return config
''',
    )

    replace_once(
        path,
        '''def _version(
    session: Session,
    *,
    organization_id: int,
    external_entity_id: int,
    sync_run_id: int,
    payload_hash: str,
    payload_json: dict[str, Any],
    normalized_json: dict[str, Any],
    source_updated_at: datetime | None,
) -> ExternalEntityVersion:
    row = ExternalEntityVersion(
        organization_id=organization_id,
        external_entity_id=external_entity_id,
        sync_run_id=sync_run_id,
        payload_hash=payload_hash,
        payload_json=payload_json,
        normalized_json=normalized_json,
        source_updated_at=source_updated_at,
    )
    session.add(row)
    return row
''',
        '''def _version(
    session: Session,
    *,
    organization_id: int,
    external_entity_id: int,
    sync_run_id: int,
    payload_hash: str,
    payload_json: dict[str, Any],
    normalized_json: dict[str, Any],
    source_updated_at: datetime | None,
) -> ExternalEntityVersion:
    existing = session.scalar(
        select(ExternalEntityVersion).where(
            ExternalEntityVersion.organization_id == organization_id,
            ExternalEntityVersion.external_entity_id == external_entity_id,
            ExternalEntityVersion.payload_hash == payload_hash,
        )
    )
    if existing is not None:
        return existing
    row = ExternalEntityVersion(
        organization_id=organization_id,
        external_entity_id=external_entity_id,
        sync_run_id=sync_run_id,
        payload_hash=payload_hash,
        payload_json=payload_json,
        normalized_json=normalized_json,
        source_updated_at=source_updated_at,
    )
    session.add(row)
    return row
''',
    )

    replace_once(
        path,
        '''        actor_user_id: int | None = None,
    ) -> IntegrationConnection:''',
        '''        actor_user_id: int | None = None,
    ) -> ConnectionWriteResult:''',
    )
    replace_once(
        path,
        '''            _event(
                self.session,
                organization_id=self.organization_id,
                event_type="CONNECTION_CREATED",
                connection_id=connection.id,
                actor_user_id=actor_user_id,
                metadata={"connector_type": normalized_type},
            )
            self.session.commit()
            return connection''',
        '''            _event(
                self.session,
                organization_id=self.organization_id,
                event_type="CONNECTION_CREATED",
                connection_id=connection.id,
                actor_user_id=actor_user_id,
                metadata={"connector_type": normalized_type},
            )
            result = ConnectionWriteResult(
                public_id=connection.public_id,
                name=connection.name,
                connector_type=connection.connector_type,
                status=connection.status,
            )
            self.session.commit()
            return result''',
    )

    replace_once(
        path,
        '''        actor_user_id: int | None = None,
    ) -> IntegrationConnection:
        normalized = str(status or "").strip().upper()''',
        '''        actor_user_id: int | None = None,
    ) -> ConnectionWriteResult:
        normalized = str(status or "").strip().upper()''',
    )
    replace_once(
        path,
        '''        try:
            self.session.commit()
            return connection
        except SQLAlchemyError as exc:
            self.session.rollback()
            raise IntegrationPersistenceError(
                "No fue posible actualizar la conexión."
            ) from exc

    def stage_generic_erp(''',
        '''        try:
            result = ConnectionWriteResult(
                public_id=connection.public_id,
                name=connection.name,
                connector_type=connection.connector_type,
                status=connection.status,
            )
            self.session.commit()
            return result
        except SQLAlchemyError as exc:
            self.session.rollback()
            raise IntegrationPersistenceError(
                "No fue posible actualizar la conexión."
            ) from exc

    def stage_generic_erp(''',
    )

    replace_once(
        path,
        '''            run.records_conflict = conflict
            run.status = "PARTIAL" if conflict else "SUCCEEDED"
            run.finished_at = _now()
            _event(''',
        '''            run.records_conflict = conflict
            run.status = "PARTIAL" if conflict else "SUCCEEDED"
            run.finished_at = _now()
            self.session.flush()
            result = SyncResult(
                public_id=run.public_id,
                status=run.status,
                records_seen=run.records_seen,
                records_created=created,
                records_updated=updated,
                records_unchanged=unchanged,
                records_conflict=conflict,
            )
            _event(''',
    )
    replace_once(
        path,
        '''            self.session.commit()
        except IntegrityError as exc:''',
        '''            self.session.commit()
        except IntegrityError as exc:''',
    )
    replace_once(
        path,
        '''        return SyncResult(
            public_id=run.public_id,
            status=run.status,
            records_seen=run.records_seen,
            records_created=created,
            records_updated=updated,
            records_unchanged=unchanged,
            records_conflict=conflict,
        )

    def reconcile_entity(''',
        '''        return result

    def reconcile_entity(''',
    )

    replace_once(
        path,
        '''                previous_hash = existing.payload_hash
                was_reconciled = existing.status == "RECONCILED"
                _version(''',
        '''                previous_hash = existing.payload_hash
                has_reference = self.session.scalar(
                    select(ExternalReference.id).where(
                        ExternalReference.organization_id == self.organization_id,
                        ExternalReference.external_entity_id == existing.id,
                    )
                ) is not None
                reconciliation_protected = (
                    existing.status in {"RECONCILED", "CONFLICT"} or has_reference
                )
                _version(''',
    )
    replace_once(path, '                if was_reconciled:\n', '                if reconciliation_protected:\n')

    replace_once(
        path,
        '''        user_id: int | None,
    ) -> ExternalReference:''',
        '''        user_id: int | None,
    ) -> ReconciliationWriteResult:''',
    )
    replace_once(
        path,
        '''        try:
            self.session.commit()
            return reference
        except SQLAlchemyError as exc:
            self.session.rollback()
            raise IntegrationPersistenceError(
                "No fue posible guardar la reconciliación."
            ) from exc''',
        '''        try:
            self.session.flush()
            result = ReconciliationWriteResult(
                public_id=reference.public_id,
                target_type=reference.target_type,
                target_reference=reference.target_reference,
            )
            self.session.commit()
            return result
        except SQLAlchemyError as exc:
            self.session.rollback()
            raise IntegrationPersistenceError(
                "No fue posible guardar la reconciliación."
            ) from exc''',
    )


def patch_web_actor() -> None:
    path = "src/litoral_trace/web/integrations.py"
    replace_once(
        path,
        '''            secret_ref=str(form.get("secret_ref", "")) or None,
            config_json={"mode": "staging_only"},
        )''',
        '''            secret_ref=str(form.get("secret_ref", "")) or None,
            config_json={"mode": "staging_only"},
            actor_user_id=user.user_id,
        )''',
    )
    replace_once(
        path,
        '''            payload=payload,
            idempotency_key=str(form.get("idempotency_key", "")),
        )''',
        '''            payload=payload,
            idempotency_key=str(form.get("idempotency_key", "")),
            actor_user_id=user.user_id,
        )''',
    )


def patch_tests() -> None:
    path = "tests/test_p1a_integration_core_unittest.py"
    replace_once(
        path,
        '''    with pytest.raises(IntegrationValidationError) as exc:
        _validate_config({"password": "never-store-this"})
    assert exc.value.code == "SENSITIVE_CONFIG_REJECTED"

    with pytest.raises(IntegrationValidationError) as exc:''',
        '''    with pytest.raises(IntegrationValidationError) as exc:
        _validate_config({"password": "never-store-this"})
    assert exc.value.code == "SENSITIVE_CONFIG_REJECTED"

    with pytest.raises(IntegrationValidationError) as exc:
        _validate_config({"auth": {"credentials": [{"api_key": "nested-secret"}]}})
    assert exc.value.code == "SENSITIVE_CONFIG_REJECTED"

    with pytest.raises(IntegrationValidationError) as exc:''',
    )
    replace_once(
        path,
        '''    assert "SOURCE_CHANGED_AFTER_RECONCILIATION" in source
    assert "ExternalEntityVersion" in source
''',
        '''    assert "SOURCE_CHANGED_AFTER_RECONCILIATION" in source
    assert "ExternalEntityVersion" in source
    assert 'existing.status in {"RECONCILED", "CONFLICT"} or has_reference' in source
    assert "ConnectionWriteResult" in source
    assert "ReconciliationWriteResult" in source
''',
    )
    replace_once(
        path,
        '''    assert "actor_user_id=user.user_id" in source
''',
        '''    assert "actor_user_id=user.user_id" in source
    web_source = Path("src/litoral_trace/web/integrations.py").read_text(encoding="utf-8")
    assert web_source.count("actor_user_id=user.user_id") >= 2
''',
    )


def main() -> None:
    patch_core()
    patch_web_actor()
    patch_tests()
    print("P1A_REVIEW_FIXES_APPLIED")


if __name__ == "__main__":
    main()
