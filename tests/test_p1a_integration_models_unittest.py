"""P1-A ORM metadata contract."""
from litoral_trace.db.models import ExternalEntityVersion, IntegrationEvent


def test_external_versions_are_modeled_separately_from_current_entity() -> None:
    assert ExternalEntityVersion.__tablename__ == "external_entity_versions"
    assert "payload_hash" in ExternalEntityVersion.__table__.c
    assert "payload_json" in ExternalEntityVersion.__table__.c
    assert "normalized_json" in ExternalEntityVersion.__table__.c
    assert "sync_run_id" in ExternalEntityVersion.__table__.c


def test_integration_events_preserve_actor_identity_when_available() -> None:
    assert "actor_user_id" in IntegrationEvent.__table__.c
