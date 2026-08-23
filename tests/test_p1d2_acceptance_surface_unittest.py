from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
API = (ROOT / "src/litoral_trace/api/eudr_acceptance.py").read_text(encoding="utf-8")
SUBMISSION = (ROOT / "src/litoral_trace/services/eudr_acceptance_submission.py").read_text(encoding="utf-8")
MIGRATION = (ROOT / "alembic/versions/026_add_eudr_acceptance_attempts.py").read_text(encoding="utf-8")
WORKFLOW = (ROOT / ".github/workflows/p1d2-eudr-acceptance-postgres-gate.yml").read_text(encoding="utf-8")


def test_public_api_has_no_live_or_force_retry_surface() -> None:
    assert 'prefix="/api/v1/eudr-acceptance"' in API
    assert "allow_retry_after_transport_error=False" in API
    assert "allow_retry_after_transport_error=True" not in API
    assert "force_retry" not in API.lower()
    assert "LIVE" not in API.replace('"live_submission_performed": False', "")


def test_submission_fail_closed_on_ambiguous_delivery_and_confidentiality() -> None:
    assert "ACCEPTANCE_DELIVERY_UNCERTAIN" in SUBMISSION
    assert "ACCEPTANCE_RETRY_REQUIRES_EXPLICIT_OVERRIDE" in SUBMISSION
    assert "ACCEPTANCE_GEOLOCATION_CONFIDENTIALITY_NOT_SUPPORTED" in SUBMISSION
    assert "geo_location_confidential=False" in SUBMISSION


def test_migration_is_acceptance_only_and_runtime_has_no_delete() -> None:
    assert 'down_revision: Union[str, Sequence[str], None] = "025_add_eudr_dds_candidates"' in MIGRATION
    assert "environment = 'ACCEPTANCE'" in MIGRATION
    assert "GRANT SELECT, INSERT, UPDATE ON TABLE" in MIGRATION
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE" not in MIGRATION
    assert "REVOKE ALL PRIVILEGES ON TABLE" in MIGRATION


def test_p1d2_ci_cannot_use_real_acceptance_credentials() -> None:
    assert 'EUDR_ACCEPTANCE_ENABLED: "false"' in WORKFLOW
    assert "EUDR_ACCEPTANCE_AUTHENTICATION_KEY:" not in WORKFLOW
    assert "EUDR_ACCEPTANCE_USERNAME:" not in WORKFLOW
    assert "EUDR_ACCEPTANCE_WEB_SERVICE_CLIENT_ID:" not in WORKFLOW
