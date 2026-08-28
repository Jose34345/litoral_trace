from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_staging_compose_uses_dedicated_resources_and_no_owner_in_long_lived_app():
    compose = (ROOT / "docker-compose.assurance-pilot-staging.yml").read_text(
        encoding="utf-8"
    )
    app_block = compose.split("\n  migrate:", 1)[0]
    assert "ENVIRONMENT: staging" in app_block
    assert "DATABASE_URL: ${ASSURANCE_PILOT_DATABASE_URL:" in app_block
    assert "STORAGE_BUCKET_NAME: ${ASSURANCE_PILOT_STORAGE_BUCKET_NAME:" in app_block
    assert 'LT_ASSURANCE_PILOT_MODE: "1"' in app_block
    assert "LT_ASSURANCE_PILOT_ORGANIZATION_ID" in app_block
    assert "MIGRATION_DATABASE_URL" not in app_block
    assert '"127.0.0.1:${ASSURANCE_PILOT_PORT:-8001}:8000"' in app_block
    assert '"--no-access-log"' in app_block
    assert 'EUDR_ACCEPTANCE_ENABLED: "0"' in app_block
    assert 'WORKERS_ENABLED: "0"' in app_block


def test_real_pilot_config_and_env_are_git_ignored_but_examples_are_versioned():
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
    assert "!.env.assurance-pilot-staging.example" in gitignore
    assert "/pilot/assurance-pilot.json" in gitignore
    assert (ROOT / ".env.assurance-pilot-staging.example").is_file()
    assert (ROOT / "pilot" / "assurance-pilot.example.json").is_file()


def test_runbook_explicitly_requires_anonymization_no_erp_and_no_process_change():
    runbook = (ROOT / "ASSURANCE_PILOT_RUNBOOK.md").read_text(encoding="utf-8")
    assert "Replay histórico sin ERP" in runbook
    assert "Procedimiento de anonimización" in runbook
    assert "No exigir cambio de proceso" in runbook
    assert "no demuestra por sí sola que exista un staging persistente" in runbook
    assert "no cargue al staging el mapa real↔seudónimo" in runbook
