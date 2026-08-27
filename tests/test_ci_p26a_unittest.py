from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "ci.yml"
RUNBOOK_PATH = ROOT / "DEPLOYMENT_RUNBOOK.md"


def _workflow_text() -> str:
    return WORKFLOW_PATH.read_text(encoding="utf-8")


def _runbook_text() -> str:
    return RUNBOOK_PATH.read_text(encoding="utf-8")


def test_p26a_ci_workflow_exists():
    assert WORKFLOW_PATH.exists()


def test_p26a_ci_uses_read_only_permissions():
    workflow = _workflow_text()
    assert "permissions:" in workflow
    assert "contents: read" in workflow
    assert "contents: write" not in workflow
    assert "pull-requests: write" not in workflow
    assert "id-token: write" not in workflow


def test_p26a_ci_runs_on_pull_requests_and_enterprise_branch():
    workflow = _workflow_text()
    assert "pull_request:" in workflow
    assert "push:" in workflow
    assert "- p2-enterprise-production" in workflow
    assert "workflow_dispatch:" in workflow


def test_p26a_ci_python_job_uses_production_runtime_version():
    workflow = _workflow_text()
    assert "python-tests:" in workflow
    assert 'python-version: "3.11"' in workflow


def test_p26a_ci_runs_pytest_and_disables_postgres_integration():
    workflow = _workflow_text()
    python_job = workflow.split("python-tests:", 1)[1].split("frontend-build:", 1)[0]
    assert 'ENABLE_POSTGRES_TESTS: "0"' in workflow
    assert "python -m pytest -q -rs" in workflow
    assert "\n      DATABASE_URL:" not in python_job
    assert "\n      MIGRATION_DATABASE_URL:" not in python_job
    assert "\n      WORKER_DATABASE_URL:" not in python_job
    assert "TEST_POSTGRES_DATABASE_URL" not in workflow
    assert "TEST_POSTGRES_MIGRATION_DATABASE_URL" not in workflow
    assert "secrets." not in workflow


def test_p26a_ci_checks_single_canonical_alembic_head():
    workflow = _workflow_text()
    assert "alembic heads" in workflow
    assert "032_assurance_operational_exceptions (head)" in workflow
    assert "alembic upgrade head" not in workflow


def test_p26a_ci_frontend_is_reproducible():
    workflow = _workflow_text()
    assert "frontend-build:" in workflow
    assert "npm ci" in workflow
    assert "npm run build" in workflow
    assert "npm audit --audit-level=high" in workflow
    assert "git diff --exit-code" in workflow


def test_p26a_ci_validates_docker_and_compose():
    workflow = _workflow_text()
    assert "production-build:" in workflow
    assert "docker build -f Dockerfile -t litoral-trace-ci ." in workflow
    assert "docker compose -f docker-compose.prod.yml config --quiet" in workflow
    assert "docker compose up" not in workflow
    assert "alembic upgrade" not in workflow


def test_p26a_ci_does_not_deploy():
    workflow = _workflow_text()
    forbidden = (
        "ssh ",
        "scp ",
        "deploy_production.sh",
        "docker push",
        "gh release",
        "actions/upload-artifact",
        "environment:",
    )
    for token in forbidden:
        assert token not in workflow


def test_p26a_runbook_documents_ci_release_gate():
    runbook = _runbook_text()
    assert "6A. CI release gate" in runbook
    assert "GitHub CI must be green." in runbook
    assert "Python tests must be green." in runbook
    assert "Alembic single canonical head check must pass." in runbook
    assert "frontend build must be reproducible." in runbook
    assert "npm high-severity audit must pass." in runbook
    assert "production Docker image must build." in runbook
    assert "Production Compose configuration validation must pass." in runbook
    assert "P2.6A does not deploy automatically." in runbook
