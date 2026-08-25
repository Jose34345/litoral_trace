from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"

MANUAL_ONLY_WORKFLOWS = (
    "p17k-nonsuperuser-migration-gate.yml",
    "p1a-integration-core-postgres-gate.yml",
    "p1b-export-case-postgres-gate.yml",
    "p1c-phytosanitary-postgres-gate.yml",
    "p1d-eudr-conformance-postgres-gate.yml",
    "p1d2-eudr-acceptance-postgres-gate.yml",
    "p1d3-eudr-acceptance-readiness-gate.yml",
    "p1e-origin-dossier-gate.yml",
    "p1f-pilot-readiness-postgres-gate.yml",
    "release-integration-gates.yml",
    "ux10-dashboard-gate.yml",
    "ux10-language-gate.yml",
    "ux10c-lineage-graph-gate.yml",
    "ux10d-chain-of-custody-operations-gate.yml",
    "ux10e-contextual-evidence-gate.yml",
    "ux10f-release-control-gate.yml",
    "ux10g-postgres-web-stabilization-gate.yml",
    "v1-final-release-acceptance.yml",
    "v1-satellite-browser-staging-e2e.yml",
)


def _read(name: str) -> str:
    return (WORKFLOWS / name).read_text(encoding="utf-8")


def test_expensive_specialized_gates_are_manual_only() -> None:
    for name in MANUAL_ONLY_WORKFLOWS:
        workflow = _read(name)
        assert "workflow_dispatch:" in workflow, name
        assert "pull_request:" not in workflow, name
        assert "\n  push:" not in workflow, name
        assert "\n  schedule:" not in workflow, name


def test_ordinary_ci_uses_one_automatic_runner_job() -> None:
    workflow = _read("ci.yml")
    assert "pull_request:" in workflow
    assert "paths-ignore:" in workflow
    assert 'if: github.event_name == \'workflow_dispatch\'' in workflow.split(
        "frontend-build:", 1
    )[1].split("production-build:", 1)[0]
    assert 'if: github.event_name == \'workflow_dispatch\'' in workflow.split(
        "production-build:", 1
    )[1]
    assert "if: github.event_name == 'workflow_dispatch'" not in workflow.split(
        "python-tests:", 1
    )[1].split("frontend-build:", 1)[0]


def test_logical_backup_runs_at_most_once_per_day_automatically() -> None:
    workflow = _read("postgres-logical-backup.yml")
    cron_lines = [
        line.strip()
        for line in workflow.splitlines()
        if line.strip().startswith("- cron:")
    ]
    assert cron_lines == ['- cron: "17 3 * * *"']
    assert "workflow_dispatch:" in workflow
