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

    python_job = workflow.split("python-tests:", 1)[1].split("frontend-build:", 1)[0]
    frontend_job = workflow.split("frontend-build:", 1)[1].split("production-build:", 1)[0]
    production_job = workflow.split("production-build:", 1)[1]

    assert "if: github.event_name == 'workflow_dispatch'" not in python_job
    assert "runs-on: ubuntu-latest" in python_job
    assert "timeout-minutes: 20" in python_job
    assert "if: github.event_name == 'workflow_dispatch'" in frontend_job
    assert "if: github.event_name == 'workflow_dispatch'" in production_job


def test_required_pr_ci_always_reports_and_push_can_ignore_docs_only_changes() -> None:
    workflow = _read("ci.yml")
    before_push, after_push = workflow.split("\n  push:\n", 1)
    push_block = after_push.split("\n  workflow_dispatch:", 1)[0]

    # A branch-required PR check must not be suppressed by workflow-level path
    # filters, otherwise GitHub can leave it indefinitely in Expected/Pending.
    assert "pull_request:" in before_push
    assert "paths-ignore:" not in before_push

    # Push CI is not a branch-required PR report, so docs-only pushes may still
    # be ignored to conserve private-repository Actions minutes.
    assert "paths-ignore:" in push_block
    for ignored_path in (
        '- "**/*.md"',
        '- "commercial/**"',
        '- "docs/**"',
        '- ".github/ISSUE_TEMPLATE/**"',
    ):
        assert ignored_path in push_block

    assert "concurrency:" in workflow
    assert "cancel-in-progress: true" in workflow


def test_logical_backup_runs_at_most_once_per_day_automatically() -> None:
    workflow = _read("postgres-logical-backup.yml")
    cron_lines = [
        line.strip()
        for line in workflow.splitlines()
        if line.strip().startswith("- cron:")
    ]
    assert cron_lines == ['- cron: "17 3 * * *"']
    assert "workflow_dispatch:" in workflow
