from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = (
    ROOT
    / ".github"
    / "workflows"
    / "postgres-logical-backup.yml"
)
CI_WORKFLOW_PATH = (
    ROOT / ".github" / "workflows" / "ci.yml"
)


def _workflow_text() -> str:
    return WORKFLOW_PATH.read_text(encoding="utf-8")


def _ci_text() -> str:
    return CI_WORKFLOW_PATH.read_text(encoding="utf-8")


def test_p27a3c2_workflow_exists():
    assert WORKFLOW_PATH.exists()


def test_p27a3c2_trigger_and_cadence_are_operator_only():
    workflow = _workflow_text()

    assert "name: PostgreSQL Logical Backup" in workflow
    assert "schedule:" in workflow
    assert 'cron: "17 3 * * *"' in workflow
    assert "workflow_dispatch:" in workflow
    assert "\n  push:" not in workflow
    assert "\n  pull_request:" not in workflow
    assert "\n  pull_request_target:" not in workflow


def test_p27a3c2_permissions_match_oidc_minimum():
    workflow = _workflow_text()

    assert "permissions:" in workflow
    assert "contents: read" in workflow
    assert "id-token: write" in workflow
    for forbidden in (
        "contents: write",
        "actions: write",
        "packages: write",
        "issues: write",
        "pull-requests: write",
        "deployments: write",
        "security-events: write",
    ):
        assert forbidden not in workflow


def test_p27a3c2_environment_and_concurrency_are_isolated():
    workflow = _workflow_text()

    assert "environment: production-backup" in workflow
    assert "concurrency:" in workflow
    assert "group: postgres-logical-backup-production" in workflow
    assert "cancel-in-progress: false" in workflow


def test_p27a3c2_runtime_uses_python_311_and_timeout():
    workflow = _workflow_text()

    assert "runs-on: ubuntu-latest" in workflow
    assert 'python-version: "3.11"' in workflow
    assert "timeout-minutes: 30" in workflow


def test_p27a3c2_explicitly_installs_and_verifies_postgresql_17_client():
    workflow = _workflow_text()

    assert "postgresql-client-17" in workflow
    assert "pg_dump --version" in workflow
    assert "pg_restore --version" in workflow
    assert 'grep -E "17\\."' in workflow


def test_p27a3c2_uses_minimal_operator_dependencies_not_requirements_txt():
    workflow = _workflow_text()

    assert 'python -m pip install "psycopg[binary]" boto3' in workflow
    assert "pip install -r requirements.txt" not in workflow


def test_p27a3c2_uses_environment_secrets_and_variables_without_hardcoding():
    workflow = _workflow_text()

    assert "BACKUP_DATABASE_URL: ${{ secrets.BACKUP_DATABASE_URL }}" in workflow
    assert "BACKUP_AWS_ROLE_ARN: ${{ vars.BACKUP_AWS_ROLE_ARN }}" in workflow
    assert "BACKUP_S3_BUCKET: ${{ vars.BACKUP_S3_BUCKET }}" in workflow
    assert "BACKUP_S3_PREFIX: ${{ vars.BACKUP_S3_PREFIX }}" in workflow
    assert "BACKUP_S3_REGION: ${{ vars.BACKUP_S3_REGION }}" in workflow
    assert "BACKUP_RELEASE_COMMIT: ${{ vars.BACKUP_RELEASE_COMMIT }}" in workflow
    assert "894f5d3" not in workflow
    assert "postgresql://" not in workflow
    assert "postgresql+psycopg://" not in workflow
    assert "secrets.AWS_ACCESS_KEY_ID" not in workflow
    assert "secrets.AWS_SECRET_ACCESS_KEY" not in workflow
    assert "secrets.AWS_SESSION_TOKEN" not in workflow
    assert "BACKUP_S3_ENDPOINT_URL" not in workflow


def test_p27a3c2_validates_required_config_without_printing_values():
    workflow = _workflow_text()

    for token in (
        "Missing required configuration: $name",
        "BACKUP_DATABASE_URL",
        "BACKUP_S3_BUCKET",
        "BACKUP_S3_PREFIX",
        "BACKUP_S3_REGION",
        "BACKUP_RELEASE_COMMIT",
        "BACKUP_AWS_ROLE_ARN",
    ):
        assert token in workflow


def test_p27a3c2_uses_private_runner_temp_workspace_and_umask():
    workflow = _workflow_text()

    # `runner` is not an allowed context in jobs.<job_id>.env. Initialize the
    # private path after the runner exists and persist it through GITHUB_ENV.
    assert "BACKUP_WORKDIR: ${{ runner.temp }}" not in workflow
    assert "Initialize backup workdir" in workflow
    assert (
        'echo "BACKUP_WORKDIR=$RUNNER_TEMP/litoral-trace-postgres-backup" '
        '>> "$GITHUB_ENV"'
    ) in workflow
    assert 'mkdir -p "$BACKUP_WORKDIR"' in workflow
    assert "umask 077" in workflow
    assert "backups/postgres" not in workflow


def test_p27a3c2_runs_backup_then_oidc_then_publisher_and_derives_paths_from_backup_json():
    workflow = _workflow_text()

    assert "python scripts/postgres_logical_backup.py" in workflow
    assert "--source-label production" in workflow
    assert '--release-commit "$BACKUP_RELEASE_COMMIT"' in workflow
    assert '> "$BACKUP_WORKDIR/backup-result.json"' in workflow
    assert 'payload["dump_filename"]' in workflow
    assert 'payload["manifest_filename"]' in workflow
    assert 'write_text(str(dump_path)' in workflow
    assert 'write_text(str(manifest_path)' in workflow
    assert "aws-actions/configure-aws-credentials@" in workflow
    assert "role-to-assume: ${{ vars.BACKUP_AWS_ROLE_ARN }}" in workflow
    assert "aws-region: ${{ vars.BACKUP_S3_REGION }}" in workflow
    assert "role-session-name: litoral-trace-postgres-backup" in workflow
    assert "python -m scripts.postgres_backup_publish" in workflow
    assert '--dump-file "$DUMP_PATH"' in workflow
    assert '--manifest "$MANIFEST_PATH"' in workflow

    backup_index = workflow.index(
        "Create logical backup"
    )
    oidc_index = workflow.index(
        "Configure AWS credentials via OIDC"
    )
    publish_index = workflow.index(
        "Publish logical backup"
    )
    assert backup_index < oidc_index < publish_index


def test_p27a3c2_disallows_ci_artifacts_remote_delete_and_deploy_commands():
    workflow = _workflow_text()

    forbidden = (
        "actions/upload-artifact",
        "continue-on-error: true",
        "aws s3 rm",
        "aws s3 cp",
        "aws s3 sync",
        "DeleteObject",
        "deploy_production.sh",
        "docker push",
        "kubectl",
        "helm ",
    )
    for token in forbidden:
        assert token not in workflow


def test_p27a3c2_cleanup_is_always_run_and_scoped_to_fixed_runner_temp_directory():
    workflow = _workflow_text()

    assert "if: always()" in workflow
    assert 'rm -rf "$RUNNER_TEMP/litoral-trace-postgres-backup"' in workflow
    assert 'rm -rf "$GITHUB_WORKSPACE"' not in workflow
    assert 'rm -rf "$HOME"' not in workflow


def test_p27a3c2_workflow_avoids_secret_spilling_commands():
    workflow = _workflow_text()

    for forbidden in (
        "echo $BACKUP_DATABASE_URL",
        "\nenv\n",
        "\nprintenv\n",
        "set -x",
    ):
        assert forbidden not in workflow


def test_p27a3c2_pins_actions_to_immutable_shas_and_records_default_branch_activation_gap():
    workflow = _workflow_text()

    assert "# actions/checkout@v4" in workflow
    assert "uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262" in workflow
    assert "# actions/setup-python@v5" in workflow
    assert "uses: actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065" in workflow
    assert "# aws-actions/configure-aws-credentials@v5" in workflow
    assert "uses: aws-actions/configure-aws-credentials@61815dcd50bd041e203e49132bacad1fd04d2708" in workflow
    assert "actions/checkout@v4" not in workflow.replace(
        "# actions/checkout@v4", ""
    )
    assert "actions/setup-python@v5" not in workflow.replace(
        "# actions/setup-python@v5", ""
    )
    assert "Scheduled execution is operational only when this workflow exists on the repository default branch." in workflow
    assert "workflow_dispatch also requires this workflow file to exist on the default branch." in workflow
    assert "Default-branch activation is an operational release step and is not performed by this workflow." in workflow


def test_p27a3c2_ci_workflow_remains_separate_from_backup_runtime():
    ci = _ci_text()

    assert "name: CI" in ci
    assert "postgres-logical-backup-production" not in ci
    assert "production-backup" not in ci
    assert "postgresql-client-17" not in ci
