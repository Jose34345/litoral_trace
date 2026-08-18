from __future__ import annotations

import os
import re
import unittest
from pathlib import Path


class TestDeployPhase3(unittest.TestCase):
    def setUp(self):
        self.base_dir = Path(__file__).resolve().parents[1]

    def _read(self, relative_path: str) -> str:
        path = self.base_dir / relative_path
        self.assertTrue(
            path.exists(),
            f"Missing required file: {relative_path}",
        )
        return path.read_text(encoding="utf-8")

    @staticmethod
    def _service_block(
        compose_text: str,
        service_name: str,
    ) -> str:
        lines = compose_text.splitlines()
        marker = f"  {service_name}:"
        start = None

        for index, line in enumerate(lines):
            if line == marker:
                start = index
                break

        if start is None:
            raise AssertionError(
                f"Service not found: {service_name}"
            )

        collected = [lines[start]]
        for line in lines[start + 1 :]:
            if (
                line.startswith("  ")
                and not line.startswith("    ")
                and line.endswith(":")
            ):
                break
            collected.append(line)

        return "\n".join(collected)

    def test_dockerfile_supports_api_and_worker_from_same_image(self):
        content = self._read("Dockerfile")

        self.assertIn("uvicorn", content)
        self.assertIn("main:app", content)
        self.assertIn("PYTHONPATH=/app/src", content)
        self.assertIn("EXPOSE 8000 9108", content)
        self.assertIn(
            'CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]',
            content,
        )
        self.assertNotIn(
            'CMD [\n    "uvicorn",',
            content,
        )

        self.assertNotIn("HEALTHCHECK", content)

    def test_docker_compose_contains_expected_services(self):
        content = self._read(
            "docker-compose.prod.yml"
        )

        for service_name in (
            "app",
            "worker",
            "db",
            "proxy",
        ):
            self.assertIn(
                f"  {service_name}:",
                content,
            )

        self.assertIn(
            "postgis/postgis",
            content,
        )

    def test_app_service_uses_runtime_db_only_and_ready_healthcheck(self):
        content = self._read(
            "docker-compose.prod.yml"
        )
        app = self._service_block(
            content,
            "app",
        )

        self.assertIn("DATABASE_URL:", app)
        self.assertNotIn(
            "WORKER_DATABASE_URL",
            app,
        )
        self.assertNotIn(
            "MIGRATION_DATABASE_URL",
            app,
        )
        self.assertNotIn(
            "POSTGRES_URL:",
            app,
        )
        self.assertNotIn(
            "DB_URL:",
            app,
        )

        self.assertIn(
            "127.0.0.1:8000/ready",
            app,
        )
        self.assertIn("expose:", app)
        self.assertIn('"8000"', app)

    def test_app_service_wires_complete_private_vault_storage_contract(self):
        content = self._read(
            "docker-compose.prod.yml"
        )
        app = self._service_block(
            content,
            "app",
        )

        required_names = (
            "STORAGE_BACKEND",
            "STORAGE_BUCKET_NAME",
            "STORAGE_REGION",
            "STORAGE_ENDPOINT_URL",
            "STORAGE_ACCESS_KEY_ID",
            "STORAGE_SECRET_ACCESS_KEY",
            "STORAGE_SESSION_TOKEN",
            "STORAGE_FORCE_PATH_STYLE",
            "STORAGE_USE_TLS",
            "STORAGE_VERIFY_TLS",
            "STORAGE_CA_BUNDLE_PATH",
            "STORAGE_CONNECT_TIMEOUT_SECONDS",
            "STORAGE_READ_TIMEOUT_SECONDS",
            "STORAGE_MAX_RETRIES",
            "STORAGE_KEY_PREFIX",
            "STORAGE_MAX_UPLOAD_BYTES",
            "STORAGE_ALLOWED_CONTENT_TYPES",
        )

        for variable_name in required_names:
            self.assertIn(
                f"{variable_name}:",
                app,
            )

        self.assertIn(
            "${STORAGE_BACKEND:?",
            app,
        )
        self.assertIn(
            "${STORAGE_BUCKET_NAME:?",
            app,
        )
        self.assertIn(
            "${STORAGE_USE_TLS:-1}",
            app,
        )
        self.assertIn(
            "${STORAGE_VERIFY_TLS:-1}",
            app,
        )

    def test_worker_does_not_receive_vault_storage_credentials(self):
        content = self._read(
            "docker-compose.prod.yml"
        )
        worker = self._service_block(
            content,
            "worker",
        )

        for variable_name in (
            "STORAGE_BACKEND",
            "STORAGE_BUCKET_NAME",
            "STORAGE_ACCESS_KEY_ID",
            "STORAGE_SECRET_ACCESS_KEY",
            "STORAGE_SESSION_TOKEN",
        ):
            self.assertNotIn(
                variable_name,
                worker,
            )

    def test_worker_service_uses_dedicated_capability_and_non_destructive_check(self):
        content = self._read(
            "docker-compose.prod.yml"
        )
        worker = self._service_block(
            content,
            "worker",
        )

        self.assertIn("DATABASE_URL:", worker)
        self.assertIn(
            "WORKER_DATABASE_URL:",
            worker,
        )
        self.assertNotIn(
            "MIGRATION_DATABASE_URL",
            worker,
        )

        self.assertIn(
            "litoral_trace.workers.satellite_worker",
            worker,
        )
        self.assertIn(
            '"--check"',
            worker,
        )

        self.assertIn(
            "SATELLITE_METRICS_ENABLED:",
            worker,
        )
        self.assertIn(
            "SATELLITE_METRICS_HOST:",
            worker,
        )
        self.assertIn(
            "SATELLITE_METRICS_PORT:",
            worker,
        )
        self.assertIn(
            "SATELLITE_QUEUE_METRICS_REFRESH_SECONDS:",
            worker,
        )

        self.assertIn("expose:", worker)
        self.assertIn('"9108"', worker)

        self.assertNotRegex(
            worker,
            r"(?m)^\s*ports:\s*$",
        )
        self.assertNotIn(
            "9108:9108",
            worker,
        )

    def test_compose_contains_no_hardcoded_legacy_database_credentials(self):
        content = self._read(
            "docker-compose.prod.yml"
        )

        forbidden_literals = (
            "litoral_secure_pass",
            "postgresql+psycopg://litoral_user:",
            "postgresql://litoral_user:",
        )
        for literal in forbidden_literals:
            self.assertNotIn(
                literal,
                content,
            )

        self.assertIn(
            "${POSTGRES_USER:",
            content,
        )
        self.assertIn(
            "${POSTGRES_PASSWORD:",
            content,
        )
        self.assertIn(
            "${POSTGRES_DB:",
            content,
        )

    def test_proxy_depends_on_healthy_app(self):
        content = self._read(
            "docker-compose.prod.yml"
        )
        proxy = self._service_block(
            content,
            "proxy",
        )

        self.assertIn(
            "depends_on:",
            proxy,
        )
        self.assertIn("app:", proxy)
        self.assertIn(
            "condition: service_healthy",
            proxy,
        )

    def test_nginx_conf_exists_and_routes_to_internal_app(self):
        content = self._read(
            "nginx/nginx.conf"
        )

        self.assertIn(
            "litoraltrace.com",
            content,
        )
        self.assertIn(
            "app:8000",
            content,
        )

    def test_deploy_script_exists_and_executable(self):
        deploy_script = (
            self.base_dir
            / "deploy_production.sh"
        )

        self.assertTrue(
            deploy_script.exists()
        )
        self.assertTrue(
            os.access(
                deploy_script,
                os.X_OK,
            )
        )

    def test_deploy_script_checks_vault_before_migration_and_after_start(self):
        content = self._read(
            "deploy_production.sh"
        )

        readiness_command = (
            "python -m "
            "litoral_trace.storage.readiness"
        )
        migration_command = (
            "python -m alembic upgrade head"
        )

        self.assertGreaterEqual(
            content.count(
                readiness_command
            ),
            2,
        )

        first_readiness = content.index(
            readiness_command
        )
        migration = content.index(
            migration_command
        )

        self.assertLess(
            first_readiness,
            migration,
        )
        self.assertIn(
            "Verifying Vault storage readiness "
            "from inside the API container",
            content,
        )

    def test_deploy_script_enforces_pre_migration_recovery_gate_before_alembic(
        self,
    ):
        content = self._read(
            "deploy_production.sh"
        )

        for variable_name in (
            "PRE_MIGRATION_RECOVERY_MANIFEST",
            "PRE_MIGRATION_RECOVERY_COMPLETE",
            "PRE_MIGRATION_SOURCE_RELEASE_COMMIT",
            "PRE_MIGRATION_OPERATOR",
            "PRE_MIGRATION_TARGET_ENV",
            "PRE_MIGRATION_MAX_AGE_MINUTES",
        ):
            self.assertIn(
                variable_name,
                content,
            )

        self.assertIn(
            "python -m scripts.pre_migration_recovery_gate",
            content,
        )
        self.assertIn(
            "/run/litoral-recovery/manifest.json:ro",
            content,
        )
        self.assertIn(
            "/run/litoral-recovery/complete.json:ro",
            content,
        )

        gate_position = content.index(
            "python -m scripts.pre_migration_recovery_gate"
        )
        migration_position = content.index(
            "python -m alembic upgrade head"
        )

        self.assertLess(
            gate_position,
            migration_position,
        )

        self.assertGreaterEqual(
            content.count(
                "-e MIGRATION_DATABASE_URL"
            ),
            2,
        )
        self.assertNotIn(
            '-e MIGRATION_DATABASE_URL="$MIGRATION_DATABASE_URL"',
            content,
        )


    def test_runbook_documents_private_bucket_iam_and_fail_closed_readiness(self):
        content = self._read(
            "DEPLOYMENT_RUNBOOK.md"
        )

        for required_text in (
            "s3:ListBucket",
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject",
            "STORAGE_USE_TLS=1",
            "STORAGE_VERIFY_TLS=1",
            "production Vault storage is unconfigured",
            "python -m litoral_trace.storage.readiness",
        ):
            self.assertIn(
                required_text,
                content,
            )

        self.assertIn(
            "application runtime can create buckets",
            content,
        )

    def test_no_worker_metrics_host_port_publication(self):
        content = self._read(
            "docker-compose.prod.yml"
        )

        self.assertNotIn(
            '"9108:9108"',
            content,
        )
        self.assertNotIn(
            "'9108:9108'",
            content,
        )

        self.assertIsNone(
            re.search(
                r"(?m)^\s*-\s*9108:9108\s*$",
                content,
            )
        )


if __name__ == "__main__":
    unittest.main()
