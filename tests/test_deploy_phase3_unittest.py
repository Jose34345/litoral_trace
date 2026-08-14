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
        self.assertTrue(path.exists(), f"Missing required file: {relative_path}")
        return path.read_text(encoding="utf-8")

    @staticmethod
    def _service_block(compose_text: str, service_name: str) -> str:
        lines = compose_text.splitlines()
        marker = f"  {service_name}:"
        start = None

        for index, line in enumerate(lines):
            if line == marker:
                start = index
                break

        if start is None:
            raise AssertionError(f"Service not found: {service_name}")

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

        # A shared image must not impose an API-specific healthcheck on worker.
        self.assertNotIn("HEALTHCHECK", content)

    def test_docker_compose_contains_expected_services(self):
        content = self._read("docker-compose.prod.yml")

        for service_name in ("app", "worker", "db", "proxy"):
            self.assertIn(f"  {service_name}:", content)

        self.assertIn("postgis/postgis", content)

    def test_app_service_uses_runtime_db_only_and_ready_healthcheck(self):
        content = self._read("docker-compose.prod.yml")
        app = self._service_block(content, "app")

        self.assertIn("DATABASE_URL:", app)
        self.assertNotIn("WORKER_DATABASE_URL", app)
        self.assertNotIn("MIGRATION_DATABASE_URL", app)
        self.assertNotIn("POSTGRES_URL:", app)
        self.assertNotIn("DB_URL:", app)

        self.assertIn("127.0.0.1:8000/ready", app)
        self.assertIn('expose:', app)
        self.assertIn('"8000"', app)

    def test_worker_service_uses_dedicated_capability_and_non_destructive_check(self):
        content = self._read("docker-compose.prod.yml")
        worker = self._service_block(content, "worker")

        self.assertIn("DATABASE_URL:", worker)
        self.assertIn("WORKER_DATABASE_URL:", worker)
        self.assertNotIn("MIGRATION_DATABASE_URL", worker)

        self.assertIn("litoral_trace.workers.satellite_worker", worker)
        self.assertIn('"--check"', worker)

        self.assertIn("SATELLITE_METRICS_ENABLED:", worker)
        self.assertIn("SATELLITE_METRICS_HOST:", worker)
        self.assertIn("SATELLITE_METRICS_PORT:", worker)
        self.assertIn("SATELLITE_QUEUE_METRICS_REFRESH_SECONDS:", worker)

        self.assertIn('expose:', worker)
        self.assertIn('"9108"', worker)

        # Metrics are internal-only in F3; no host publication.
        self.assertNotRegex(worker, r'(?m)^\s*ports:\s*$')
        self.assertNotIn("9108:9108", worker)

    def test_compose_contains_no_hardcoded_legacy_database_credentials(self):
        content = self._read("docker-compose.prod.yml")

        forbidden_literals = (
            "litoral_secure_pass",
            "postgresql+psycopg://litoral_user:",
            "postgresql://litoral_user:",
        )
        for literal in forbidden_literals:
            self.assertNotIn(literal, content)

        # Database bootstrap values must come from environment interpolation.
        self.assertIn("${POSTGRES_USER:", content)
        self.assertIn("${POSTGRES_PASSWORD:", content)
        self.assertIn("${POSTGRES_DB:", content)

    def test_proxy_depends_on_healthy_app(self):
        content = self._read("docker-compose.prod.yml")
        proxy = self._service_block(content, "proxy")

        self.assertIn("depends_on:", proxy)
        self.assertIn("app:", proxy)
        self.assertIn("condition: service_healthy", proxy)

    def test_nginx_conf_exists_and_routes_to_internal_app(self):
        content = self._read("nginx/nginx.conf")

        self.assertIn("litoraltrace.com", content)
        self.assertIn("app:8000", content)

    def test_deploy_script_exists_and_executable(self):
        deploy_script = self.base_dir / "deploy_production.sh"

        self.assertTrue(deploy_script.exists())
        self.assertTrue(os.access(deploy_script, os.X_OK))

    def test_no_worker_metrics_host_port_publication(self):
        content = self._read("docker-compose.prod.yml")

        self.assertNotIn('"9108:9108"', content)
        self.assertNotIn("'9108:9108'", content)

        # Also reject an unquoted host mapping such as "- 9108:9108".
        self.assertIsNone(
            re.search(r"(?m)^\s*-\s*9108:9108\s*$", content)
        )


if __name__ == "__main__":
    unittest.main()