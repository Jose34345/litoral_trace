import unittest
import os
from pathlib import Path

class TestDeployPhase3(unittest.TestCase):
    def setUp(self):
        self.base_dir = Path(__file__).resolve().parents[1]

    def test_dockerfile_fastapi_uvicorn_exists(self):
        dockerfile = self.base_dir / "Dockerfile"
        self.assertTrue(dockerfile.exists())
        content = dockerfile.read_text(encoding="utf-8")
        self.assertIn("uvicorn", content)
        self.assertIn("8000", content)

    def test_docker_compose_prod_exists(self):
        compose_file = self.base_dir / "docker-compose.prod.yml"
        self.assertTrue(compose_file.exists())
        content = compose_file.read_text(encoding="utf-8")
        self.assertIn("postgis", content)
        self.assertIn("8000", content)

    def test_nginx_conf_exists(self):
        nginx_file = self.base_dir / "nginx" / "nginx.conf"
        self.assertTrue(nginx_file.exists())
        content = nginx_file.read_text(encoding="utf-8")
        self.assertIn("litoraltrace.com", content)
        self.assertIn("app:8000", content)

    def test_deploy_script_exists_and_executable(self):
        deploy_script = self.base_dir / "deploy_production.sh"
        self.assertTrue(deploy_script.exists())
        self.assertTrue(os.access(deploy_script, os.X_OK))

if __name__ == "__main__":
    unittest.main()
