import unittest
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

class TestStep1PostGIS(unittest.TestCase):
    def test_database_url_resolution(self):
        old_environment = os.environ.get("ENVIRONMENT")
        os.environ["DATABASE_URL"] = "postgres://user:pass@host:5432/db"
        os.environ["ENVIRONMENT"] = "development"
        from litoral_trace.db.engine import get_database_url
        try:
            url = get_database_url()
            self.assertEqual(url, "postgresql+psycopg://user:pass@host:5432/db")
        finally:
            if old_environment is None:
                os.environ.pop("ENVIRONMENT", None)
            else:
                os.environ["ENVIRONMENT"] = old_environment

    def test_test_environment_ignores_application_database_url(self):
        old_environment = os.environ.get("ENVIRONMENT")
        old_database_url = os.environ.get("DATABASE_URL")
        old_test_database_url = os.environ.get("TEST_DATABASE_URL")

        os.environ["ENVIRONMENT"] = "test"
        os.environ["DATABASE_URL"] = "postgres://user:pass@host:5432/db"
        os.environ["TEST_DATABASE_URL"] = "sqlite:///controlled_test.db"

        from litoral_trace.db.engine import get_database_url

        try:
            url = get_database_url()
            self.assertEqual(url, "sqlite:///controlled_test.db")
        finally:
            if old_environment is None:
                os.environ.pop("ENVIRONMENT", None)
            else:
                os.environ["ENVIRONMENT"] = old_environment

            if old_database_url is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = old_database_url

            if old_test_database_url is None:
                os.environ.pop("TEST_DATABASE_URL", None)
            else:
                os.environ["TEST_DATABASE_URL"] = old_test_database_url

    def test_alembic_script_exists(self):
        migration_file = Path(__file__).resolve().parents[1] / "alembic" / "versions" / "001_initial_postgis_schema.py"
        self.assertTrue(migration_file.exists())
        content = migration_file.read_text(encoding="utf-8")
        self.assertIn("postgis", content)
        self.assertIn("organizations", content)

if __name__ == "__main__":
    unittest.main()
