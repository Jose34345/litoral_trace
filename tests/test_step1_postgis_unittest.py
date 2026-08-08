import unittest
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

class TestStep1PostGIS(unittest.TestCase):
    def test_database_url_resolution(self):
        os.environ["DATABASE_URL"] = "postgres://user:pass@host:5432/db"
        from litoral_trace.db.engine import get_database_url
        url = get_database_url()
        self.assertEqual(url, "postgresql+psycopg://user:pass@host:5432/db")

    def test_alembic_script_exists(self):
        migration_file = Path(__file__).resolve().parents[1] / "alembic" / "versions" / "001_initial_postgis_schema.py"
        self.assertTrue(migration_file.exists())
        content = migration_file.read_text(encoding="utf-8")
        self.assertIn("postgis", content)
        self.assertIn("organizations", content)

if __name__ == "__main__":
    unittest.main()
