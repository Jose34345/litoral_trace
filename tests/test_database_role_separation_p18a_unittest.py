import os
import unittest

from litoral_trace.config import get_settings
from litoral_trace.config.settings import (
    resolve_migration_database_url,
    resolve_runtime_database_url,
)
from litoral_trace.db.engine import get_database_url, reset_engine_state


class TestDatabaseRoleSeparationP18A(unittest.TestCase):
    def setUp(self):
        self._original_env = {
            variable_name: os.environ.get(variable_name)
            for variable_name in (
                "ENVIRONMENT",
                "DATABASE_URL",
                "MIGRATION_DATABASE_URL",
                "POSTGRES_URL",
                "DB_URL",
                "TEST_DATABASE_URL",
                "TEST_POSTGRES_DATABASE_URL",
                "ENABLE_POSTGRES_TESTS",
            )
        }

    def tearDown(self):
        for variable_name, original_value in self._original_env.items():
            if original_value is None:
                os.environ.pop(variable_name, None)
            else:
                os.environ[variable_name] = original_value
        reset_engine_state()

    def test_runtime_engine_uses_database_url_not_migration_database_url(self):
        os.environ["ENVIRONMENT"] = "development"
        os.environ["DATABASE_URL"] = "postgres://runtime_user:runtime-secret@host:5432/appdb"
        os.environ["MIGRATION_DATABASE_URL"] = "postgresql://owner_user:owner-secret@host:5432/appdb"

        self.assertEqual(
            resolve_runtime_database_url(),
            "postgresql+psycopg://runtime_user:runtime-secret@host:5432/appdb",
        )
        self.assertEqual(
            get_database_url(),
            "postgresql+psycopg://runtime_user:runtime-secret@host:5432/appdb",
        )

    def test_migration_resolver_prefers_migration_database_url(self):
        os.environ["ENVIRONMENT"] = "development"
        os.environ["DATABASE_URL"] = "postgres://runtime_user:runtime-secret@host:5432/appdb"
        os.environ["MIGRATION_DATABASE_URL"] = "postgres://owner_user:owner-secret@host:5432/appdb"

        self.assertEqual(
            resolve_migration_database_url(),
            "postgresql+psycopg://owner_user:owner-secret@host:5432/appdb",
        )

    def test_migration_resolver_falls_back_to_runtime_database_url(self):
        os.environ["ENVIRONMENT"] = "development"
        os.environ["DATABASE_URL"] = "postgres://runtime_user:runtime-secret@host:5432/appdb"
        os.environ.pop("MIGRATION_DATABASE_URL", None)

        self.assertEqual(
            resolve_migration_database_url(),
            "postgresql+psycopg://runtime_user:runtime-secret@host:5432/appdb",
        )

    def test_test_environment_ignores_runtime_and_migration_urls(self):
        os.environ["ENVIRONMENT"] = "test"
        os.environ["DATABASE_URL"] = "postgres://runtime_user:runtime-secret@host:5432/appdb"
        os.environ["MIGRATION_DATABASE_URL"] = "postgres://owner_user:owner-secret@host:5432/appdb"
        os.environ["TEST_DATABASE_URL"] = "sqlite:///isolated_test.db"

        self.assertEqual(get_database_url(), "sqlite:///isolated_test.db")

    def test_settings_repr_does_not_expose_database_secrets(self):
        os.environ["ENVIRONMENT"] = "development"
        os.environ["DATABASE_URL"] = "postgres://runtime_user:runtime-secret@host:5432/appdb"
        os.environ["MIGRATION_DATABASE_URL"] = "postgres://owner_user:owner-secret@host:5432/appdb"

        settings = get_settings()
        settings_repr = repr(settings)
        database_repr = repr(settings.database)

        self.assertNotIn("runtime-secret", settings_repr)
        self.assertNotIn("owner-secret", settings_repr)
        self.assertNotIn("runtime-secret", database_repr)
        self.assertNotIn("owner-secret", database_repr)


if __name__ == "__main__":
    unittest.main()
