import os
import sys
from pathlib import Path

# Ensure the source package directory is importable from pytest.
ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
TEST_DB_PATH = ROOT_DIR / "litoral_trace_test.db"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Force a deterministic test environment and strip inherited application DB URLs.
os.environ["ENVIRONMENT"] = "test"
os.environ.setdefault(
    "TEST_DATABASE_URL",
    f"sqlite:///{TEST_DB_PATH.as_posix()}",
)
os.environ.setdefault(
    "JWT_SECRET_KEY",
    "test-only-jwt-secret-key-1234567890",
)

for variable_name in ("DATABASE_URL", "POSTGRES_URL", "DB_URL"):
    os.environ.pop(variable_name, None)


def pytest_configure(config):
    """Prepare a local SQLite schema and seed superadmin data for integration-style tests."""
    try:
        from litoral_trace.db.engine import reset_engine_state
        from litoral_trace.db.init_db import inicializar_base_datos_postgis

        reset_engine_state()

        if TEST_DB_PATH.exists():
            TEST_DB_PATH.unlink()

        inicializar_base_datos_postgis()
    except Exception as exc:
        raise RuntimeError(
            f"Could not initialize test database in '{ROOT_DIR}': {exc}"
        ) from exc
