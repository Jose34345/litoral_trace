import os
import sys
from pathlib import Path

# Ensure the source package directory is importable from pytest.
ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Default test environment to avoid production DB behavior during local test runs.
os.environ.setdefault("ENVIRONMENT", "test")


def pytest_configure(config):
    """Prepare a local SQLite schema and seed superadmin data for integration-style tests."""
    try:
        from litoral_trace.db.init_db import inicializar_base_datos_postgis
        inicializar_base_datos_postgis()
    except Exception as exc:
        raise RuntimeError(
            f"Could not initialize test database in '{ROOT_DIR}': {exc}"
        ) from exc
