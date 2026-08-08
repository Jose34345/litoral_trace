from __future__ import annotations

import os
import sys
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool


# ============================================================
# Configuración Alembic
# ============================================================

config = context.config


# ============================================================
# Logging
# ============================================================

if config.config_file_name is not None:
    fileConfig(config.config_file_name)


# ============================================================
# Path del proyecto
# ============================================================

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "src",
        )
    ),
)


# ============================================================
# Metadata de SQLAlchemy
# ============================================================

from litoral_trace.db.base import Base
from litoral_trace.db.models import *  # noqa: F401,F403


target_metadata = Base.metadata


# ============================================================
# DATABASE_URL
# ============================================================

database_url = (
    os.environ.get("DATABASE_URL")
    or os.environ.get("POSTGRES_URL")
    or os.environ.get("DB_URL")
)

if not database_url:
    raise RuntimeError(
        "DATABASE_URL is not set in the environment. "
        "Configure it before running Alembic commands."
    )


if database_url.startswith("postgres://"):
    database_url = database_url.replace(
        "postgres://",
        "postgresql+psycopg://",
        1,
    )

elif database_url.startswith("postgresql://"):
    database_url = database_url.replace(
        "postgresql://",
        "postgresql+psycopg://",
        1,
    )


config.set_main_option(
    "sqlalchemy.url",
    database_url,
)


# ============================================================
# Filtro de objetos para Alembic
# ============================================================

def include_object(
    object_,
    name,
    type_,
    reflected,
    compare_to,
):
    """
    Controla qué objetos deben ser considerados por Alembic.

    Ignoramos spatial_ref_sys porque pertenece a PostGIS y no
    forma parte del modelo de datos de LitoralTrace.
    """

    if type_ == "table" and name == "spatial_ref_sys":
        return False

    return True


# ============================================================
# Migraciones OFFLINE
# ============================================================

def run_migrations_offline() -> None:
    """Run migrations in offline mode."""

    url = config.get_main_option(
        "sqlalchemy.url"
    )

    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={
            "paramstyle": "named",
        },
        compare_type=True,
        compare_server_default=False,
        include_schemas=False,
        include_object=include_object,
    )

    with context.begin_transaction():
        context.run_migrations()


# ============================================================
# Migraciones ONLINE
# ============================================================

def run_migrations_online() -> None:
    """Run migrations in online mode."""

    connectable = engine_from_config(
        config.get_section(
            config.config_ini_section
        ),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:

        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=False,
            include_schemas=False,
            include_object=include_object,
        )

        with context.begin_transaction():
            context.run_migrations()


# ============================================================
# Ejecutar
# ============================================================

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()