
"""Agregar geometría PostGIS real a lotes.

Revision ID: 004_add_postgis_geometry_to_lotes
Revises: 003_add_api_audit_license_tables
Create Date: 2026-08-04
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "004_add_postgis_geometry_to_lotes"
down_revision: Union[str, Sequence[str], None] = (
    "003_add_api_audit_license_tables"
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Agrega geometría PostGIS Polygon/SRID 4326 y migra WKT existente."""

    conn = op.get_bind()

    # ============================================================
    # 0. Ampliar el campo interno de Alembic.
    #
    # La tabla alembic_version actualmente utiliza VARCHAR(32),
    # pero el revision ID de esta migración supera los 32 caracteres.
    # ============================================================

    op.execute(
        """
        ALTER TABLE alembic_version
        ALTER COLUMN version_num TYPE VARCHAR(64)
        """
    )

    # ============================================================
    # 1. Verificar que estamos utilizando PostgreSQL.
    # ============================================================

    if conn.dialect.name != "postgresql":
        return

    # ============================================================
    # 2. Asegurar que PostGIS esté disponible.
    # ============================================================

    op.execute(
        "CREATE EXTENSION IF NOT EXISTS postgis;"
    )

    # ============================================================
    # 3. Crear columna geométrica PostGIS.
    #
    # Polygon + SRID 4326:
    # - Polygon: geometría de cada lote.
    # - 4326: WGS84, coordenadas latitud/longitud.
    # ============================================================

    op.execute(
        """
        ALTER TABLE lotes
        ADD COLUMN geom geometry(Polygon, 4326)
        """
    )

    # ============================================================
    # 4. Migrar polygon_wkt -> geom.
    #
    # Se ignoran valores NULL y strings vacíos.
    # ============================================================

    op.execute(
        """
        UPDATE lotes
        SET geom = ST_SetSRID(
            ST_GeomFromText(polygon_wkt),
            4326
        )
        WHERE polygon_wkt IS NOT NULL
          AND TRIM(polygon_wkt) <> ''
        """
    )

    # ============================================================
    # 5. Validar las geometrías migradas.
    # ============================================================

    invalid_count = conn.execute(
        sa.text(
            """
            SELECT COUNT(*)
            FROM lotes
            WHERE geom IS NOT NULL
              AND NOT ST_IsValid(geom)
            """
        )
    ).scalar()

    if invalid_count:
        raise RuntimeError(
            f"Se encontraron {invalid_count} geometrías inválidas "
            "durante la migración de polygon_wkt a geom."
        )

    # ============================================================
    # 6. Crear índice espacial GiST.
    #
    # Mejora las consultas espaciales PostGIS.
    # ============================================================

    op.execute(
        """
        CREATE INDEX ix_lotes_geom_gist
        ON lotes
        USING GIST (geom)
        """
    )


def downgrade() -> None:
    """Elimina la geometría PostGIS y su índice espacial."""

    conn = op.get_bind()

    if conn.dialect.name != "postgresql":
        return

    # ============================================================
    # 1. Eliminar índice espacial.
    # ============================================================

    op.execute(
        """
        DROP INDEX IF EXISTS ix_lotes_geom_gist
        """
    )

    # ============================================================
    # 2. Eliminar columna geom.
    # ============================================================

    op.execute(
        """
        ALTER TABLE lotes
        DROP COLUMN IF EXISTS geom
        """
    )
