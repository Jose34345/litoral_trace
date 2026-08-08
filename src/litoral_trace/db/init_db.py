"""Inicializador de Esquema de Base de Datos PostgreSQL / PostGIS y Seeding de SuperAdmin.

WARNING: Alembic es el mecanismo oficial para migrar producciÃ³n.
Este mÃ³dulo solo debe usarse en desarrollo/local.
"""
from __future__ import annotations
import os
from sqlalchemy import text
from sqlalchemy.orm import Session

from litoral_trace.config import get_settings
from litoral_trace.db.base import Base
from litoral_trace.db.engine import get_engine, get_session_factory
from litoral_trace.db.models import License, Lote, Organization, User
from litoral_trace.auth.passwords import hash_password


DEVELOPMENT_SUPERADMIN_USERNAME = "admin"
NON_PRODUCTION_SUPERADMIN_PASSWORD_ENV_VAR = (
    "LITORAL_TRACE_BOOTSTRAP_SUPERADMIN_PASSWORD"
)


def _is_production_environment() -> bool:
    return get_settings().is_production


def _get_non_production_superadmin_password() -> str:
    password = os.getenv(NON_PRODUCTION_SUPERADMIN_PASSWORD_ENV_VAR, "").strip()
    if not password:
        raise RuntimeError(
            "Debe definir la variable de entorno "
            f"{NON_PRODUCTION_SUPERADMIN_PASSWORD_ENV_VAR} "
            "para inicializar el superadmin no productivo."
        )
    return password


def get_non_production_superadmin_seed() -> tuple[str, str]:
    """Return the bootstrap superadmin credential reserved for local/dev/test.

    Production must provision or rotate privileged credentials out of band.
    """
    if _is_production_environment():
        raise RuntimeError(
            "El seed inicial de superadmin no debe utilizarse en produccion."
        )

    return DEVELOPMENT_SUPERADMIN_USERNAME, _get_non_production_superadmin_password()


def inicializar_base_datos_postgis() -> None:
    """Inicializa la extensiÃ³n PostGIS, crea todas las tablas e inyecta la organizaciÃ³n SuperAdmin inicial."""
    if _is_production_environment():
        raise RuntimeError(
            "init_db.py no debe ejecutarse en producciÃ³n. Use Alembic para aplicar migraciones."
        )

    engine = get_engine()
    
    # 1. Habilitar extensiÃ³n PostGIS en PostgreSQL (se ignora en SQLite)
    if engine.dialect.name == "postgresql":
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
        except Exception as e:
            print(f"Aviso PostGIS: {e}")

    # 2. Crear todas las tablas definidas en los modelos
    Base.metadata.create_all(bind=engine)

    # 3. Seeding de la OrganizaciÃ³n SuperAdmin inicial
    admin_username, admin_password = get_non_production_superadmin_seed()
    session_factory = get_session_factory()
    with session_factory() as session: # type: Session
        # Verificar si ya existe la organizaciÃ³n SuperAdmin
        admin_org = session.query(Organization).filter_by(slug="exp-chaco").first()
        if not admin_org:
            admin_org = Organization(
                name="Exportadora Forestal del Chaco S.A.",
                slug="exp-chaco",
                tax_id="30-55555555-9",
                tier="enterprise",
                description="OrganizaciÃ³n SuperAdmin de GestiÃ³n B2B Litoral Trace",
                is_active=True
            )
            session.add(admin_org)
            session.commit()
            session.refresh(admin_org)

        # Verificar si existe el usuario admin de plataforma
        admin_user = session.query(User).filter_by(username=admin_username).first()
        if not admin_user:
            admin_user = User(
                organization_id=admin_org.id,
                email="comercial@litoraltrace.com",
                username=admin_username,
                password_hash=hash_password(admin_password),
                role="superadmin",
                full_name="JosÃ© David Lezcano (Fundador)",
                is_active=True
            )
            session.add(admin_user)

        # Verificar si existe la licencia Enterprise
        admin_lic = session.query(License).filter_by(organization_id=admin_org.id).first()
        if not admin_lic:
            admin_lic = License(
                organization_id=admin_org.id,
                plan_type="enterprise",
                max_lotes=1000,
                max_volume_tons=50000.0,
                max_batch_rows=2000,
                is_active=True
            )
            session.add(admin_lic)

        existing_lotes = (
            session.query(Lote)
            .filter_by(organization_id=admin_org.id)
            .count()
        )
        if existing_lotes == 0:
            session.add_all(
                [
                    Lote(
                        id=101,
                        organization_id=admin_org.id,
                        identificador="RODAL-NORTE-01",
                        productor_id="30-12345678-9",
                        producto_forestal="Madera Aserrada (Pino)",
                        hectareas=120.5,
                        latitud=-27.45,
                        longitud=-58.90,
                        polygon_wkt=(
                            "POLYGON(("
                            "-58.91 -27.46, -58.89 -27.46, "
                            "-58.89 -27.44, -58.91 -27.44, "
                            "-58.91 -27.46"
                            "))"
                        ),
                        estatus="Verde",
                        volumen_ingresado_ton=500.0,
                        volumen_exportar_ton=225.0,
                    ),
                    Lote(
                        id=102,
                        organization_id=admin_org.id,
                        identificador="RODAL-SUR-02",
                        productor_id="30-12345678-9",
                        producto_forestal="Madera Aserrada (Eucalipto)",
                        hectareas=85.0,
                        latitud=-27.52,
                        longitud=-58.97,
                        polygon_wkt=(
                            "POLYGON(("
                            "-58.98 -27.53, -58.96 -27.53, "
                            "-58.96 -27.51, -58.98 -27.51, "
                            "-58.98 -27.53"
                            "))"
                        ),
                        estatus="Pendiente",
                        volumen_ingresado_ton=300.0,
                        volumen_exportar_ton=120.0,
                    ),
                ]
            )

        session.commit()

if __name__ == "__main__":
    inicializar_base_datos_postgis()
    print("âœ… Base de Datos PostgreSQL/PostGIS inicializada y sembrada con Ã©xito.")

