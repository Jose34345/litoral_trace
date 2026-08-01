"""Pruebas unitarias del esquema completo de base de datos y aislamiento multi-tenant."""
from __future__ import annotations
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from litoral_trace.db.base import Base
from litoral_trace.db.models import Organization, User, Lote, AuditLog, ApiKey, License
from litoral_trace.db.tenant import apply_tenant_filter, verify_tenant_access

@pytest.fixture
def db_engine():
    """Crea una base de datos SQLite en memoria para validar el esquema completo."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)

def test_creacion_esquema_completo(db_engine):
    """Valida la creación de todas las tablas e índices del esquema."""
    with Session(db_engine) as session:
        # Crear Organización
        org1 = Organization(name="Aserradero San José", slug="aserradero-san-jose", tax_id="30-71234567-8", tier="enterprise")
        org2 = Organization(name="Carbonera del Chaco", slug="carbonera-chaco", tax_id="30-79876543-2", tier="pro")
        session.add_all([org1, org2])
        session.commit()

        # Crear Usuarios
        user1 = User(organization_id=org1.id, email="admin@sanjose.com", username="admin_sanjose", password_hash="hash123", role="admin")
        user2 = User(organization_id=org2.id, email="auditor@carbonera.com", username="auditor_chaco", password_hash="hash456", role="auditor")
        session.add_all([user1, user2])
        session.commit()

        # Crear Lotes
        lote1 = Lote(
            organization_id=org1.id,
            identificador="RODAL-NORTE-01",
            productor_id="20-33444555-9",
            producto_forestal="Madera Aserrada (Pino)",
            hectareas=120.5,
            latitud=-27.45,
            longitud=-58.90,
            polygon_wkt="POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))",
            estatus="Verde",
            volumen_ingresado_ton=500.0,
            volumen_exportar_ton=225.0
        )
        lote2 = Lote(
            organization_id=org2.id,
            identificador="LOTE-CARBON-02",
            productor_id="20-11223344-5",
            producto_forestal="Extracto de Quebracho (Tanino)",
            hectareas=85.0,
            latitud=-26.80,
            longitud=-60.40,
            estatus="Pendiente",
            volumen_ingresado_ton=300.0,
            volumen_exportar_ton=90.0
        )
        session.add_all([lote1, lote2])
        session.commit()

        # Validar persistencia y relaciones
        saved_org = session.execute(select(Organization).where(Organization.slug == "aserradero-san-jose")).scalar_one()
        assert len(saved_org.users) == 1
        assert len(saved_org.lotes) == 1
        assert saved_org.users[0].username == "admin_sanjose"
        assert saved_org.lotes[0].identificador == "RODAL-NORTE-01"

def test_aislamiento_multi_tenant(db_engine):
    """Valida el filtrado estricto de consultas por organización."""
    with Session(db_engine) as session:
        org1 = Organization(name="Empresa A", slug="org-a")
        org2 = Organization(name="Empresa B", slug="org-b")
        session.add_all([org1, org2])
        session.commit()

        lote_a = Lote(organization_id=org1.id, identificador="LOTE-A", productor_id="P-A", producto_forestal="Pino", latitud=-27.0, longitud=-58.0)
        lote_b = Lote(organization_id=org2.id, identificador="LOTE-B", productor_id="P-B", producto_forestal="Eucalipto", latitud=-27.1, longitud=-58.1)
        session.add_all([lote_a, lote_b])
        session.commit()

        # Filtrar consulta para Org 1
        q_a = apply_tenant_filter(select(Lote), Lote, organization_id=org1.id)
        lotes_org1 = session.execute(q_a).scalars().all()
        assert len(lotes_org1) == 1
        assert lotes_org1[0].identificador == "LOTE-A"

        # Verificar acceso
        assert verify_tenant_access(lote_a, organization_id=org1.id) is True
        assert verify_tenant_access(lote_a, organization_id=org2.id) is False
