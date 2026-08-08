import unittest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from litoral_trace.db.base import Base
from litoral_trace.db.models import Organization, User, Lote, AuditLog, ApiKey, License
from litoral_trace.db.tenant import apply_tenant_filter, verify_tenant_access

class TestSchemaAndMultiTenancy(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", echo=False)
        Base.metadata.create_all(self.engine)

    def tearDown(self):
        Base.metadata.drop_all(self.engine)

    def test_creacion_esquema_completo(self):
        with Session(self.engine) as session:
            org1 = Organization(name="Aserradero San José", slug="aserradero-san-jose", tax_id="30-71234567-8", tier="enterprise")
            org2 = Organization(name="Carbonera del Chaco", slug="carbonera-chaco", tax_id="30-79876543-2", tier="pro")
            session.add_all([org1, org2])
            session.commit()

            user1 = User(organization_id=org1.id, email="admin@sanjose.com", username="admin_sanjose", password_hash="hash123", role="admin")
            user2 = User(organization_id=org2.id, email="auditor@carbonera.com", username="auditor_chaco", password_hash="hash456", role="auditor")
            session.add_all([user1, user2])
            session.commit()

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
            session.add(lote1)
            session.commit()

            saved_org = session.execute(select(Organization).where(Organization.slug == "aserradero-san-jose")).scalar_one()
            self.assertEqual(len(saved_org.users), 1)
            self.assertEqual(len(saved_org.lotes), 1)
            self.assertEqual(saved_org.users[0].username, "admin_sanjose")
            self.assertEqual(saved_org.lotes[0].identificador, "RODAL-NORTE-01")

    def test_aislamiento_multi_tenant(self):
        with Session(self.engine) as session:
            org1 = Organization(name="Empresa A", slug="org-a")
            org2 = Organization(name="Empresa B", slug="org-b")
            session.add_all([org1, org2])
            session.commit()

            lote_a = Lote(organization_id=org1.id, identificador="LOTE-A", productor_id="P-A", producto_forestal="Pino", latitud=-27.0, longitud=-58.0)
            lote_b = Lote(organization_id=org2.id, identificador="LOTE-B", productor_id="P-B", producto_forestal="Eucalipto", latitud=-27.1, longitud=-58.1)
            session.add_all([lote_a, lote_b])
            session.commit()

            q_a = apply_tenant_filter(select(Lote), Lote, organization_id=org1.id)
            lotes_org1 = session.execute(q_a).scalars().all()
            self.assertEqual(len(lotes_org1), 1)
            self.assertEqual(lotes_org1[0].identificador, "LOTE-A")

            self.assertTrue(verify_tenant_access(lote_a, organization_id=org1.id))
            self.assertFalse(verify_tenant_access(lote_a, organization_id=org2.id))

if __name__ == "__main__":
    unittest.main()
