import unittest
import asyncio
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from litoral_trace.services.gee import generate_geometry_hash, consultar_serie_temporal_ndvi_gee
from litoral_trace.api.auth import UserTenantContext
from litoral_trace.api.satellite import consultar_ndvi_satelital_lote_endpoint, SatelliteQueryByLoteRequest

class TestStep2Satellite(unittest.TestCase):
    def setUp(self):
        self.tenant_user = UserTenantContext(
            username="admin",
            organization_id=1,
            organization_name="Demo Organization",
            role="admin",
            email="admin@example.com",
        )

    def test_geometry_hash_deterministic(self):
        wkt = "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))"
        hash1 = generate_geometry_hash(wkt)
        hash2 = generate_geometry_hash(wkt)
        self.assertEqual(hash1, hash2)
        self.assertEqual(len(hash1), 64)  # SHA-256 hex length

    def test_consultar_serie_temporal_ndvi_gee_structure(self):
        res = consultar_serie_temporal_ndvi_gee(
            "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))",
            start_date="2020-12-31",
            end_date="2026-08-01",
            max_cloud_pct=20.0
        )
        self.assertEqual(res["status"], "success")
        self.assertIn("geometry_hash", res)
        self.assertGreater(res["total_observations"], 0)
        
        first_obs = res["observations"][0]
        self.assertIn("observation_date", first_obs)
        self.assertIn("ndvi_mean", first_obs)
        self.assertIn("satellite", first_obs)

    def test_consultar_ndvi_satelital_endpoint(self):
        payload = SatelliteQueryByLoteRequest(
            lote_id=101,
        )
        res = asyncio.run(consultar_ndvi_satelital_lote_endpoint(payload, user=self.tenant_user))
        self.assertEqual(res.status_code, 200)
        body = json.loads(res.body.decode('utf-8'))
        self.assertEqual(body["status"], "success")
        self.assertEqual(body["organization_id"], 1)
        self.assertIn("eudr_vegetation_analysis", body)
        self.assertEqual(body["eudr_vegetation_analysis"]["status"], "SUCCESS")
        self.assertGreater(body["total_observations"], 0)

if __name__ == "__main__":
    unittest.main()
