import unittest
from litoral_trace.services.gee import generate_geometry_hash, ALGORITHM_VERSION
from litoral_trace.services.cache import build_ndvi_cache_key
from litoral_trace.services.ndvi import evaluar_indicador_variacion_biomasa

class TestStep2SatelliteUnit(unittest.TestCase):
    def test_geometry_hash_deterministic(self):
        wkt = "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))"
        hash1 = generate_geometry_hash(wkt)
        hash2 = generate_geometry_hash(wkt)
        self.assertEqual(hash1, hash2)
        self.assertEqual(len(hash1), 64)

    def test_cache_key_formatting(self):
        key = build_ndvi_cache_key(
            org_id=1,
            lote_id=101,
            geometry_hash="abc123hash",
            start_date="2020-12-31",
            end_date="2026-08-01",
            cloud_threshold=20.0,
            algorithm_version=ALGORITHM_VERSION
        )
        self.assertEqual(key, f"ndvi:v1:1:101:abc123hash:2020-12-31:2026-08-01:20.0:{ALGORITHM_VERSION}")

    def test_honest_eudr_indicator_stable(self):
        obs = [
            {"observation_date": "2020-06-15", "ndvi_mean": 0.60},
            {"observation_date": "2020-12-15", "ndvi_mean": 0.62},
            {"observation_date": "2025-06-15", "ndvi_mean": 0.61},
            {"observation_date": "2025-12-15", "ndvi_mean": 0.60},
        ]
        res = evaluar_indicador_variacion_biomasa(obs)
        self.assertEqual(res["status"], "SUCCESS")
        self.assertEqual(res["vegetation_change_indicator"], "ESTABLE_SIN_CAMBIOS_SIGNIFICATIVOS")
        self.assertEqual(res["eudr_vegetation_risk_indicator"], "BAJO_RIESGO_VEGETACIONAL")
        self.assertIn("disclaimer", res)

    def test_honest_eudr_indicator_drop(self):
        obs = [
            {"observation_date": "2020-06-15", "ndvi_mean": 0.70},
            {"observation_date": "2020-12-15", "ndvi_mean": 0.70},
            {"observation_date": "2025-06-15", "ndvi_mean": 0.30},
            {"observation_date": "2025-12-15", "ndvi_mean": 0.30},
        ]
        res = evaluar_indicador_variacion_biomasa(obs)
        self.assertEqual(res["status"], "SUCCESS")
        self.assertEqual(res["vegetation_change_indicator"], "CAIDA_SIGNIFICATIVA_DE_BIOMASA")
        self.assertEqual(res["eudr_vegetation_risk_indicator"], "ALTA_VARIACION_REQUIERE_EVALUAR_COBERTURA")

if __name__ == "__main__":
    unittest.main()
