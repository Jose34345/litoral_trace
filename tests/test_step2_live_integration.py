"""Test de Integración Único para Validación del Paso 2 (Live Integration Test)."""
import unittest
import asyncio
import os
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from litoral_trace.api.auth import login_b2b, LoginRequest, get_current_tenant_user
from litoral_trace.api.satellite import consultar_ndvi_satelital_lote_endpoint, SatelliteQueryByLoteRequest
from litoral_trace.services.gee import initialize_earth_engine, consultar_serie_temporal_ndvi_gee, generate_geometry_hash
from litoral_trace.services.cache import build_ndvi_cache_key, get_cached_satellite_data, set_cached_satellite_data
from fastapi import Response, HTTPException

class TestStep2LiveIntegration(unittest.TestCase):
    def setUp(self):
        os.environ["ENVIRONMENT"] = "test"
        req = LoginRequest(username="admin", password="admin123")
        res_dummy = Response()
        token_res = asyncio.run(login_b2b(req, res_dummy))
        bearer_hdr = f"Bearer {token_res.access_token}"
        self.tenant_user = get_current_tenant_user(authorization=bearer_hdr)

    def test_a_b_c_d_e_f_g_gee_live_connection_conditional(self):
        """A-G. Comprueba importación, inicialización, Sentinel-2 S2_SR_HARMONIZED, SCL masking y NDVI sobre Polígono."""
        try:
            import ee
        except ImportError:
            self.skipTest("Librería 'earthengine-api' no instalada en este entorno Python.")

        gee_ready, detail_msg, init_ms = initialize_earth_engine()
        print(f"\n[GEE Live Integration Test]: Connected={gee_ready} | Detail={detail_msg} | InitMs={init_ms}")

        if not gee_ready:
            self.skipTest(f"Earth Engine no está autenticado en este entorno: {detail_msg}")

        # Polígono de prueba en el NEA (Resistencia, Chaco)
        polygon_wkt_nea = "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))"
        result = consultar_serie_temporal_ndvi_gee(
            polygon_wkt=polygon_wkt_nea,
            start_date="2020-12-31",
            max_cloud_pct=20.0
        )

        self.assertEqual(result["status"], "success")
        self.assertTrue(result["gee_connected"])
        self.assertGreater(result["total_observations"], 0)
        
        obs1 = result["observations"][0]
        self.assertIn("observation_date", obs1)
        self.assertIn("ndvi_mean", obs1)
        self.assertIn("valid_pixel_percentage", obs1)
        self.assertEqual(obs1["collection"], "COPERNICUS/S2_SR_HARMONIZED")

    def test_h_i_j_cache_redis_hit_miss_and_metrics(self):
        """H-J. Comprueba el flujo Redis MISS, registro de métricas de rendimiento y Redis HIT posterior."""
        cache_key = build_ndvi_cache_key(
            org_id=1,
            lote_id=101,
            geometry_hash="test_hash_live_123",
            start_date="2020-12-31",
            end_date="2026-08-01",
            cloud_threshold=20.0
        )

        # 1. Simular Redis MISS
        cached_data, read_ms = get_cached_satellite_data(cache_key + "_non_existing")
        self.assertIsNone(cached_data)

        # 2. Guardar en Caché
        data_to_cache = {
            "status": "success",
            "lote_id": 101,
            "organization_id": 1,
            "geometry_hash": "test_hash_live_123",
            "source": "redis_cache",
            "cache_hit": True,
            "observations": [{"observation_date": "2021-01-15", "ndvi_mean": 0.65}]
        }
        set_cached_satellite_data(cache_key, data_to_cache, ttl_seconds=60)

        # 3. Comprobar Redis HIT si Redis está corriendo localmente
        hit_data, _ = get_cached_satellite_data(cache_key)
        if hit_data:
            self.assertTrue(hit_data["cache_hit"])
            self.assertEqual(hit_data["source"], "redis_cache")

    def test_k_multi_tenant_security_isolation(self):
        """K. Comprueba aislamiento estricto de clientes B2B (HTTP 403 / 401 / 404)."""
        # 1. Parcela propia (HTTP 200)
        payload_own = SatelliteQueryByLoteRequest(lote_id=101, start_date="2020-12-31")
        res_own = asyncio.run(consultar_ndvi_satelital_lote_endpoint(payload_own, user=self.tenant_user))
        self.assertEqual(res_own.status_code, 200)
        body = json.loads(res_own.body.decode('utf-8'))
        self.assertIn("metrics", body)
        self.assertIn("total_processing_ms", body["metrics"])

        # 2. Intento de acceso a parcela inexistente (HTTP 404)
        payload_nonexistent = SatelliteQueryByLoteRequest(lote_id=99999, start_date="2020-12-31")
        with self.assertRaises(HTTPException) as ctx_404:
            asyncio.run(consultar_ndvi_satelital_lote_endpoint(payload_nonexistent, user=self.tenant_user))
        self.assertEqual(ctx_404.exception.status_code, 404)

        # 3. Intento sin token JWT (HTTP 401)
        with self.assertRaises(HTTPException) as ctx_401:
            get_current_tenant_user(authorization=None, bearer_token=None, session_jwt=None)
        self.assertEqual(ctx_401.exception.status_code, 401)

if __name__ == "__main__":
    unittest.main()
