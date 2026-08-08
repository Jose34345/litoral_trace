import unittest
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from litoral_trace.services.gee import initialize_earth_engine, consultar_serie_temporal_ndvi_gee

class TestStep2EarthEngineReal(unittest.TestCase):
    def test_real_gee_connection_conditional(self):
        """Test de conexión REAL contra Google Earth Engine (se ejecuta solo si hay credenciales GCP)."""
        gee_ready, msg, ms = initialize_earth_engine()
        print(f"\n[GEE Real Test Init Status]: Connected={gee_ready} | Detail={msg} | Time={ms}ms")
        
        if not gee_ready:
            print("[GEE Real Test Skipped]: No se detectaron credenciales GCP Service Account ni ADC en el entorno.")
            self.skipTest("Google Earth Engine no está autenticado en este entorno de prueba.")

        polygon_wkt_nea = "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))"
        result = consultar_serie_temporal_ndvi_gee(
            polygon_wkt=polygon_wkt_nea,
            start_date="2020-12-31",
            max_cloud_pct=20.0
        )
        self.assertEqual(result["status"], "success")
        self.assertTrue(result["gee_connected"])
        self.assertGreater(result["total_observations"], 0)

if __name__ == "__main__":
    unittest.main()
