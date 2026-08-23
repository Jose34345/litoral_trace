import unittest
import json

from litoral_trace.services.mass_balance import evaluar_balance_masas
from litoral_trace.services.ndvi import calcular_ndvi_simulado, evaluar_deforestacion_eudr
from litoral_trace.services.compliance import (
    LEGACY_NON_REGULATORY_PROFILE,
    evaluar_compliance_lote,
    generar_dds_json_traces_nt,
)
from litoral_trace.services.reports import generar_pdf_reporte_bytes


class TestIntelligenceEngine(unittest.TestCase):
    def test_evaluar_balance_masas_pino_valido(self):
        res = evaluar_balance_masas(
            volumen_ingresado=500.0,
            volumen_exportar=225.0,
            tipo_cultivo="Madera Aserrada (Pino)",
        )
        self.assertTrue(res.es_valido)
        self.assertEqual(res.coeficiente_rendimiento, 0.50)
        self.assertEqual(res.volumen_maximo_permitido_ton, 250.0)

    def test_evaluar_balance_masas_pino_exceso(self):
        res = evaluar_balance_masas(
            volumen_ingresado=100.0,
            volumen_exportar=60.0,
            tipo_cultivo="Madera Aserrada (Pino)",
        )
        self.assertFalse(res.es_valido)
        self.assertIn("Alerta de Sobredeclaración", res.mensaje_observacion)

    def test_evaluar_deforestacion_eudr_verde(self):
        puntos = [
            {"fecha": "2020-06-15", "ndvi": 0.60},
            {"fecha": "2020-12-15", "ndvi": 0.62},
            {"fecha": "2025-06-15", "ndvi": 0.65},
            {"fecha": "2025-12-15", "ndvi": 0.64},
        ]
        res = evaluar_deforestacion_eudr(puntos)
        self.assertEqual(res["status"], "SUCCESS")
        self.assertEqual(res["ndvi_base_2020"], 0.61)
        self.assertAlmostEqual(res["ndvi_actual_12m"], 0.645, places=3)

    def test_evaluar_compliance_lote_is_only_non_regulatory_preview(self):
        lote = {
            "identificador": "RODAL-SUD-04",
            "productor_id": "CUIT-30123456789",
            "producto_forestal": "Madera Aserrada (Eucalipto)",
            "latitud": -27.50,
            "longitud": -58.90,
        }
        res = evaluar_compliance_lote(
            lote,
            volumen_ingresado_ton=200.0,
            volumen_exportar_ton=80.0,
        )
        self.assertEqual(res["profile"], LEGACY_NON_REGULATORY_PROFILE)
        self.assertIsNone(res["regulatory_conclusion"])
        self.assertEqual(res["dictamen"], "Verde")
        self.assertTrue(res["balance_masas"].es_valido)
        self.assertIn("PREVIEW NO REGULATORIO", res["observacion"])
        self.assertEqual(res["satelital"]["source"], "SIMULATED_LEGACY_SERIES")

    def test_generar_dds_json_traces_nt_is_retired_non_regulatory_preview(self):
        lote = {
            "identificador": "RODAL-SUD-04",
            "productor_id": "CUIT-30123456789",
            "producto_forestal": "Madera Aserrada (Pino)",
            "latitud": -27.50,
            "longitud": -58.90,
            "polygon_wkt": (
                "POLYGON((-58.91 -27.51, -58.89 -27.51, -58.89 -27.49, "
                "-58.91 -27.49, -58.91 -27.51))"
            ),
        }
        payload = json.loads(
            generar_dds_json_traces_nt(lote, volumen_exportar_ton=100.0)
        )

        self.assertEqual(payload["profile"], LEGACY_NON_REGULATORY_PROFILE)
        self.assertTrue(payload["retired_generator"])
        self.assertTrue(payload["not_a_due_diligence_statement"])
        self.assertFalse(payload["submit_ready"])
        self.assertIsNone(payload["regulatory_conclusion"])
        self.assertNotIn("compliance", payload)
        serialized = json.dumps(payload).upper()
        self.assertNotIn('"STATUS": "COMPLIANT"', serialized)
        self.assertNotIn("DEFORESTATION_FREE", serialized)
        self.assertNotIn("LEGAL_HARVEST_VERIFIED", serialized)

    def test_generar_pdf_reporte_bytes(self):
        lote = {
            "identificador": "LOTE-DEMO-01",
            "productor_id": "CUIT-30000000001",
            "producto_forestal": "Carbón Vegetal",
        }
        pdf_bytes = generar_pdf_reporte_bytes(
            lote,
            "Verde",
            "Aprobado",
            100.0,
            20.0,
            0.25,
        )
        self.assertIsInstance(pdf_bytes, bytes)
        self.assertGreater(len(pdf_bytes), 0)


if __name__ == "__main__":
    unittest.main()
