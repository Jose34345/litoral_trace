import unittest
import io
import pandas as pd
from litoral_trace.ui.theme import PALETTE, ENTERPRISE_THEME_CSS
from litoral_trace.services.batch import generar_plantilla_excel, procesar_lote_masivo, BATCH_COLUMNAS

class TestBatchAndUI(unittest.TestCase):
    def test_theme_palette_and_css(self):
        self.assertIn("primary", PALETTE)
        self.assertIn("success", PALETTE)
        self.assertIn("kpi-container", ENTERPRISE_THEME_CSS)

    def test_generar_plantilla_excel(self):
        template_bytes = generar_plantilla_excel()
        self.assertIsInstance(template_bytes, bytes)
        self.assertGreater(len(template_bytes), 0)
        
        df_read = pd.read_excel(io.BytesIO(template_bytes), sheet_name="Plantilla_LitoralTrace")
        self.assertEqual(list(df_read.columns), BATCH_COLUMNAS)
        self.assertEqual(len(df_read), 1)

    def test_procesar_lote_masivo_multi_filas(self):
        df_input = pd.DataFrame([
            {
                "Identificador_Lote": "RODAL-APTO-01",
                "ID_Proveedor": "30-11111111-1",
                "Producto_Forestal": "Madera Aserrada (Pino)",
                "Hectareas": 50.0,
                "Latitud": -27.45,
                "Longitud": -58.90,
                "Volumen_Ingresado_Ton": 100.0,
                "Volumen_Exportar_Ton": 45.0
            },
            {
                "Identificador_Lote": "RODAL-EXCESO-02",
                "ID_Proveedor": "30-22222222-2",
                "Producto_Forestal": "Madera Aserrada (Pino)",
                "Hectareas": 30.0,
                "Latitud": -27.50,
                "Longitud": -58.95,
                "Volumen_Ingresado_Ton": 100.0,
                "Volumen_Exportar_Ton": 90.0
            }
        ])
        
        df_resumen, zip_bytes = procesar_lote_masivo(df_input)
        self.assertEqual(len(df_resumen), 2)
        self.assertEqual(df_resumen.iloc[0]["Dictamen"], "Verde")
        self.assertEqual(df_resumen.iloc[1]["Dictamen"], "Rojo")
        self.assertGreater(len(zip_bytes), 0)

if __name__ == "__main__":
    unittest.main()
