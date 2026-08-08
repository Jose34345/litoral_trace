import unittest
import json

from litoral_trace.auth.tokens import create_jwt_token, verify_jwt_token
from litoral_trace.auth.api_keys import generate_api_key, verify_api_key_hash
from litoral_trace.auth.rbac import Permission, has_permission
from litoral_trace.services.compliance import evaluar_compliance_lote, generar_dds_json_traces_nt
from litoral_trace.services.reports import generar_pdf_reporte_bytes

class TestFullProductionIntegration(unittest.TestCase):
    def test_e2e_saas_workflow(self):
        # 1. Autenticación y Token JWT
        username = "comercial@expchaco.com"
        org_id = 42
        role = "manager"

        api_key = generate_api_key()
        self.assertTrue(verify_api_key_hash(api_key.full_key, api_key.key_hash))
        
        jwt_token = create_jwt_token({"sub": username, "org_id": org_id, "role": role})
        payload = verify_jwt_token(jwt_token)
        self.assertEqual(payload["org_id"], org_id)
        self.assertTrue(has_permission(payload["role"], Permission.LOTE_CREATE))

        # 2. Evaluación de Compliance
        lote_dict = {
            "identificador": "RODAL-EUCALIPTO-09",
            "productor_id": "20-34345942-0",
            "producto_forestal": "Madera Aserrada (Eucalipto)",
            "hectareas": 150.0,
            "latitud": -27.48,
            "longitud": -58.98,
            "polygon_wkt": "POLYGON((-58.99 -27.49, -58.97 -27.49, -58.97 -27.47, -58.99 -27.49, -58.99 -27.49))"
        }
        comp_res = evaluar_compliance_lote(lote_dict, volumen_ingresado_ton=400.0, volumen_exportar_ton=160.0)
        self.assertEqual(comp_res["dictamen"], "Verde")

        # 3. Emisión de Entregables TRACES NT
        dds_json = generar_dds_json_traces_nt(lote_dict, volumen_exportar_ton=160.0, operador_username=username)
        dds_data = json.loads(dds_json)
        self.assertEqual(dds_data["compliance"]["status"], "COMPLIANT")

        pdf_bytes = generar_pdf_reporte_bytes(
            lote_dict,
            comp_res["dictamen"],
            comp_res["observacion"],
            volumen_ingresado=400.0,
            volumen_exportar=160.0,
            coeficiente_rendimiento=comp_res["balance_masas"].coeficiente_rendimiento
        )
        self.assertGreater(len(pdf_bytes), 0)

if __name__ == "__main__":
    unittest.main()
