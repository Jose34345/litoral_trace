import unittest
import asyncio
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from main import health_check, root_index, render_login_view, render_dashboard_view
from litoral_trace.api.auth import login_b2b, LoginRequest, get_current_tenant_user
from litoral_trace.api.lotes import listar_lotes_tenant, evaluar_compliance_endpoint, LoteEvaluacionRequest
from fastapi import Response, Request

class TestFastAPIStep5FullIntegration(unittest.TestCase):
    def test_e2e_fastapi_workflow(self):
        # 1. Healthcheck
        h_res = asyncio.run(health_check())
        self.assertEqual(h_res.status_code, 200)

        # 2. Login B2B
        req_login = LoginRequest(username="admin", password="admin123")
        res_dummy = Response()
        token_res = asyncio.run(login_b2b(req_login, res_dummy))
        self.assertIsNotNone(token_res.access_token)

        # 3. Contexto Tenant
        bearer_hdr = f"Bearer {token_res.access_token}"
        tenant_user = get_current_tenant_user(authorization=bearer_hdr)
        self.assertEqual(tenant_user.username, "admin")

        # 4. Listar Lotes
        lotes_res = asyncio.run(listar_lotes_tenant(user=tenant_user))
        self.assertEqual(lotes_res.status_code, 200)

        # 5. Evaluar Compliance EUDR
        eval_req = LoteEvaluacionRequest(
            identificador="RODAL-PINO-EXPORT-01",
            productor_id="30-12345678-9",
            producto_forestal="Madera Aserrada (Pino)",
            hectareas=200.0,
            latitud=-27.45,
            longitud=-58.90,
            volumen_ingresado_ton=600.0,
            volumen_exportar_ton=280.0
        )
        comp_res = asyncio.run(evaluar_compliance_endpoint(eval_req, user=tenant_user))
        self.assertEqual(comp_res.status_code, 200)
        body = json.loads(comp_res.body.decode('utf-8'))
        self.assertEqual(body["dictamen"], "Verde")
        self.assertIsNotNone(body["dds_traces_nt_json"])

        # 6. HTML Template Rendering
        req_view = Request(scope={"type": "http", "method": "GET", "path": "/dashboard", "headers": []})
        dash_res = asyncio.run(render_dashboard_view(req_view))
        self.assertEqual(dash_res.status_code, 200)

if __name__ == "__main__":
    unittest.main()
