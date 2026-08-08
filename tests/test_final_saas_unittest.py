import unittest
import asyncio
import sys
import json
from http.cookies import SimpleCookie
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from main import health_check, render_login_view, render_dashboard_view, render_vault_view, render_settings_view
from litoral_trace.api.auth import login_b2b, LoginRequest, get_current_tenant_user
from litoral_trace.api.lotes import evaluar_compliance_endpoint, LoteEvaluacionRequest
from litoral_trace.api.vault import consultar_documentos_boveda
from litoral_trace.api.settings import consultar_licencia_tenant
from litoral_trace.db.init_db import get_non_production_superadmin_seed
from fastapi import Response, Request

class TestFinalSaaS(unittest.TestCase):
    @staticmethod
    def _extract_cookies(response: Response) -> dict[str, str]:
        parsed_cookie = SimpleCookie()
        for set_cookie_header in response.headers.getlist("set-cookie"):
            parsed_cookie.load(set_cookie_header)
        return {
            cookie_name: morsel.value
            for cookie_name, morsel in parsed_cookie.items()
        }

    def test_full_saas_suite_verification(self):
        # 1. Healthcheck API
        h_res = asyncio.run(health_check())
        self.assertEqual(h_res.status_code, 200)

        # 2. Login B2B
        req_login = LoginRequest(
            username="admin",
            password=get_non_production_superadmin_seed()[1],
        )
        login_response = Response()
        token_res = asyncio.run(login_b2b(req_login, login_response))
        self.assertIsNotNone(token_res.access_token)

        # 3. Contexto Tenant
        bearer = f"Bearer {token_res.access_token}"
        tenant_user = get_current_tenant_user(authorization=bearer)
        self.assertEqual(tenant_user.username, "admin")

        # 4. Compliance EUDR
        eval_req = LoteEvaluacionRequest(
            identificador="RODAL-FINAL-01",
            productor_id="30-12345678-9",
            producto_forestal="Madera Aserrada (Pino)",
            hectareas=100.0,
            latitud=-27.45,
            longitud=-58.90,
            volumen_ingresado_ton=500.0,
            volumen_exportar_ton=220.0
        )
        comp_res = asyncio.run(evaluar_compliance_endpoint(eval_req, user=tenant_user))
        self.assertEqual(comp_res.status_code, 200)

        # 5. Bóveda Privada
        vault_res = asyncio.run(consultar_documentos_boveda(q=None, type=None, user=tenant_user))
        self.assertEqual(vault_res.status_code, 200)

        # 6. Licencias & Cuotas
        lic_res = asyncio.run(consultar_licencia_tenant(user=tenant_user))
        self.assertEqual(lic_res.status_code, 200)

        # 7. Frontend Views Rendering
        cookie_header = "; ".join(
            f"{cookie_name}={cookie_value}"
            for cookie_name, cookie_value in self._extract_cookies(login_response).items()
        ).encode("utf-8")
        req_view = Request(
            scope={
                "type": "http",
                "method": "GET",
                "path": "/dashboard",
                "headers": [(b"cookie", cookie_header)],
            }
        )
        dash_res = asyncio.run(render_dashboard_view(req_view))
        self.assertEqual(dash_res.status_code, 200)

if __name__ == "__main__":
    unittest.main()
