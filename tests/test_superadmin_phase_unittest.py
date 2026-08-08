import unittest
import asyncio
import sys
import json
from http.cookies import SimpleCookie
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from main import render_login_view, render_admin_view
from litoral_trace.api.auth import login_b2b, LoginRequest, get_current_tenant_user
from litoral_trace.api.admin import listar_organizaciones_endpoint, crear_organizacion_endpoint, CrearEmpresaClienteRequest
from litoral_trace.db.init_db import get_non_production_superadmin_seed
from fastapi import Response, Request

class TestSuperAdminPhase(unittest.TestCase):
    @staticmethod
    def _extract_cookies(response: Response) -> dict[str, str]:
        parsed_cookie = SimpleCookie()
        for set_cookie_header in response.headers.getlist("set-cookie"):
            parsed_cookie.load(set_cookie_header)
        return {
            cookie_name: morsel.value
            for cookie_name, morsel in parsed_cookie.items()
        }

    def setUp(self):
        req = LoginRequest(
            username="admin",
            password=get_non_production_superadmin_seed()[1],
        )
        res_dummy = Response()
        token_res = asyncio.run(login_b2b(req, res_dummy))
        bearer_hdr = f"Bearer {token_res.access_token}"
        self.superadmin_user = get_current_tenant_user(authorization=bearer_hdr)
        self.cookies = self._extract_cookies(res_dummy)

    def test_render_login_view_displays_login_page(self):
        req = Request(scope={"type": "http", "method": "GET", "path": "/", "headers": []})
        response = asyncio.run(render_login_view(req))
        self.assertEqual(response.status_code, 200)
        self.assertIn("Litoral Trace", response.body.decode('utf-8'))
        self.assertIn("Ingresar", response.body.decode('utf-8'))

    def test_listar_organizaciones_superadmin(self):
        res = asyncio.run(listar_organizaciones_endpoint(admin=self.superadmin_user))
        self.assertEqual(res.status_code, 200)
        body = json.loads(res.body.decode('utf-8'))
        self.assertIn("organizations", body)
        self.assertGreater(body["total"], 0)

    def test_crear_organizacion_superadmin(self):
        payload = CrearEmpresaClienteRequest(
            name="Aserradero San Miguel S.R.L.",
            tax_id="30-88888888-4",
            admin_email="contacto@sanmiguel.com",
            admin_username="sanmiguel_admin",
            admin_password="SanMiguelPassword2026!",
            tier="pro",
            monthly_lote_limit=50,
            monthly_ton_limit=3000.0
        )
        res = asyncio.run(crear_organizacion_endpoint(payload, admin=self.superadmin_user))
        self.assertEqual(res.status_code, 201)
        body = json.loads(res.body.decode('utf-8'))
        self.assertEqual(body["status"], "success")
        self.assertIn("whatsapp_share_text", body)

    def test_render_admin_view(self):
        req = Request(
            scope={
                "type": "http",
                "method": "GET",
                "path": "/admin",
                "headers": [
                    (
                        b"cookie",
                        "; ".join(
                            f"{cookie_name}={cookie_value}"
                            for cookie_name, cookie_value in self.cookies.items()
                        ).encode("utf-8"),
                    )
                ],
            }
        )
        res = asyncio.run(render_admin_view(req))
        self.assertEqual(res.status_code, 200)
        self.assertIn("PANEL SUPERADMIN", res.body.decode('utf-8'))

if __name__ == "__main__":
    unittest.main()
