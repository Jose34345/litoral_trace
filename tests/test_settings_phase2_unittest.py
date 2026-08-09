import unittest
import asyncio
import sys
import json
import os
from http.cookies import SimpleCookie
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi import HTTPException, Response, Request

from litoral_trace.services.licenses import obtener_cuota_tenant, generar_invitacion_demo_prospecto
from litoral_trace.api.auth import login_b2b, LoginRequest, get_current_tenant_user
from litoral_trace.api.settings import consultar_licencia_tenant, generar_invitacion_demo_endpoint, InviteDemoUserRequest
from litoral_trace.db.init_db import get_non_production_superadmin_seed
from main import render_settings_view

class TestSettingsPhase2(unittest.TestCase):
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
        self.login_response = Response()
        token_res = asyncio.run(login_b2b(req, self.login_response))
        bearer_hdr = f"Bearer {token_res.access_token}"
        self.tenant_user = get_current_tenant_user(authorization=bearer_hdr)

    def test_obtener_cuota_tenant(self):
        status_obj = obtener_cuota_tenant(organization_id=1)
        self.assertEqual(status_obj.monthly_lote_limit, 1000)
        self.assertTrue(status_obj.quota_available)

    def test_generar_invitacion_demo_prospecto(self):
        demo = generar_invitacion_demo_prospecto(
            cuit_empresa="30-71234567-8",
            nombre_contacto="Mario Darío Benítez",
            email_contacto="mario.benitez@despachantes.com"
        )
        self.assertIn("username_demo", demo)
        self.assertIn("password_demo", demo)

    def test_consultar_licencia_tenant_endpoint(self):
        res = asyncio.run(consultar_licencia_tenant(user=self.tenant_user))
        self.assertEqual(res.status_code, 200)
        body = json.loads(res.body.decode('utf-8'))
        self.assertEqual(body["monthly_lote_limit"], 1000)

    def test_generar_invitacion_demo_endpoint(self):
        payload = InviteDemoUserRequest(
            cuit_empresa="30-71234567-8",
            nombre_contacto="Mario Darío Benítez",
            email_contacto="mario.benitez@despachantes.com",
            especie_principal="Madera Aserrada (Pino)"
        )
        res = asyncio.run(generar_invitacion_demo_endpoint(payload, user=self.tenant_user))
        self.assertEqual(res.status_code, 201)
        body = json.loads(res.body.decode('utf-8'))
        self.assertEqual(body["status"], "success")

    def test_generar_invitacion_demo_prospecto_disabled_in_production(self):
        previous_environment = os.environ.get("ENVIRONMENT")
        os.environ["ENVIRONMENT"] = "production"
        try:
            with self.assertRaises(RuntimeError):
                generar_invitacion_demo_prospecto(
                    cuit_empresa="30-71234567-8",
                    nombre_contacto="Mario DarÃ­o BenÃ­tez",
                    email_contacto="mario.benitez@despachantes.com"
                )
        finally:
            if previous_environment is None:
                os.environ.pop("ENVIRONMENT", None)
            else:
                os.environ["ENVIRONMENT"] = previous_environment

    def test_generar_invitacion_demo_endpoint_disabled_in_production(self):
        previous_environment = os.environ.get("ENVIRONMENT")
        os.environ["ENVIRONMENT"] = "production"
        payload = InviteDemoUserRequest(
            cuit_empresa="30-71234567-8",
            nombre_contacto="Mario DarÃ­o BenÃ­tez",
            email_contacto="mario.benitez@despachantes.com",
            especie_principal="Madera Aserrada (Pino)"
        )
        try:
            with self.assertRaises(HTTPException) as exc_info:
                asyncio.run(
                    generar_invitacion_demo_endpoint(
                        payload,
                        user=self.tenant_user,
                    )
                )
        finally:
            if previous_environment is None:
                os.environ.pop("ENVIRONMENT", None)
            else:
                os.environ["ENVIRONMENT"] = previous_environment

        self.assertEqual(exc_info.exception.status_code, 404)

    def test_render_settings_view(self):
        cookie_header = "; ".join(
            f"{cookie_name}={cookie_value}"
            for cookie_name, cookie_value in self._extract_cookies(self.login_response).items()
        ).encode("utf-8")
        req = Request(
            scope={
                "type": "http",
                "method": "GET",
                "path": "/settings",
                "headers": [(b"cookie", cookie_header)],
            }
        )
        res = asyncio.run(render_settings_view(req))
        self.assertEqual(res.status_code, 200)
        self.assertIn("Control de Licencia", res.body.decode('utf-8'))

if __name__ == "__main__":
    unittest.main()
