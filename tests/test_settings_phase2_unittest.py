import unittest
import asyncio
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from litoral_trace.services.licenses import obtener_cuota_tenant, generar_invitacion_demo_prospecto
from litoral_trace.api.auth import login_b2b, LoginRequest, get_current_tenant_user
from litoral_trace.api.settings import consultar_licencia_tenant, generar_invitacion_demo_endpoint, InviteDemoUserRequest
from main import render_settings_view
from fastapi import Response, Request

class TestSettingsPhase2(unittest.TestCase):
    def setUp(self):
        req = LoginRequest(username="admin", password="admin123")
        token_res = asyncio.run(login_b2b(req, Response()))
        bearer_hdr = f"Bearer {token_res.access_token}"
        self.tenant_user = get_current_tenant_user(authorization=bearer_hdr)

    def test_obtener_cuota_tenant(self):
        status_obj = obtener_cuota_tenant(organization_id=1)
        self.assertEqual(status_obj.monthly_lote_limit, 100)
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
        self.assertEqual(body["monthly_lote_limit"], 100)

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

    def test_render_settings_view(self):
        req = Request(scope={"type": "http", "method": "GET", "path": "/settings", "headers": []})
        res = asyncio.run(render_settings_view(req))
        self.assertEqual(res.status_code, 200)
        self.assertIn("Control de Licencia", res.body.decode('utf-8'))

if __name__ == "__main__":
    unittest.main()
