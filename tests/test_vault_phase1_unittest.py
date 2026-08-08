import unittest
import asyncio
import sys
import json
from http.cookies import SimpleCookie
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from litoral_trace.services.vault import listar_documentos_boveda_tenant
from litoral_trace.api.auth import login_b2b, LoginRequest, get_current_tenant_user
from litoral_trace.api.vault import consultar_documentos_boveda, descargar_documento_boveda
from litoral_trace.db.init_db import get_non_production_superadmin_seed
from main import render_vault_view
from fastapi import Response, Request

class TestVaultPhase1(unittest.TestCase):
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

    def test_listar_documentos_boveda_tenant_isolation(self):
        docs_org1 = listar_documentos_boveda_tenant(organization_id=1)
        self.assertGreater(len(docs_org1), 0)
        for doc in docs_org1:
            self.assertEqual(doc["organization_id"], 1)

        docs_org2 = listar_documentos_boveda_tenant(organization_id=999)
        self.assertEqual(len(docs_org2), 0)

    def test_consultar_documentos_boveda_endpoint(self):
        res = asyncio.run(consultar_documentos_boveda(q="Rodal", type=None, user=self.tenant_user))
        self.assertEqual(res.status_code, 200)
        body = json.loads(res.body.decode('utf-8'))
        self.assertIn("documents", body)
        self.assertGreater(body["total_documents"], 0)

    def test_descargar_documento_boveda_endpoint(self):
        res = asyncio.run(descargar_documento_boveda(doc_id="DOC-DDS-2026-001", user=self.tenant_user))
        self.assertEqual(res.media_type, "application/json")

    def test_render_vault_view(self):
        cookie_header = "; ".join(
            f"{cookie_name}={cookie_value}"
            for cookie_name, cookie_value in self._extract_cookies(self.login_response).items()
        ).encode("utf-8")
        req = Request(
            scope={
                "type": "http",
                "method": "GET",
                "path": "/vault",
                "headers": [(b"cookie", cookie_header)],
            }
        )
        res = asyncio.run(render_vault_view(req))
        self.assertEqual(res.status_code, 200)
        self.assertIn("BÓVEDA PRIVADA", res.body.decode('utf-8'))

if __name__ == "__main__":
    unittest.main()
