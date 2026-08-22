import unittest
import asyncio
import sys
from http.cookies import SimpleCookie
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi import Request, Response

from main import render_dashboard_view, render_login_view
from litoral_trace.api.auth import LoginRequest, login_b2b
from litoral_trace.db.init_db import get_non_production_superadmin_seed

class TestFastAPIStep4Templates(unittest.TestCase):
    @staticmethod
    def _extract_cookies(response: Response) -> dict[str, str]:
        parsed_cookie = SimpleCookie()
        for set_cookie_header in response.headers.getlist("set-cookie"):
            parsed_cookie.load(set_cookie_header)
        return {
            cookie_name: morsel.value
            for cookie_name, morsel in parsed_cookie.items()
        }

    def test_render_login_view(self):
        req = Request(scope={"type": "http", "method": "GET", "path": "/", "headers": []})
        response = asyncio.run(render_login_view(req))
        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("Litoral Trace", body)
        self.assertIn("Acceso seguro", body)

    def test_render_dashboard_view(self):
        login_response = Response()
        asyncio.run(
            login_b2b(
                LoginRequest(
                    username="admin",
                    password=get_non_production_superadmin_seed()[1],
                ),
                login_response,
            )
        )
        cookie_header = "; ".join(
            f"{cookie_name}={cookie_value}"
            for cookie_name, cookie_value in self._extract_cookies(login_response).items()
        ).encode("utf-8")
        req = Request(
            scope={
                "type": "http",
                "method": "GET",
                "path": "/dashboard",
                "headers": [(b"cookie", cookie_header)],
            }
        )
        response = asyncio.run(render_dashboard_view(req))
        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("Trazabilidad de despachos", body)
        self.assertIn("Reconstruir origen", body)
        self.assertIn("TRACES NT", body)

if __name__ == "__main__":
    unittest.main()
