import asyncio
import json
import sys
import unittest
from http.cookies import SimpleCookie
from pathlib import Path
from uuid import uuid4

from fastapi import Request, Response

sys.path.insert(
    0,
    str(
        Path(__file__)
        .resolve()
        .parents[1]
    ),
)

from main import (
    render_admin_view,
    render_login_view,
)
from litoral_trace.api.admin import (
    CrearEmpresaClienteRequest,
    crear_organizacion_endpoint,
    listar_organizaciones_endpoint,
)
from litoral_trace.api.auth import (
    LoginRequest,
    get_current_tenant_user,
    login_b2b,
)
from litoral_trace.db.init_db import (
    get_non_production_superadmin_seed,
)


class TestSuperAdminPhase(unittest.TestCase):
    @staticmethod
    def _extract_cookies(
        response: Response,
    ) -> dict[str, str]:
        parsed_cookie = SimpleCookie()

        for set_cookie_header in (
            response.headers.getlist(
                "set-cookie"
            )
        ):
            parsed_cookie.load(
                set_cookie_header
            )

        return {
            cookie_name: morsel.value
            for (
                cookie_name,
                morsel,
            ) in parsed_cookie.items()
        }

    def setUp(self):
        req = LoginRequest(
            username="admin",
            password=(
                get_non_production_superadmin_seed()[1]
            ),
        )

        res_dummy = Response()

        token_res = asyncio.run(
            login_b2b(
                req,
                res_dummy,
            )
        )

        bearer_hdr = (
            f"Bearer "
            f"{token_res.access_token}"
        )

        self.superadmin_user = (
            get_current_tenant_user(
                authorization=bearer_hdr
            )
        )

        self.cookies = (
            self._extract_cookies(
                res_dummy
            )
        )

    def test_render_login_view_displays_login_page(
        self,
    ):
        req = Request(
            scope={
                "type": "http",
                "method": "GET",
                "path": "/",
                "headers": [],
            }
        )

        response = asyncio.run(
            render_login_view(
                req
            )
        )

        body = response.body.decode(
            "utf-8"
        )

        self.assertEqual(
            response.status_code,
            200,
        )

        self.assertIn(
            "Litoral Trace",
            body,
        )

        self.assertIn(
            "Acceso de clientes",
            body,
        )

        self.assertIn(
            "Acceso seguro a Litoral Trace",
            body,
        )

        self.assertIn(
            'action="/login"',
            body,
        )

        self.assertIn(
            'autocomplete="username"',
            body,
        )

        self.assertIn(
            'autocomplete="current-password"',
            body,
        )

    def test_listar_organizaciones_superadmin(
        self,
    ):
        res = asyncio.run(
            listar_organizaciones_endpoint(
                refresh_token_cookie=(
                    self.cookies.get(
                        "refresh_token"
                    )
                ),
                admin=(
                    self.superadmin_user
                ),
            )
        )

        self.assertEqual(
            res.status_code,
            200,
        )

        body = json.loads(
            res.body.decode(
                "utf-8"
            )
        )

        self.assertIn(
            "organizations",
            body,
        )

        self.assertGreater(
            body["total"],
            0,
        )

    def test_crear_organizacion_superadmin(
        self,
    ):
        suffix = uuid4().hex[:8]

        payload = (
            CrearEmpresaClienteRequest(
                name=(
                    "Aserradero "
                    f"San Miguel {suffix}"
                ),
                tax_id=(
                    f"30-{suffix}"
                ),
                admin_email=(
                    f"contacto.{suffix}"
                    "@sanmiguel.com"
                ),
                admin_username=(
                    "sanmiguel_admin_"
                    f"{suffix}"
                ),
                admin_password=(
                    "SanMiguelPassword2026!"
                ),
                tier="pro",
                monthly_lote_limit=50,
                monthly_ton_limit=3000.0,
            )
        )

        res = asyncio.run(
            crear_organizacion_endpoint(
                payload,
                refresh_token_cookie=(
                    self.cookies.get(
                        "refresh_token"
                    )
                ),
                admin=(
                    self.superadmin_user
                ),
            )
        )

        self.assertEqual(
            res.status_code,
            201,
        )

        body = json.loads(
            res.body.decode(
                "utf-8"
            )
        )

        self.assertEqual(
            body["status"],
            "success",
        )

        self.assertEqual(
            body["admin_username"],
            (
                "sanmiguel_admin_"
                f"{suffix}"
            ),
        )

        self.assertIn(
            "license_id",
            body,
        )

        self.assertNotIn(
            "whatsapp_share_text",
            body,
        )

        self.assertNotIn(
            "admin_password",
            json.dumps(
                body
            ),
        )

    def test_render_admin_view(
        self,
    ):
        cookie_header = "; ".join(
            (
                f"{cookie_name}="
                f"{cookie_value}"
            )
            for (
                cookie_name,
                cookie_value,
            ) in self.cookies.items()
        ).encode(
            "utf-8"
        )

        req = Request(
            scope={
                "type": "http",
                "method": "GET",
                "path": "/admin",
                "headers": [
                    (
                        b"cookie",
                        cookie_header,
                    )
                ],
            }
        )

        res = asyncio.run(
            render_admin_view(
                req
            )
        )

        body = res.body.decode(
            "utf-8"
        )

        self.assertEqual(
            res.status_code,
            200,
        )

        self.assertIn(
            "Administración global",
            body,
        )

        self.assertIn(
            "Organizaciones de la plataforma",
            body,
        )


if __name__ == "__main__":
    unittest.main()
