from __future__ import annotations

import asyncio
from http.cookies import SimpleCookie
import sys
import unittest
from pathlib import Path

import pytest
from fastapi import HTTPException, Request, Response

sys_path_root = str(Path(__file__).resolve().parents[1])
if sys_path_root not in sys.path:
    sys.path.insert(0, sys_path_root)

from main import render_admin_view, render_login_view
from litoral_trace.api.auth import LoginRequest, login_b2b
from litoral_trace.db.init_db import (
    DEVELOPMENT_SUPERADMIN_USERNAME,
    get_non_production_superadmin_seed,
    inicializar_base_datos_postgis,
)


class TestLoginSecurityHotfix(unittest.TestCase):
    @staticmethod
    def _extract_cookies(response: Response) -> dict[str, str]:
        parsed_cookie = SimpleCookie()
        for set_cookie_header in response.headers.getlist("set-cookie"):
            parsed_cookie.load(set_cookie_header)
        return {
            cookie_name: morsel.value
            for cookie_name, morsel in parsed_cookie.items()
        }

    def test_login_page_does_not_embed_public_credentials(self):
        request = Request(
            scope={"type": "http", "method": "GET", "path": "/", "headers": []}
        )
        response = asyncio.run(render_login_view(request))
        body = response.body.decode("utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertIn('autocomplete="username"', body)
        self.assertIn('autocomplete="current-password"', body)
        self.assertNotIn('value="admin"', body)
        self.assertNotIn("admin123", body)
        self.assertNotIn('value="admin123"', body)

    def test_public_admin_view_does_not_embed_assigned_credentials(self):
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
        cookies = self._extract_cookies(login_response)
        request = Request(
            scope={
                "type": "http",
                "method": "GET",
                "path": "/admin",
                "headers": [
                    (
                        b"cookie",
                        "; ".join(
                            f"{cookie_name}={cookie_value}"
                            for cookie_name, cookie_value in cookies.items()
                        ).encode("utf-8"),
                    )
                ],
            }
        )
        response = asyncio.run(render_admin_view(request))
        body = response.body.decode("utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertIn('autocomplete="username"', body)
        self.assertIn('autocomplete="new-password"', body)
        self.assertNotIn("SanMiguel2026!", body)
        self.assertNotIn('value="sanmiguel_admin"', body)

    def test_valid_login_still_succeeds(self):
        token_response = asyncio.run(
            login_b2b(
                LoginRequest(
                    username="admin",
                    password=get_non_production_superadmin_seed()[1],
                ),
                Response(),
            )
        )

        self.assertEqual(token_response.user_info["username"], "admin")
        self.assertTrue(token_response.access_token)

    def test_invalid_login_keeps_generic_401_semantics(self):
        with self.assertRaises(HTTPException) as exc_info:
            asyncio.run(
                login_b2b(
                    LoginRequest(username="admin", password="wrong-password"),
                    Response(),
                )
            )

        self.assertEqual(exc_info.exception.status_code, 401)
        self.assertEqual(
            exc_info.exception.detail,
            "Usuario o contrasena incorrectos.",
        )


def test_non_production_seed_is_available_under_test_environment():
    username, password = get_non_production_superadmin_seed()

    assert username == DEVELOPMENT_SUPERADMIN_USERNAME
    assert password
    assert password != "admin123"


def test_production_seed_is_blocked(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")

    with pytest.raises(RuntimeError, match="producci"):
        get_non_production_superadmin_seed()

    with pytest.raises(RuntimeError, match="producci"):
        inicializar_base_datos_postgis()
