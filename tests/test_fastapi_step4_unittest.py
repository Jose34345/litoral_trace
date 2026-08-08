import unittest
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from main import render_login_view, render_dashboard_view
from fastapi import Request

class TestFastAPIStep4Templates(unittest.TestCase):
    def test_render_login_view(self):
        req = Request(scope={"type": "http", "method": "GET", "path": "/", "headers": []})
        response = asyncio.run(render_login_view(req))
        self.assertEqual(response.status_code, 200)
        self.assertIn("Litoral Trace", response.body.decode('utf-8'))
        self.assertIn("EUDR", response.body.decode('utf-8'))

    def test_render_dashboard_view(self):
        req = Request(scope={"type": "http", "method": "GET", "path": "/dashboard", "headers": []})
        response = asyncio.run(render_dashboard_view(req))
        self.assertEqual(response.status_code, 200)
        self.assertIn("Trazabilidad Forestal", response.body.decode('utf-8'))
        self.assertIn("TRACES NT", response.body.decode('utf-8'))

if __name__ == "__main__":
    unittest.main()
