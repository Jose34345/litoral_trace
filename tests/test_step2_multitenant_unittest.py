import unittest
import asyncio
import os
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from litoral_trace.api.auth import login_b2b, LoginRequest, get_current_tenant_user
from litoral_trace.api.satellite import consultar_ndvi_satelital_lote_endpoint, SatelliteQueryByLoteRequest
from fastapi import Response, HTTPException

class TestStep2MultiTenant(unittest.TestCase):
    def setUp(self):
        os.environ["ENVIRONMENT"] = "test"
        req = LoginRequest(username="admin", password="admin123")
        res_dummy = Response()
        token_res = asyncio.run(login_b2b(req, res_dummy))
        bearer_hdr = f"Bearer {token_res.access_token}"
        self.tenant_user = get_current_tenant_user(authorization=bearer_hdr)

    def test_tenant_query_own_lote_success(self):
        payload = SatelliteQueryByLoteRequest(lote_id=101, start_date="2020-12-31")
        res = asyncio.run(consultar_ndvi_satelital_lote_endpoint(payload, user=self.tenant_user))
        self.assertEqual(res.status_code, 200)
        body = json.loads(res.body.decode('utf-8'))
        self.assertEqual(body["lote_id"], 101)
        self.assertEqual(body["organization_id"], self.tenant_user.organization_id)

    def test_unauthenticated_request_rejected(self):
        with self.assertRaises(HTTPException) as ctx:
            get_current_tenant_user(authorization=None, bearer_token=None, session_jwt=None)
        self.assertEqual(ctx.exception.status_code, 401)

if __name__ == "__main__":
    unittest.main()
