import unittest
import asyncio
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from litoral_trace.api.auth import login_b2b, LoginRequest, get_current_tenant_user, UserTenantContext
from litoral_trace.auth.tokens import create_jwt_token
from fastapi import HTTPException
from fastapi import Response

class TestFastAPIStep2Auth(unittest.TestCase):
    def test_login_success_admin(self):
        req = LoginRequest(username="admin", password="admin123")
        res_dummy = Response()
        
        token_res = asyncio.run(login_b2b(req, res_dummy))
        self.assertIsNotNone(token_res.access_token)
        self.assertEqual(token_res.user_info["username"], "admin")
        self.assertEqual(token_res.user_info["organization_id"], 1)
        self.assertEqual(token_res.user_info["role"], "superadmin")

    def test_get_current_tenant_user_from_bearer(self):
        req = LoginRequest(username="admin", password="admin123")
        token_res = asyncio.run(login_b2b(req, Response()))
        
        bearer_hdr = f"Bearer {token_res.access_token}"
        tenant_context = get_current_tenant_user(authorization=bearer_hdr)
        
        self.assertIsInstance(tenant_context, UserTenantContext)
        self.assertEqual(tenant_context.username, "admin")
        self.assertEqual(tenant_context.organization_id, 1)

    def test_get_current_tenant_user_rejects_incomplete_claims(self):
        token = create_jwt_token({"sub": "admin", "role": "admin"})

        with self.assertRaises(HTTPException) as ctx:
            get_current_tenant_user(authorization=f"Bearer {token}")

        self.assertEqual(ctx.exception.status_code, 401)

if __name__ == "__main__":
    unittest.main()
