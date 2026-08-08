import os
import unittest
from litoral_trace.auth.tokens import create_jwt_token, verify_jwt_token
from litoral_trace.auth.api_keys import generate_api_key, hash_api_key, verify_api_key_hash
from litoral_trace.auth.rbac import Permission, has_permission, permissions_for_role

class TestAuthAndRBAC(unittest.TestCase):
    def test_jwt_token_flow(self):
        payload = {"sub": "user_123", "org_id": 42, "role": "admin"}
        old_secret = os.environ.get("JWT_SECRET_KEY")
        os.environ["JWT_SECRET_KEY"] = "test_secret_key_with_32_chars_123456"

        try:
            token = create_jwt_token(payload, expires_in_seconds=3600)
            self.assertIsNotNone(token)
            self.assertEqual(len(token.split('.')), 3)

            decoded = verify_jwt_token(token)
            self.assertIsNotNone(decoded)
            self.assertEqual(decoded["sub"], "user_123")
            self.assertEqual(decoded["org_id"], 42)
            self.assertEqual(decoded["role"], "admin")
        finally:
            if old_secret is None:
                os.environ.pop("JWT_SECRET_KEY", None)
            else:
                os.environ["JWT_SECRET_KEY"] = old_secret

    def test_jwt_invalid_signature_and_expiration(self):
        payload = {"sub": "user_456"}
        old_secret = os.environ.get("JWT_SECRET_KEY")
        os.environ["JWT_SECRET_KEY"] = "correct_secret_key_for_tests_123456"

        try:
            token = create_jwt_token(payload, expires_in_seconds=-10)
            self.assertIsNone(verify_jwt_token(token))

            valid_token = create_jwt_token(payload, expires_in_seconds=3600)
            os.environ["JWT_SECRET_KEY"] = "wrong_secret_key_for_tests_123456"
            self.assertIsNone(verify_jwt_token(valid_token))
        finally:
            if old_secret is None:
                os.environ.pop("JWT_SECRET_KEY", None)
            else:
                os.environ["JWT_SECRET_KEY"] = old_secret

    def test_jwt_supports_explicit_security_context_injection(self):
        payload = {"sub": "user_789", "org_id": 7, "role": "manager"}
        token = create_jwt_token(
            payload,
            expires_in_seconds=900,
            secret_key="explicit_secret_key_for_tests_123456",
            issuer="litoral-trace-api",
            audience="litoral-trace-b2b",
            issued_at_epoch=1_700_000_000,
        )

        decoded = verify_jwt_token(
            token,
            secret_key="explicit_secret_key_for_tests_123456",
            issuer="litoral-trace-api",
            audience="litoral-trace-b2b",
            now_epoch=1_700_000_100,
        )
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded["token_type"], "access")
        self.assertEqual(decoded["iss"], "litoral-trace-api")
        self.assertEqual(decoded["aud"], "litoral-trace-b2b")

        self.assertIsNone(
            verify_jwt_token(
                token,
                secret_key="explicit_secret_key_for_tests_123456",
                issuer="otro-issuer",
                audience="litoral-trace-b2b",
                now_epoch=1_700_000_100,
            )
        )

    def test_api_key_generation_and_verification(self):
        generated = generate_api_key()
        self.assertTrue(generated.full_key.startswith("lt_live_"))
        self.assertEqual(len(generated.prefix), 16)
        self.assertIsNotNone(generated.key_hash)
        
        # Verificar hash
        self.assertTrue(verify_api_key_hash(generated.full_key, generated.key_hash))
        self.assertFalse(verify_api_key_hash("lt_live_invalidkey12345", generated.key_hash))

    def test_rbac_permission_matrix(self):
        self.assertEqual(permissions_for_role("unknown"), frozenset())
        self.assertEqual(permissions_for_role(None), frozenset())

        self.assertEqual(permissions_for_role("superadmin"), frozenset(Permission))
        self.assertIn(Permission.PLATFORM_ADMIN, permissions_for_role("superadmin"))
        self.assertNotIn(Permission.PLATFORM_ADMIN, permissions_for_role("admin"))
        self.assertNotIn(Permission.SETTINGS_WRITE, permissions_for_role("manager"))
        self.assertNotIn(Permission.SATELLITE_RUN, permissions_for_role("auditor"))

        self.assertTrue(has_permission("admin", Permission.LOTE_CREATE))
        self.assertTrue(has_permission("manager", Permission.SATELLITE_RUN))
        self.assertTrue(has_permission("auditor", Permission.LOTE_READ))
        self.assertFalse(has_permission("cliente", Permission.LOTE_DELETE))
        self.assertFalse(has_permission("unknown", Permission.LOTE_READ))

if __name__ == "__main__":
    unittest.main()
