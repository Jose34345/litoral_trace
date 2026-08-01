import unittest
import time
from litoral_trace.auth.tokens import create_jwt_token, verify_jwt_token
from litoral_trace.auth.api_keys import generate_api_key, hash_api_key, verify_api_key_hash
from litoral_trace.auth.rbac import has_permission, get_role_level

class TestAuthAndRBAC(unittest.TestCase):
    def test_jwt_token_flow(self):
        payload = {"sub": "user_123", "org_id": 42, "role": "admin"}
        secret = "test_secret_key"
        
        token = create_jwt_token(payload, secret_key=secret, expires_in_seconds=3600)
        self.assertIsNotNone(token)
        self.assertEqual(len(token.split('.')), 3)
        
        decoded = verify_jwt_token(token, secret_key=secret)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded["sub"], "user_123")
        self.assertEqual(decoded["org_id"], 42)
        self.assertEqual(decoded["role"], "admin")

    def test_jwt_invalid_signature_and_expiration(self):
        payload = {"sub": "user_456"}
        secret = "correct_secret"
        
        token = create_jwt_token(payload, secret_key=secret, expires_in_seconds=-10) # Expirado
        self.assertIsNone(verify_jwt_token(token, secret_key=secret))
        
        valid_token = create_jwt_token(payload, secret_key=secret, expires_in_seconds=3600)
        self.assertIsNone(verify_jwt_token(valid_token, secret_key="wrong_secret"))

    def test_api_key_generation_and_verification(self):
        generated = generate_api_key()
        self.assertTrue(generated.full_key.startswith("lt_live_"))
        self.assertEqual(len(generated.prefix), 16)
        self.assertIsNotNone(generated.key_hash)
        
        # Verificar hash
        self.assertTrue(verify_api_key_hash(generated.full_key, generated.key_hash))
        self.assertFalse(verify_api_key_hash("lt_live_invalidkey12345", generated.key_hash))

    def test_rbac_hierarchy(self):
        self.assertEqual(get_role_level("admin"), 100)
        self.assertEqual(get_role_level("manager"), 75)
        self.assertEqual(get_role_level("auditor"), 50)
        self.assertEqual(get_role_level("cliente"), 25)
        self.assertEqual(get_role_level("unknown"), 0)
        
        # Permisos
        self.assertTrue(has_permission("admin", "manager"))
        self.assertTrue(has_permission("manager", "auditor"))
        self.assertTrue(has_permission("auditor", "cliente"))
        self.assertFalse(has_permission("cliente", "manager"))
        self.assertFalse(has_permission("auditor", "admin"))

if __name__ == "__main__":
    unittest.main()
