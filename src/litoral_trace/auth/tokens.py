"""Gestión de tokens JWT y sesiones seguras B2B."""
from __future__ import annotations
import hmac
import hashlib
import json
import base64
import time
from typing import Any

DEFAULT_SECRET_KEY = "litoraltrace_production_secret_key_change_in_secrets"

def _base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode('utf-8')

def _base64url_decode(data: str) -> bytes:
    padding = '=' * (4 - (len(data) % 4))
    return base64.urlsafe_b64decode(data + padding)

def create_jwt_token(
    payload: dict[str, Any],
    secret_key: str = DEFAULT_SECRET_KEY,
    expires_in_seconds: int = 86400
) -> str:
    """Genera un token JWT HMAC-SHA256 firmado para autenticación de API/Sesión."""
    header = {"alg": "HS256", "typ": "JWT"}
    now = int(time.time())
    full_payload = {
        **payload,
        "iat": now,
        "exp": now + expires_in_seconds
    }
    
    encoded_header = _base64url_encode(json.dumps(header).encode('utf-8'))
    encoded_payload = _base64url_encode(json.dumps(full_payload).encode('utf-8'))
    
    signing_input = f"{encoded_header}.{encoded_payload}".encode('utf-8')
    signature = hmac.new(secret_key.encode('utf-8'), signing_input, hashlib.sha256).digest()
    encoded_signature = _base64url_encode(signature)
    
    return f"{encoded_header}.{encoded_payload}.{encoded_signature}"

def verify_jwt_token(
    token: str,
    secret_key: str = DEFAULT_SECRET_KEY
) -> dict[str, Any] | None:
    """Verifica y decodifica un token JWT firmado. Retorna el payload si es válido."""
    try:
        parts = token.split('.')
        if len(parts) != 3:
            return None
        
        encoded_header, encoded_payload, encoded_signature = parts
        signing_input = f"{encoded_header}.{encoded_payload}".encode('utf-8')
        
        expected_signature = hmac.new(secret_key.encode('utf-8'), signing_input, hashlib.sha256).digest()
        actual_signature = _base64url_decode(encoded_signature)
        
        if not hmac.compare_digest(expected_signature, actual_signature):
            return None
        
        payload_bytes = _base64url_decode(encoded_payload)
        payload = json.loads(payload_bytes.decode('utf-8'))
        
        # Verificar expiración
        exp = payload.get("exp")
        if exp and int(time.time()) > exp:
            return None
            
        return payload
    except Exception:
        return None
