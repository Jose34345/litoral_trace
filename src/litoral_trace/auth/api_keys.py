"""Gestión criptográfica de Claves de API B2B (API Keys)."""
from __future__ import annotations
import secrets
import hashlib
from typing import NamedTuple

API_KEY_PREFIX = "lt_live_"

class ApiKeyGenerated(NamedTuple):
    full_key: str
    prefix: str
    key_hash: str

def generate_api_key() -> ApiKeyGenerated:
    """Genera una nueva API Key segura para cliente B2B (formato: lt_live_<32_hex_chars>).
    
    Returns:
        ApiKeyGenerated: Contiene la clave completa (mostrar solo 1 vez al usuario),
                         el prefijo visible y el hash SHA-256 para guardar en BD.
    """
    random_secret = secrets.token_hex(20)
    full_key = f"{API_KEY_PREFIX}{random_secret}"
    prefix = full_key[:16]
    key_hash = hash_api_key(full_key)
    return ApiKeyGenerated(full_key=full_key, prefix=prefix, key_hash=key_hash)

def hash_api_key(raw_key: str) -> str:
    """Genera el hash SHA-256 inmutable de la API Key para persistencia segura."""
    return hashlib.sha256(raw_key.strip().encode('utf-8')).hexdigest()

def verify_api_key_hash(raw_key: str, stored_hash: str) -> bool:
    """Verifica si una clave sin procesar coincide con el hash almacenado."""
    if not raw_key or not stored_hash:
        return False
    computed = hash_api_key(raw_key)
    return secrets.compare_digest(computed, stored_hash)
