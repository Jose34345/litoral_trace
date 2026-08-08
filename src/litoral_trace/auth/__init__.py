"""Módulo de autenticación, JWT, API Keys y RBAC para Litoral Trace."""
from litoral_trace.auth.tokens import create_jwt_token, verify_jwt_token
from litoral_trace.auth.api_keys import generate_api_key, hash_api_key, verify_api_key_hash
from litoral_trace.auth.rbac import (
    Permission,
    has_permission,
    permissions_for_role,
    require_any_permission,
    require_permission,
)

__all__ = [
    "create_jwt_token",
    "verify_jwt_token",
    "generate_api_key",
    "hash_api_key",
    "verify_api_key_hash",
    "Permission",
    "has_permission",
    "permissions_for_role",
    "require_any_permission",
    "require_permission",
]
