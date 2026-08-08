"""Gestion segura de tokens JWT para Litoral Trace."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any


def _get_secret_key() -> str:
    """Obtiene la clave JWT desde el entorno."""
    secret_key = os.environ.get("JWT_SECRET_KEY", "").strip()

    if not secret_key:
        raise RuntimeError(
            "JWT_SECRET_KEY no esta configurada."
        )

    if len(secret_key) < 32:
        raise RuntimeError(
            "JWT_SECRET_KEY debe tener al menos 32 caracteres."
        )

    return secret_key


def _base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8")


def _base64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def create_jwt_token(
    payload: dict[str, Any],
    expires_in_seconds: int = 86400,
) -> str:
    """Genera un JWT firmado mediante HMAC-SHA256."""
    secret_key = _get_secret_key()

    header = {
        "alg": "HS256",
        "typ": "JWT",
    }

    now = int(time.time())

    full_payload = {
        **payload,
        "iat": now,
        "exp": now + expires_in_seconds,
    }

    encoded_header = _base64url_encode(
        json.dumps(
            header,
            separators=(",", ":"),
        ).encode("utf-8")
    )

    encoded_payload = _base64url_encode(
        json.dumps(
            full_payload,
            separators=(",", ":"),
        ).encode("utf-8")
    )

    signing_input = (
        f"{encoded_header}.{encoded_payload}"
    ).encode("utf-8")

    signature = hmac.new(
        secret_key.encode("utf-8"),
        signing_input,
        hashlib.sha256,
    ).digest()

    encoded_signature = _base64url_encode(signature)

    return (
        f"{encoded_header}."
        f"{encoded_payload}."
        f"{encoded_signature}"
    )


def verify_jwt_token(
    token: str,
) -> dict[str, Any] | None:
    """Verifica firma y expiracion de un JWT."""
    try:
        secret_key = _get_secret_key()

        parts = token.split(".")

        if len(parts) != 3:
            return None

        encoded_header, encoded_payload, encoded_signature = parts

        # Validar header
        header_bytes = _base64url_decode(encoded_header)
        header = json.loads(header_bytes.decode("utf-8"))

        if header.get("alg") != "HS256":
            return None

        if header.get("typ") != "JWT":
            return None

        signing_input = (
            f"{encoded_header}.{encoded_payload}"
        ).encode("utf-8")

        expected_signature = hmac.new(
            secret_key.encode("utf-8"),
            signing_input,
            hashlib.sha256,
        ).digest()

        actual_signature = _base64url_decode(
            encoded_signature
        )

        if not hmac.compare_digest(
            expected_signature,
            actual_signature,
        ):
            return None

        payload_bytes = _base64url_decode(
            encoded_payload
        )

        payload = json.loads(
            payload_bytes.decode("utf-8")
        )

        exp = payload.get("exp")

        if exp is None:
            return None

        if int(time.time()) >= int(exp):
            return None

        return payload

    except Exception:
        return None
