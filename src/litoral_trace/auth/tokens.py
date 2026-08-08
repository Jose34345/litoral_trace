"""Gestion segura de tokens JWT para Litoral Trace."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any

from litoral_trace.config import get_settings


def _get_secret_key(secret_key: str | None = None) -> str:
    """Obtiene la clave JWT desde configuracion centralizada o inyeccion explicita."""
    resolved_secret_key = (secret_key or get_settings().jwt.secret_key or "").strip()

    if not resolved_secret_key:
        raise RuntimeError(
            "JWT_SECRET_KEY no esta configurada."
        )

    if len(resolved_secret_key) < 32:
        raise RuntimeError(
            "JWT_SECRET_KEY debe tener al menos 32 caracteres."
        )

    return resolved_secret_key


def _get_algorithm(algorithm: str | None = None) -> str:
    resolved_algorithm = (
        algorithm
        or get_settings().jwt.algorithm
    ).strip().upper()
    if resolved_algorithm != "HS256":
        raise RuntimeError(
            "JWT_ALGORITHM no soportado. Actualmente solo se admite HS256."
        )
    return resolved_algorithm


def _get_issuer(issuer: str | None = None) -> str | None:
    resolved_issuer = issuer if issuer is not None else get_settings().jwt.issuer
    if resolved_issuer is None:
        return None
    normalized_issuer = resolved_issuer.strip()
    return normalized_issuer or None


def _get_audience(audience: str | None = None) -> str | None:
    resolved_audience = (
        audience
        if audience is not None
        else get_settings().jwt.audience
    )
    if resolved_audience is None:
        return None
    normalized_audience = resolved_audience.strip()
    return normalized_audience or None


def _base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8")


def _base64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _validate_subject(payload: dict[str, Any]) -> str:
    subject = str(payload.get("sub", "")).strip()
    if not subject:
        raise RuntimeError(
            "JWT payload requiere claim 'sub' no vacio."
        )
    return subject


def _audience_matches(payload_audience: Any, expected_audience: str) -> bool:
    if isinstance(payload_audience, str):
        return payload_audience == expected_audience
    if isinstance(payload_audience, list):
        return expected_audience in payload_audience
    return False


def create_jwt_token(
    payload: dict[str, Any],
    expires_in_seconds: int | None = None,
    *,
    secret_key: str | None = None,
    algorithm: str | None = None,
    issuer: str | None = None,
    audience: str | None = None,
    issued_at_epoch: int | None = None,
    token_type: str = "access",
) -> str:
    """Genera un JWT firmado mediante HMAC-SHA256."""
    resolved_secret_key = _get_secret_key(secret_key)
    resolved_algorithm = _get_algorithm(algorithm)
    resolved_issuer = _get_issuer(issuer)
    resolved_audience = _get_audience(audience)
    resolved_token_type = token_type.strip().lower()

    if not resolved_token_type:
        raise RuntimeError(
            "token_type debe ser un valor no vacio."
        )

    token_expiration_seconds = (
        expires_in_seconds
        if expires_in_seconds is not None
        else get_settings().jwt.access_token_expire_seconds
    )
    now = issued_at_epoch if issued_at_epoch is not None else int(time.time())
    subject = _validate_subject(payload)

    header = {
        "alg": resolved_algorithm,
        "typ": "JWT",
    }

    full_payload: dict[str, Any] = {
        **payload,
        "sub": subject,
        "iat": now,
        "nbf": now,
        "exp": now + token_expiration_seconds,
        "token_type": resolved_token_type,
    }

    if resolved_issuer:
        full_payload["iss"] = resolved_issuer
    if resolved_audience:
        full_payload["aud"] = resolved_audience

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
        resolved_secret_key.encode("utf-8"),
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
    *,
    secret_key: str | None = None,
    algorithm: str | None = None,
    issuer: str | None = None,
    audience: str | None = None,
    expected_token_type: str | None = "access",
    now_epoch: int | None = None,
) -> dict[str, Any] | None:
    """Verifica firma, claims criticos y expiracion de un JWT."""
    try:
        resolved_secret_key = _get_secret_key(secret_key)
        resolved_algorithm = _get_algorithm(algorithm)
        resolved_issuer = _get_issuer(issuer)
        resolved_audience = _get_audience(audience)
        current_epoch = now_epoch if now_epoch is not None else int(time.time())

        parts = token.split(".")
        if len(parts) != 3:
            return None

        encoded_header, encoded_payload, encoded_signature = parts

        header_bytes = _base64url_decode(encoded_header)
        header = json.loads(header_bytes.decode("utf-8"))

        if header.get("alg") != resolved_algorithm:
            return None
        if header.get("typ") != "JWT":
            return None

        signing_input = (
            f"{encoded_header}.{encoded_payload}"
        ).encode("utf-8")

        expected_signature = hmac.new(
            resolved_secret_key.encode("utf-8"),
            signing_input,
            hashlib.sha256,
        ).digest()

        actual_signature = _base64url_decode(encoded_signature)
        if not hmac.compare_digest(
            expected_signature,
            actual_signature,
        ):
            return None

        payload_bytes = _base64url_decode(encoded_payload)
        payload = json.loads(payload_bytes.decode("utf-8"))
        if not isinstance(payload, dict):
            return None

        _validate_subject(payload)

        exp = payload.get("exp")
        if exp is None or current_epoch >= int(exp):
            return None

        nbf = payload.get("nbf")
        if nbf is not None and current_epoch < int(nbf):
            return None

        iat = payload.get("iat")
        if iat is not None and int(iat) > current_epoch + 60:
            return None

        if resolved_issuer and payload.get("iss") != resolved_issuer:
            return None

        if resolved_audience and not _audience_matches(
            payload.get("aud"),
            resolved_audience,
        ):
            return None

        if expected_token_type is not None:
            normalized_token_type = expected_token_type.strip().lower()
            if payload.get("token_type") != normalized_token_type:
                return None

        return payload

    except Exception:
        return None
