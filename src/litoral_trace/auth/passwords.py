"""Password hashing utilities."""
from __future__ import annotations

import bcrypt


def hash_password(password: str) -> str:
    """Generate a bcrypt hash for a password."""
    if not password:
        raise ValueError("Password cannot be empty.")

    password_bytes = password.encode("utf-8")
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password_bytes, salt).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    """Verify a password against a bcrypt hash."""
    if not password or not password_hash:
        return False

    try:
        return bcrypt.checkpw(
            password.encode("utf-8"),
            password_hash.encode("utf-8"),
        )
    except (ValueError, TypeError):
        return False
