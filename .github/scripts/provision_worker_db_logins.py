from __future__ import annotations

import base64
import json
import os
import secrets
from urllib.parse import quote, urlsplit, urlunsplit

import psycopg
from psycopg import sql
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


MIGRATION_DATABASE_URL = os.environ["MIGRATION_DATABASE_URL"]
QUEUE_LOGIN = "litoral_trace_worker_queue_login"
RUNTIME_LOGIN = "litoral_trace_worker_runtime_login"
QUEUE_GROUP = "litoral_trace_worker_executor"
RUNTIME_GROUP = "litoral_trace_app"

PUBLIC_KEY = b"""-----BEGIN PUBLIC KEY-----
MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAsU4i60NPyA583Ae1+dcv
f3V4PDLKLaYnPW3jAmW8M3z8IgOhrigZgZDKisLuYmTPOuk3TY1ZfWL9kjANqp7G
QJOA6DOVJTjN67SZOTQw0Q/f8MmK1SxAtKgRURMylScMN9gJt/UhJbRhuNRTb2TR
xN7Ph1bfiR3KBFLtskoQT4t9FAO7eVGJlPCFulsxf8xOMZj/ibG0c40LbJ7HpQfD
UFQi5eHCWa9SZGsZe3oiaefxw124aObXZrd+e5ZhshW+t2Hhf0erx6gmhZqTWud5
v3DqZK2F8a86BgimKLL2A2VteHW4ogH3oY49van3xAXVjyF4LJgFRosieqptzaAU
qB1Dc9T1HGaQbaMFNH4FZuujdCfjPsV7H/MqC6eUMY9pW7WAvvC9g3cfbnRc2bH7
OT0+1wiY+ESRDq/4iohsvyka9iZFUG9otlVIF8c7Y+GiUl9aDCJaVoYcMcpe1BwU
vv+xuFW1vVM51d3+fefZG8IOgLUqRVoD+YYsnY5+UQyhAgMBAAE=
-----END PUBLIC KEY-----
"""


def _ensure_login(cur, *, login: str, password: str, group: str) -> None:
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (login,))
    if cur.fetchone() is None:
        cur.execute(
            sql.SQL("CREATE ROLE {} LOGIN INHERIT NOBYPASSRLS").format(
                sql.Identifier(login)
            )
        )
    cur.execute(
        sql.SQL(
            "ALTER ROLE {} WITH LOGIN INHERIT NOBYPASSRLS PASSWORD %s"
        ).format(sql.Identifier(login)),
        (password,),
    )
    cur.execute(
        sql.SQL("GRANT {} TO {}").format(
            sql.Identifier(group),
            sql.Identifier(login),
        )
    )


def _build_url(base_url: str, *, username: str, password: str) -> str:
    parsed = urlsplit(base_url)
    host = parsed.hostname or ""
    if not host:
        raise RuntimeError("Migration URL has no hostname.")
    port = f":{parsed.port}" if parsed.port else ""
    netloc = (
        f"{quote(username, safe='')}:{quote(password, safe='')}@{host}{port}"
    )
    return urlunsplit(
        ("postgresql+psycopg", netloc, parsed.path, parsed.query, "")
    )


def main() -> None:
    queue_password = secrets.token_urlsafe(36)
    runtime_password = secrets.token_urlsafe(36)

    with psycopg.connect(MIGRATION_DATABASE_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '20s'")
            cur.execute(
                "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)",
                ([QUEUE_GROUP, RUNTIME_GROUP],),
            )
            present = {row[0] for row in cur.fetchall()}
            missing = {QUEUE_GROUP, RUNTIME_GROUP} - present
            if missing:
                raise RuntimeError(
                    f"Required group roles missing: {sorted(missing)}"
                )

            _ensure_login(
                cur,
                login=QUEUE_LOGIN,
                password=queue_password,
                group=QUEUE_GROUP,
            )
            _ensure_login(
                cur,
                login=RUNTIME_LOGIN,
                password=runtime_password,
                group=RUNTIME_GROUP,
            )

            cur.execute(
                """
                SELECT
                    %s::text AS queue_login,
                    pg_has_role(%s, %s, 'MEMBER') AS queue_membership,
                    %s::text AS runtime_login,
                    pg_has_role(%s, %s, 'MEMBER') AS runtime_membership,
                    current_database() AS database_name
                """,
                (
                    QUEUE_LOGIN,
                    QUEUE_LOGIN,
                    QUEUE_GROUP,
                    RUNTIME_LOGIN,
                    RUNTIME_LOGIN,
                    RUNTIME_GROUP,
                ),
            )
            row = cur.fetchone()
            if not row[1] or not row[3]:
                raise RuntimeError("Role membership verification failed.")
            print(
                "ROLE_PROVISIONING_OK "
                + json.dumps(
                    {
                        "queue_login": row[0],
                        "queue_membership": row[1],
                        "runtime_login": row[2],
                        "runtime_membership": row[3],
                        "database_name": row[4],
                    },
                    sort_keys=True,
                )
            )

    payload = json.dumps(
        {
            "US_LACEY_WORKER_DATABASE_URL": _build_url(
                MIGRATION_DATABASE_URL,
                username=QUEUE_LOGIN,
                password=queue_password,
            ),
            "US_LACEY_DATABASE_URL": _build_url(
                MIGRATION_DATABASE_URL,
                username=RUNTIME_LOGIN,
                password=runtime_password,
            ),
        },
        sort_keys=True,
    ).encode("utf-8")

    public_key = serialization.load_pem_public_key(PUBLIC_KEY)
    aes_key = AESGCM.generate_key(bit_length=256)
    nonce = secrets.token_bytes(12)
    ciphertext = AESGCM(aes_key).encrypt(nonce, payload, None)
    encrypted_key = public_key.encrypt(
        aes_key,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )
    sealed = base64.b64encode(
        encrypted_key + nonce + ciphertext
    ).decode("ascii")
    print("SEALED_WORKER_CONFIG=" + sealed)


if __name__ == "__main__":
    main()
