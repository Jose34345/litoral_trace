from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from litoral_trace.web.csrf import (
    CsrfSubject,
    create_csrf_token,
    csrf_subject_from_user,
    verify_csrf_token,
)
from litoral_trace.web.navigation import build_navigation


_TEST_SECRET = "p2fea-test-secret-key-" + ("x" * 32)


def _user(
    *,
    role: str = "admin",
    organization_id: int = 17,
    session_id: int = 31,
):
    return SimpleNamespace(
        username="frontend-user",
        organization_id=organization_id,
        session_id=session_id,
        role=role,
    )


def test_csrf_round_trip_is_session_and_tenant_bound():
    subject = csrf_subject_from_user(_user())
    token = create_csrf_token(
        subject=subject,
        now_epoch=1_000,
        nonce="n" * 24,
        secret_key=_TEST_SECRET,
    )

    assert verify_csrf_token(
        token,
        subject=subject,
        now_epoch=1_100,
        secret_key=_TEST_SECRET,
    )


@pytest.mark.parametrize(
    "subject",
    [
        CsrfSubject("frontend-user", 18, 31),
        CsrfSubject("frontend-user", 17, 32),
        CsrfSubject("other-user", 17, 31),
    ],
)
def test_csrf_rejects_cross_context_replay(subject):
    original = csrf_subject_from_user(_user())
    token = create_csrf_token(
        subject=original,
        now_epoch=1_000,
        nonce="n" * 24,
        secret_key=_TEST_SECRET,
    )

    assert not verify_csrf_token(
        token,
        subject=subject,
        now_epoch=1_100,
        secret_key=_TEST_SECRET,
    )


def test_csrf_rejects_expired_and_tampered_tokens():
    subject = csrf_subject_from_user(_user())
    token = create_csrf_token(
        subject=subject,
        now_epoch=1_000,
        nonce="n" * 24,
        secret_key=_TEST_SECRET,
    )

    assert not verify_csrf_token(
        token,
        subject=subject,
        now_epoch=5_000,
        secret_key=_TEST_SECRET,
    )

    tampered = f"{token[:-1]}{'A' if token[-1] != 'A' else 'B'}"
    assert not verify_csrf_token(
        tampered,
        subject=subject,
        now_epoch=1_100,
        secret_key=_TEST_SECRET,
    )


def test_anonymous_csrf_is_distinct_from_authenticated_context():
    token = create_csrf_token(
        subject=None,
        now_epoch=1_000,
        nonce="n" * 24,
        secret_key=_TEST_SECRET,
    )

    assert verify_csrf_token(
        token,
        subject=None,
        now_epoch=1_100,
        secret_key=_TEST_SECRET,
    )
    assert not verify_csrf_token(
        token,
        subject=csrf_subject_from_user(_user()),
        now_epoch=1_100,
        secret_key=_TEST_SECRET,
    )


def test_navigation_is_server_side_rbac_derived():
    client_nav = build_navigation(
        _user(role="cliente"),
        current_path="/traceability",
    )
    assert [item.key for item in client_nav] == [
        "dashboard",
        "traceability",
        "release_control",
        "evidence",
    ]
    assert next(
        item for item in client_nav if item.key == "traceability"
    ).active is True

    admin_nav = build_navigation(
        _user(role="admin"),
        current_path="/settings",
    )
    assert [item.key for item in admin_nav] == [
        "dashboard",
        "operations",
        "imports",
        "traceability",
        "release_control",
        "evidence",
        "settings",
    ]

    superadmin_nav = build_navigation(
        _user(role="superadmin"),
        current_path="/admin",
    )
    assert [item.key for item in superadmin_nav] == [
        "dashboard",
        "operations",
        "imports",
        "traceability",
        "release_control",
        "evidence",
        "settings",
        "platform",
    ]


def test_frontend_toolchain_is_pinned_and_cdn_free_at_source():
    root = Path(__file__).resolve().parents[1]
    package = json.loads((root / "package.json").read_text(encoding="utf-8"))

    assert package["devDependencies"] == {
        "@fortawesome/fontawesome-free": "6.5.1",
        "@tailwindcss/cli": "4.3.1",
        "htmx.org": "2.0.10",
        "leaflet": "1.9.4",
        "tailwindcss": "4.3.1",
    }

    css = (
        root / "src/litoral_trace/static/src/app.css"
    ).read_text(encoding="utf-8")
    app_js = (
        root / "src/litoral_trace/static/src/js/app.js"
    ).read_text(encoding="utf-8")

    assert '@import "tailwindcss" source(none);' in css
    assert "cdn.tailwindcss.com" not in css
    assert "unpkg.com" not in css
    assert "https://" not in app_js
    assert "X-CSRF-Token" in app_js
