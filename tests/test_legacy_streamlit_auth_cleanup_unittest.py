from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType


class _FakeStreamlit:
    def __init__(self, session_state=None):
        self.session_state = session_state or {}
        self.markdowns: list[str] = []
        self.warnings: list[str] = []
        self.infos: list[str] = []
        self.page_configs: list[dict] = []

    def markdown(self, content: str, **_kwargs):
        self.markdowns.append(content)

    def warning(self, message: str):
        self.warnings.append(message)

    def info(self, message: str):
        self.infos.append(message)

    def set_page_config(self, **kwargs):
        self.page_configs.append(kwargs)


def _load_legacy_modules():
    streamlit_stub = ModuleType("streamlit")
    streamlit_stub.session_state = {}
    streamlit_stub.markdown = lambda *args, **kwargs: None
    streamlit_stub.warning = lambda *args, **kwargs: None
    streamlit_stub.info = lambda *args, **kwargs: None
    streamlit_stub.set_page_config = lambda **kwargs: None
    sys.modules["streamlit"] = streamlit_stub

    sys.modules.pop("app", None)
    sys.modules.pop("litoral_trace.ui.screens.login", None)

    app_module = importlib.import_module("app")
    legacy_login_module = importlib.import_module("litoral_trace.ui.screens.login")
    return app_module, legacy_login_module


def test_legacy_login_source_contains_no_demo_credentials():
    _, legacy_login = _load_legacy_modules()
    source = Path(legacy_login.__file__).read_text(encoding="utf-8")

    assert "admin123" not in source
    assert 'username == "admin"' not in source
    assert 'password == "admin123"' not in source


def test_legacy_login_screen_clears_auth_state_and_shows_disabled_message(monkeypatch):
    _, legacy_login = _load_legacy_modules()
    fake_streamlit = _FakeStreamlit(
        session_state={
            "logged_in": True,
            "username": "admin",
            "organization_id": 1,
            "pantalla": "dashboard",
        }
    )
    monkeypatch.setattr(legacy_login, "st", fake_streamlit)

    legacy_login.login_screen()

    assert fake_streamlit.session_state["logged_in"] is False
    assert "username" not in fake_streamlit.session_state
    assert "organization_id" not in fake_streamlit.session_state
    assert fake_streamlit.warnings == ["Legacy UI authentication is disabled."]
    assert fake_streamlit.infos == [legacy_login.LEGACY_AUTH_DISABLED_MESSAGE]


def test_streamlit_entrypoint_only_renders_disabled_legacy_login(monkeypatch):
    app_module, _ = _load_legacy_modules()
    fake_streamlit = _FakeStreamlit(session_state={"logged_in": True})
    called = {"theme": 0, "login": 0}

    monkeypatch.setattr(app_module, "st", fake_streamlit)
    monkeypatch.setattr(
        app_module,
        "apply_enterprise_theme",
        lambda: called.__setitem__("theme", called["theme"] + 1),
    )
    monkeypatch.setattr(
        app_module,
        "login_screen",
        lambda: called.__setitem__("login", called["login"] + 1),
    )

    app_module.main()

    assert called == {"theme": 1, "login": 1}
    assert fake_streamlit.page_configs
