from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "src" / "litoral_trace" / "templates" / "us_lacey" / "base.html"


def test_command_search_shortcut_is_platform_aware() -> None:
    markup = TEMPLATE.read_text(encoding="utf-8")

    assert 'id="lt-command-shortcut"' in markup
    assert '>Ctrl K</span>' in markup
    assert "navigator.userAgentData?.platform" in markup
    assert "isApplePlatform ? '⌘ K' : 'Ctrl K'" in markup
    assert "event.metaKey || event.ctrlKey" in markup
