"""Entrypoint Streamlit - legacy UI intentionally disabled for authentication."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

import streamlit as st

from litoral_trace.ui.screens.login import login_screen
from litoral_trace.ui.theme import apply_enterprise_theme


def main() -> None:
    st.set_page_config(
        page_title="Litoral Trace | Compliance Intelligence B2B",
        page_icon="LT",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    apply_enterprise_theme()
    login_screen()


if __name__ == "__main__":
    main()
