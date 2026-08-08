"""Entrypoint Streamlit - Litoral Trace Enterprise B2B."""
from __future__ import annotations
import sys
from pathlib import Path

# Insert src directory
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

import streamlit as st
from litoral_trace.ui.theme import apply_enterprise_theme
from litoral_trace.ui.navigation import PANTALLA_DASHBOARD, PANTALLA_AUDITORIA, init_pantalla
from litoral_trace.ui.screens.login import login_screen
from litoral_trace.ui.screens.dashboard import dashboard_screen
from litoral_trace.ui.screens.auditoria import auditoria_screen

def main() -> None:
    st.set_page_config(
        page_title="Litoral Trace | Compliance Intelligence B2B",
        page_icon="🛰️",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    apply_enterprise_theme()
    
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False

    if not st.session_state["logged_in"]:
        login_screen()
    else:
        init_pantalla()
        pantalla = st.session_state.get("pantalla", PANTALLA_DASHBOARD)
        if pantalla == PANTALLA_AUDITORIA:
            auditoria_screen()
        else:
            dashboard_screen()

if __name__ == "__main__":
    main()
