"""Legacy Streamlit login placeholder kept disabled on purpose."""
from __future__ import annotations

import streamlit as st


LEGACY_AUTH_DISABLED_MESSAGE = (
    "Legacy UI authentication is disabled. Use the main Litoral Trace web application."
)


def _clear_legacy_auth_state() -> None:
    for key in (
        "logged_in",
        "username",
        "organization_name",
        "rol",
        "organization_id",
        "pantalla",
    ):
        st.session_state.pop(key, None)
    st.session_state["logged_in"] = False


def login_screen() -> None:
    _clear_legacy_auth_state()

    st.markdown(
        """
    <style>
        .login-hero {
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
            border-radius: 16px;
            padding: 40px;
            color: #ffffff;
            margin-bottom: 24px;
            text-align: center;
        }
    </style>
    """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
    <div class="login-hero">
        <h1 style="font-size: 2.2rem; font-weight: 700; margin-bottom: 8px; color: #38bdf8;">
            Litoral Trace
        </h1>
        <p style="font-size: 1.05rem; color: #94a3b8; margin: 0;">
            Compliance Intelligence &amp; Trazabilidad Forestal (EUDR)
        </p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    st.warning("Legacy UI authentication is disabled.")
    st.info(LEGACY_AUTH_DISABLED_MESSAGE)
